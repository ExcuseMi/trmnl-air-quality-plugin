import os
import asyncio
import aiosqlite
import httpx
import time
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_file
import math

app = Flask(__name__)

# Configuration
AQICN_API_KEY = os.getenv('AQICN_API_KEY')
CACHE_HOURS = 1
DB_PATH = '/data/aqi_cache.db'
TILES_PATH = '/data/tiles'
ENABLE_IP_WHITELIST = os.getenv('ENABLE_IP_WHITELIST', 'false').lower() == 'true'

# TRMNL server IPs (fetched from https://usetrmnl.com/api/ips on startup)
TRMNL_IPS = set()

# Ensure directories exist
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(TILES_PATH, exist_ok=True)


async def fetch_trmnl_ips():
    """Fetch TRMNL server IPs from their API"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get('https://usetrmnl.com/api/ips')
            response.raise_for_status()
            data = response.json()

            # Combine IPv4 and IPv6 addresses
            ips = set(data.get('data', {}).get('ipv4', []))
            ips.update(data.get('data', {}).get('ipv6', []))

            print(f"Fetched {len(ips)} TRMNL IPs from API")
            return ips
    except Exception as e:
        print(f"Warning: Failed to fetch TRMNL IPs: {e}")
        print("IP whitelist will not work until IPs are loaded")
        return set()


def check_ip_whitelist():
    """Check if request is from TRMNL servers"""
    if not ENABLE_IP_WHITELIST:
        return True

    # Get client IP (handle proxy headers)
    client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    if client_ip:
        # X-Forwarded-For can contain multiple IPs, get the first one
        client_ip = client_ip.split(',')[0].strip()

    return client_ip in TRMNL_IPS


def require_trmnl_ip(f):
    """Decorator to restrict access to TRMNL IPs only"""

    async def decorated_function(*args, **kwargs):
        if not check_ip_whitelist():
            client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
            return jsonify({
                'error': 'Access denied',
                'message': 'This API is restricted to TRMNL servers only',
                'client_ip': client_ip.split(',')[0].strip() if client_ip else None
            }), 403
        return await f(*args, **kwargs)

    decorated_function.__name__ = f.__name__
    return decorated_function


def get_aqi_status(aqi):
    """Convert AQI number to status text"""
    if aqi <= 50:
        return "Good"
    elif aqi <= 100:
        return "Moderate"
    elif aqi <= 150:
        return "Unhealthy for Sensitive"
    elif aqi <= 200:
        return "Unhealthy"
    elif aqi <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"


def get_pollutant_name(pol_code):
    """Convert pollutant code to readable name"""
    pollutants = {
        'pm25': 'PM2.5',
        'pm10': 'PM10',
        'o3': 'Ozone',
        'no2': 'NO₂',
        'so2': 'SO₂',
        'co': 'CO'
    }
    return pollutants.get(pol_code, pol_code.upper() if pol_code else 'Unknown')


def get_health_advice(aqi):
    """Get health advice based on AQI level"""
    if aqi <= 50:
        return "Air quality is good. Enjoy outdoor activities!"
    elif aqi <= 100:
        return "Air quality is acceptable for most people."
    elif aqi <= 150:
        return "Sensitive groups should reduce prolonged outdoor exertion."
    elif aqi <= 200:
        return "Everyone should reduce prolonged outdoor exertion."
    elif aqi <= 300:
        return "Avoid prolonged outdoor exertion. Keep windows closed."
    else:
        return "Stay indoors and keep windows closed. Health alert!"


async def init_db():
    """Initialize SQLite database"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS aqi_cache (
                lat REAL,
                lon REAL,
                aqi INTEGER,
                dominentpol TEXT,
                city_name TEXT,
                status TEXT,
                pm25 REAL,
                pm10 REAL,
                data_json TEXT,
                fetched_at TIMESTAMP,
                PRIMARY KEY (lat, lon)
            )
        ''')

        # Tile cache table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS tile_cache (
                url TEXT PRIMARY KEY,
                filepath TEXT,
                fetched_at TIMESTAMP
            )
        ''')

        # Geocoding cache table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS geocoding_cache (
                address TEXT PRIMARY KEY,
                lat REAL,
                lon REAL,
                display_name TEXT,
                cached_at TIMESTAMP
            )
        ''')
        await db.commit()


async def geocode_address(address):
    """
    Convert address to lat/lon coordinates using Nominatim (OpenStreetMap).
    Caches results to avoid repeated API calls.
    """
    # Check cache first
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            'SELECT lat, lon, display_name FROM geocoding_cache WHERE address = ?',
            (address,)
        )
        row = await cursor.fetchone()
        if row:
            print(f"Geocoding cache hit for: {address}")
            return {
                'lat': row[0],
                'lon': row[1],
                'display_name': row[2]
            }

    # Call Nominatim API
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        'q': address,
        'format': 'json',
        'limit': 1
    }
    headers = {
        'User-Agent': 'TRMNL-AQI-Plugin/1.0'
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, params=params, headers=headers)

            if response.status_code == 200:
                data = response.json()

                if data and len(data) > 0:
                    result = data[0]
                    lat = float(result['lat'])
                    lon = float(result['lon'])
                    display_name = result['display_name']

                    # Cache the result
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute(
                            '''INSERT OR REPLACE INTO geocoding_cache 
                               (address, lat, lon, display_name, cached_at)
                               VALUES (?, ?, ?, ?, ?)''',
                            (address, lat, lon, display_name, datetime.now())
                        )
                        await db.commit()

                    print(f"Geocoded '{address}' to ({lat}, {lon})")
                    return {
                        'lat': lat,
                        'lon': lon,
                        'display_name': display_name
                    }
                else:
                    print(f"No results found for address: {address}")
                    return None
            else:
                print(f"Geocoding API error: {response.status_code}")
                return None
    except Exception as e:
        print(f"Error geocoding address: {e}")
        return None


def lat_lon_to_tile(lat, lon, zoom):
    """Convert lat/lon to tile coordinates"""
    lat_rad = math.radians(lat)
    n = 2.0 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return x, y


def tile_to_lat_lon(x, y, zoom):
    """Convert tile coordinates to lat/lon"""
    n = 2.0 ** zoom
    lon = x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat = math.degrees(lat_rad)
    return lat, lon


async def fetch_nearby_stations(lat, lon, zoom=9):
    """Fetch all AQI stations in the visible map area"""
    # Calculate tile coordinates
    center_x, center_y = lat_lon_to_tile(lat, lon, zoom)

    # Get bounding box for 3x3 tile grid (corners)
    nw_lat, nw_lon = tile_to_lat_lon(center_x - 1, center_y - 1, zoom)
    se_lat, se_lon = tile_to_lat_lon(center_x + 2, center_y + 2, zoom)

    # AQICN map/bounds API expects: latlng=lat1,lng1,lat2,lng2
    bounds = f"{se_lat},{nw_lon},{nw_lat},{se_lon}"
    url = f"https://api.waqi.info/map/bounds/?latlng={bounds}&token={AQICN_API_KEY}"

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url)

            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'ok':
                    raw_stations = data.get('data', [])

                    # Filter and simplify station data
                    filtered_stations = []
                    for station in raw_stations:
                        aqi = station.get('aqi')

                        # Skip stations with invalid AQI
                        if aqi == '-' or aqi is None:
                            continue

                        # Try to convert to int
                        try:
                            aqi_num = int(aqi)
                        except (ValueError, TypeError):
                            continue  # Skip non-numeric AQI

                        # Only keep essential data
                        filtered_stations.append({
                            'lat': station.get('lat'),
                            'lon': station.get('lon'),
                            'aqi': aqi_num  # Now a proper integer
                        })

                    print(f"Fetched {len(filtered_stations)} valid stations (filtered from {len(raw_stations)})")
                    return filtered_stations
                else:
                    print(f"API error: {data}")
                    return []
            else:
                print(f"Failed to fetch stations: {response.status_code}")
                return []
    except Exception as e:
        print(f"Error fetching nearby stations: {e}")
        return []


async def get_cached_aqi(lat, lon):
    """Get cached AQI data if available and fresh"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            'SELECT * FROM aqi_cache WHERE lat = ? AND lon = ? AND fetched_at > ?',
            (lat, lon, datetime.now() - timedelta(hours=CACHE_HOURS))
        )
        row = await cursor.fetchone()
        if row:
            print(f"Cache hit for {lat},{lon}")
            city = row[4]
            country = None
            if ', ' in city:
                parts = city.split(', ')
                city = parts[0]
                country = parts[-1] if len(parts) > 1 else None

            dominentpol = row[3]
            aqi = row[2]

            return {
                'lat': row[0],
                'lon': row[1],
                'aqi': aqi,
                'dominentpol': dominentpol,
                'pollutant_name': get_pollutant_name(dominentpol),
                'city': city,
                'country': country,
                'status': row[5],
                'health_advice': get_health_advice(aqi),
                'pm25': row[6],
                'pm10': row[7],
                'fetched_at': row[9]
            }
        print(f"Cache miss for {lat},{lon}")
        return None


async def cache_aqi(lat, lon, aqi_data):
    """Cache AQI data"""
    async with aiosqlite.connect(DB_PATH) as db:
        # Reconstruct full city name for storage
        city_full = aqi_data.get('city')
        if aqi_data.get('country'):
            city_full = f"{aqi_data.get('city')}, {aqi_data.get('country')}"

        await db.execute(
            '''INSERT OR REPLACE INTO aqi_cache 
               (lat, lon, aqi, dominentpol, city_name, status, pm25, pm10, data_json, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (
                lat, lon,
                aqi_data.get('aqi'),
                aqi_data.get('dominentpol'),
                city_full,
                aqi_data.get('status'),
                aqi_data.get('pm25'),
                aqi_data.get('pm10'),
                str(aqi_data),
                datetime.now()
            )
        )
        await db.commit()
        print(f"Cached AQI data for {lat},{lon}")


async def fetch_aqi_data(lat, lon, zoom=9):
    """Fetch AQI data for a location"""
    # Check cache first
    cached = await get_cached_aqi(lat, lon)
    if cached:
        # Add tile coordinates and stations
        tile_x, tile_y = lat_lon_to_tile(lat, lon, zoom)
        cached['tile_x'] = tile_x
        cached['tile_y'] = tile_y
        cached['zoom'] = zoom
        cached['stations'] = await fetch_nearby_stations(lat, lon, zoom)
        return cached

    # Fetch from AQICN API
    url = f"https://api.waqi.info/feed/geo:{lat};{lon}/?token={AQICN_API_KEY}"

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url)

            if response.status_code != 200:
                return None

            data = response.json()

            if data.get('status') != 'ok':
                print(f"API error: {data}")
                return None

            aqi_info = data['data']
            aqi = aqi_info.get('aqi', 0)

            # Parse city name (often includes country)
            city_raw = aqi_info.get('city', {}).get('name', 'Unknown')
            # Split city and country if format is "City, Country"
            if ', ' in city_raw:
                city_parts = city_raw.split(', ')
                city = city_parts[0]
                country = city_parts[-1] if len(city_parts) > 1 else None
            else:
                city = city_raw
                country = None

            # Parse iaqi (individual pollutants)
            iaqi = aqi_info.get('iaqi', {})
            pm25 = iaqi.get('pm25', {}).get('v')
            pm10 = iaqi.get('pm10', {}).get('v')

            # Get dominant pollutant
            dominentpol = aqi_info.get('dominentpol', 'N/A')

            result = {
                'lat': lat,
                'lon': lon,
                'aqi': aqi,
                'status': get_aqi_status(aqi),
                'city': city,
                'country': country,
                'dominentpol': dominentpol,
                'pollutant_name': get_pollutant_name(dominentpol),
                'health_advice': get_health_advice(aqi),
                'pm25': pm25,
                'pm10': pm10,
                'tile_x': None,
                'tile_y': None,
                'zoom': zoom,
                'stations': []
            }

            # Add tile coordinates
            tile_x, tile_y = lat_lon_to_tile(lat, lon, zoom)
            result['tile_x'] = tile_x
            result['tile_y'] = tile_y

            # Fetch nearby stations
            result['stations'] = await fetch_nearby_stations(lat, lon, zoom)

            # Cache the result
            await cache_aqi(lat, lon, result)

            return result

    except Exception as e:
        print(f"Error fetching AQI data: {e}")
        return None


@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})


@app.route('/api/aqi')
@require_trmnl_ip
async def get_aqi():
    """
    Get AQI data for a location (returns JSON only)
    Query params:
      - lat: latitude (optional if address provided)
      - lon: longitude (optional if address provided)
      - address: location address (optional if lat/lon provided)
      - zoom: tile zoom level (default: 9)
    """
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    address = request.args.get('address', type=str)
    zoom = request.args.get('zoom', default=9, type=int)

    # If address provided, geocode it
    if address:
        geocode_result = await geocode_address(address)
        if not geocode_result:
            return jsonify({'error': f'Could not find location for address: {address}'}), 400
        lat = geocode_result['lat']
        lon = geocode_result['lon']
        print(f"Using geocoded coordinates: {lat}, {lon}")

    # Check we have coordinates
    if lat is None or lon is None:
        return jsonify({'error': 'Missing required parameters: (lat, lon) or address'}), 400

    aqi_data = await fetch_aqi_data(lat, lon, zoom)

    if not aqi_data:
        return jsonify({'error': 'Failed to fetch AQI data'}), 500

    return jsonify(aqi_data)


def create_app():
    """Application factory"""
    return app


# Initialize on module load (runs once per worker)
async def startup():
    """Initialize on startup"""
    global TRMNL_IPS

    print("Starting TRMNL AQI Plugin...")
    print(f"AQICN API Key: {'*' * 10}{AQICN_API_KEY[-4:] if AQICN_API_KEY else 'NOT SET'}")
    print(f"Cache duration: {CACHE_HOURS} hours")
    print(f"IP Whitelist enabled: {ENABLE_IP_WHITELIST}")

    # Initialize database
    await init_db()
    print("Database initialized")

    # Fetch TRMNL IPs if whitelist is enabled
    if ENABLE_IP_WHITELIST:
        TRMNL_IPS = await fetch_trmnl_ips()


# Run startup in event loop
import asyncio

asyncio.run(startup())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)