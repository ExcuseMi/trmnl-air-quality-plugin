import os
import asyncio
import asyncpg
import httpx
import time
import json
import logging
import re
from datetime import datetime, timedelta
from quart import Quart, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
import math

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Quart(__name__)

# Load translations from JSON file
TRANSLATIONS_FILE = os.path.join(os.path.dirname(__file__), 'translations.json')
with open(TRANSLATIONS_FILE, 'r', encoding='utf-8') as f:
    TRANSLATIONS = json.load(f)


def get_translations(locale):
    """Get translations for locale, fallback to English"""
    # Extract language code (ignore country: en-US -> en)
    lang = locale.split('-')[0].lower() if locale else 'en'
    return TRANSLATIONS.get(lang, TRANSLATIONS['en'])


def get_minimal_translations(locale):
    """
    Get only UI labels for translations (not data translations).
    Data fields like status, health_advice, pollutant_name are already translated
    in the response, so we only need to send UI labels here.
    """
    translations = get_translations(locale)
    return {
        'aqi_label': translations.get('aqi_label', 'AQI'),
        'tomorrow': translations.get('tomorrow', 'Tomorrow'),
        'air_quality': translations.get('air_quality', 'Air Quality'),
        'no_data': translations.get('no_data', 'No data available'),
        'error': translations.get('error', 'Error loading data')
    }


# Configuration
AQICN_API_KEY = os.getenv('AQICN_API_KEY')
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
CACHE_MINUTES = 15
DATABASE_URL = os.getenv('DATABASE_URL')
ENABLE_IP_WHITELIST = os.getenv('ENABLE_IP_WHITELIST', 'false').lower() == 'true'
IP_REFRESH_HOURS = 24

# Matches a bare "lat,lon" address (e.g. from a location picker) so we can validate
# the range directly instead of sending it to Nominatim and getting a generic miss.
COORD_ADDRESS_RE = re.compile(r'^\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*$')

# TRMNL server IPs (fetched from https://trmnl.com/api/ips on startup)
TRMNL_IPS = set()
last_ip_refresh = None
scheduler = None

# DB pool — initialized in before_serving, single event loop guaranteed by Quart+Hypercorn
_pool = None


def get_pool():
    return _pool


async def fetch_trmnl_ips():
    """Fetch TRMNL server IPs from their API"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get('https://trmnl.com/api/ips')
            response.raise_for_status()
            data = response.json()

            # Combine IPv4 and IPv6 addresses
            ips = set(data.get('data', {}).get('ipv4', []))
            ips.update(data.get('data', {}).get('ipv6', []))

            ipv4_count = len(data.get('data', {}).get('ipv4', []))
            ipv6_count = len(data.get('data', {}).get('ipv6', []))
            logger.info(f"Fetched {len(ips)} TRMNL IPs from API ({ipv4_count} IPv4, {ipv6_count} IPv6)")
            logger.info(f"Whitelisted IPs: {sorted(list(ips))}")
            return ips
    except Exception as e:
        logger.error(f"Warning: Failed to fetch TRMNL IPs: {e}")
        logger.info("Keeping previous IP whitelist until next refresh")
        return None


def update_trmnl_ips_sync():
    """Update TRMNL IPs - sync wrapper for scheduler"""
    global TRMNL_IPS, last_ip_refresh

    try:
        logger.info("Starting scheduled TRMNL IP refresh...")
        # Run async function in new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            ips = loop.run_until_complete(fetch_trmnl_ips())
            if ips is not None:
                TRMNL_IPS = ips
            last_ip_refresh = datetime.now()
            logger.info(f"TRMNL IPs updated successfully at {last_ip_refresh.isoformat()}")
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"Error updating TRMNL IPs: {e}")


def start_ip_refresh_scheduler():
    """Start background scheduler for IP refresh"""
    global scheduler

    if not ENABLE_IP_WHITELIST:
        logger.info("IP whitelist disabled, skipping refresh scheduler")
        return

    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        func=update_trmnl_ips_sync,
        trigger='interval',
        hours=IP_REFRESH_HOURS,
        id='ip_refresh',
        name='TRMNL IP Refresh',
        replace_existing=True
    )
    scheduler.start()
    logger.info(f"Started IP refresh scheduler (refresh every {IP_REFRESH_HOURS} hours)")


def check_ip_whitelist():
    """Check if request is from TRMNL servers"""
    if not ENABLE_IP_WHITELIST:
        return True

    # Get client IP - Cloudflare Tunnel uses CF-Connecting-IP
    # Priority: CF-Connecting-IP > X-Forwarded-For > X-Real-IP > remote_addr
    client_ip = (
            request.headers.get('CF-Connecting-IP') or
            request.headers.get('X-Forwarded-For') or
            request.headers.get('X-Real-IP') or
            request.remote_addr
    )

    if client_ip and ',' in client_ip:
        # X-Forwarded-For can contain multiple IPs, get the first one
        client_ip = client_ip.split(',')[0].strip()

    logger.info(f"Checking IP whitelist - Client IP: {client_ip}")
    return client_ip in TRMNL_IPS


def require_trmnl_ip(f):
    """Decorator to restrict access to TRMNL IPs only"""

    async def decorated_function(*args, **kwargs):
        if not check_ip_whitelist():
            client_ip = (
                    request.headers.get('CF-Connecting-IP') or
                    request.headers.get('X-Forwarded-For') or
                    request.headers.get('X-Real-IP') or
                    request.remote_addr
            )
            if client_ip and ',' in client_ip:
                client_ip = client_ip.split(',')[0].strip()

            logger.info(f"Access denied for IP: {client_ip}")
            return jsonify({
                'error': 'Access denied',
                'message': 'This API is restricted to TRMNL servers only',
                'client_ip': client_ip.split(',')[0].strip() if client_ip else None
            }), 403
        return await f(*args, **kwargs)

    decorated_function.__name__ = f.__name__
    return decorated_function


def get_aqi_status(aqi, locale='en'):
    """Convert AQI number to status text"""
    translations = get_translations(locale)
    if aqi <= 50:
        return translations['status']['good']
    elif aqi <= 100:
        return translations['status']['moderate']
    elif aqi <= 150:
        return translations['status']['unhealthy_sensitive']
    elif aqi <= 200:
        return translations['status']['unhealthy']
    elif aqi <= 300:
        return translations['status']['very_unhealthy']
    else:
        return translations['status']['hazardous']


def get_pollutant_name(pol_code, locale='en'):
    """Convert pollutant code to readable name"""
    translations = get_translations(locale)
    pollutants = translations['pollutants']
    return pollutants.get(pol_code, pol_code.upper() if pol_code else 'Unknown')


def get_health_advice(aqi, locale='en'):
    """Get health advice based on AQI level"""
    translations = get_translations(locale)
    if aqi <= 50:
        return translations['health_advice']['good']
    elif aqi <= 100:
        return translations['health_advice']['moderate']
    elif aqi <= 150:
        return translations['health_advice']['unhealthy_sensitive']
    elif aqi <= 200:
        return translations['health_advice']['unhealthy']
    elif aqi <= 300:
        return translations['health_advice']['very_unhealthy']
    else:
        return translations['health_advice']['hazardous']


def kmh_to_beaufort(kmh):
    """Convert wind speed (km/h) to Beaufort scale (0-12)"""
    if kmh is None:
        return None
    if kmh < 1:
        return 0
    elif kmh < 6:
        return 1
    elif kmh < 12:
        return 2
    elif kmh < 20:
        return 3
    elif kmh < 29:
        return 4
    elif kmh < 39:
        return 5
    elif kmh < 50:
        return 6
    elif kmh < 62:
        return 7
    elif kmh < 75:
        return 8
    elif kmh < 89:
        return 9
    elif kmh < 103:
        return 10
    elif kmh < 118:
        return 11
    else:
        return 12


async def init_db_schema(conn):
    await conn.execute('''
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
            temperature REAL,
            wind_speed REAL,
            PRIMARY KEY (lat, lon)
        )
    ''')
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS geocoding_cache (
            address TEXT PRIMARY KEY,
            lat REAL,
            lon REAL,
            display_name TEXT,
            cached_at TIMESTAMP
        )
    ''')
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS forecast_cache (
            lat REAL,
            lon REAL,
            forecast_json TEXT,
            cached_at TIMESTAMP,
            PRIMARY KEY (lat, lon)
        )
    ''')
    await conn.execute('''
        CREATE TABLE IF NOT EXISTS stations_cache (
            lat REAL,
            lon REAL,
            zoom INTEGER,
            stations_json TEXT,
            cached_at TIMESTAMP,
            PRIMARY KEY (lat, lon, zoom)
        )
    ''')
    logger.info("Database schema ready")


async def fetch_openweather_forecast(lat, lon):
    """
    Fetch air pollution forecast from OpenWeatherMap API.
    Returns forecast for next 24 hours.
    Cached for 24 hours per location.
    """
    if not OPENWEATHER_API_KEY:
        logger.info("OpenWeatherMap API key not configured")
        return None

    # Round coordinates to 2 decimal places for cache key
    lat_rounded = round(lat, 2)
    lon_rounded = round(lon, 2)

    # Check cache first (24 hour TTL)
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT forecast_json, cached_at FROM forecast_cache WHERE lat = $1 AND lon = $2',
            lat_rounded, lon_rounded
        )
        if row:
            age = (datetime.now() - row['cached_at']).total_seconds() / 60
            if age < 1440:
                logger.info(f"Forecast cache hit for ({lat_rounded}, {lon_rounded}), age: {age:.1f}m")
                return json.loads(row['forecast_json'])
            else:
                logger.info(f"Forecast cache expired for ({lat_rounded}, {lon_rounded}), age: {age:.1f}m")

    # Fetch from API
    url = f"http://api.openweathermap.org/data/2.5/air_pollution/forecast"
    params = {
        'lat': lat,
        'lon': lon,
        'appid': OPENWEATHER_API_KEY
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, params=params)

            if response.status_code == 200:
                data = response.json()

                if 'list' in data and len(data['list']) > 0:
                    # Get forecast for ~24 hours from now
                    forecasts = data['list']

                    # Find forecast closest to 24 hours from now
                    now = datetime.now().timestamp()
                    target_time = now + (24 * 3600)  # 24 hours from now

                    closest_forecast = min(forecasts,
                                           key=lambda x: abs(x['dt'] - target_time))

                    aqi = closest_forecast['main']['aqi']
                    components = closest_forecast['components']

                    # OpenWeather AQI scale: 1=Good, 2=Fair, 3=Moderate, 4=Poor, 5=Very Poor
                    # Convert to US AQI approximation
                    aqi_map = {
                        1: 25,  # Good
                        2: 75,  # Fair
                        3: 125,  # Moderate
                        4: 175,  # Poor
                        5: 250  # Very Poor
                    }

                    us_aqi = aqi_map.get(aqi, 50)

                    # Determine dominant pollutant
                    pm25 = components.get('pm2_5', 0)
                    pm10 = components.get('pm10', 0)
                    o3 = components.get('o3', 0)
                    no2 = components.get('no2', 0)

                    # Simple dominance check (by concentration)
                    dominant = 'pm25' if pm25 > pm10 and pm25 > o3 / 2 else 'o3'

                    forecast_data = {
                        'aqi': us_aqi,
                        'pm25': pm25,
                        'pm10': pm10,
                        'o3': o3,
                        'no2': no2,
                        'dominant': dominant,
                        'timestamp': closest_forecast['dt']
                    }

                    # Cache the result
                    async with pool.acquire() as conn:
                        await conn.execute(
                            '''INSERT INTO forecast_cache (lat, lon, forecast_json, cached_at)
                               VALUES ($1, $2, $3, $4)
                               ON CONFLICT (lat, lon) DO UPDATE SET
                                   forecast_json = EXCLUDED.forecast_json,
                                   cached_at = EXCLUDED.cached_at''',
                            lat_rounded, lon_rounded, json.dumps(forecast_data), datetime.now()
                        )

                    logger.info(f"Fetched and cached forecast for ({lat_rounded}, {lon_rounded})")
                    return forecast_data

            logger.info(f"OpenWeather API error: {response.status_code}")
            return None

    except Exception as e:
        logger.info(f"Error fetching OpenWeather forecast: {e}")
        return None


async def geocode_address(address):
    """
    Convert address to lat/lon coordinates using Nominatim (OpenStreetMap).
    Caches results to avoid repeated API calls.
    """
    # Check cache first
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT lat, lon, display_name FROM geocoding_cache WHERE address = $1',
            address
        )
        if row:
            logger.info(f"Geocoding cache hit for: {address}")
            return {'lat': row['lat'], 'lon': row['lon'], 'display_name': row['display_name']}

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
                    async with pool.acquire() as conn:
                        await conn.execute(
                            '''INSERT INTO geocoding_cache (address, lat, lon, display_name, cached_at)
                               VALUES ($1, $2, $3, $4, $5)
                               ON CONFLICT (address) DO UPDATE SET
                                   lat = EXCLUDED.lat,
                                   lon = EXCLUDED.lon,
                                   display_name = EXCLUDED.display_name,
                                   cached_at = EXCLUDED.cached_at''',
                            address, lat, lon, display_name, datetime.now()
                        )

                    logger.info(f"Geocoded '{address}' to ({lat}, {lon})")
                    return {
                        'lat': lat,
                        'lon': lon,
                        'display_name': display_name
                    }
                else:
                    logger.info(f"No results found for address: {address}")
                    return None
            else:
                logger.info(f"Geocoding API error: {response.status_code}")
                return None
    except Exception as e:
        logger.info(f"Error geocoding address: {e}")
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


def cluster_stations_grid(stations, bounds, max_stations=12, grid_size=3):
    """
    Cluster stations using grid-based spatial distribution.
    Divides the map into grid_size x grid_size cells and selects
    the most representative station from each cell.

    Args:
        stations: List of station dicts with 'lat', 'lon', 'aqi'
        bounds: Dict with 'min_lat', 'max_lat', 'min_lon', 'max_lon'
        max_stations: Maximum number of stations to return
        grid_size: Size of grid (3 = 3x3 = 9 cells)

    Returns:
        List of selected stations with good geographic distribution
    """
    if not stations:
        return []

    if len(stations) <= max_stations:
        return stations

    # Calculate grid cell dimensions
    lat_range = bounds['max_lat'] - bounds['min_lat']
    lon_range = bounds['max_lon'] - bounds['min_lon']
    cell_lat = lat_range / grid_size
    cell_lon = lon_range / grid_size

    # Create grid: dict of (cell_y, cell_x) -> [stations]
    grid = {}
    for station in stations:
        # Determine which cell this station belongs to
        cell_y = int((station['lat'] - bounds['min_lat']) / cell_lat)
        cell_x = int((station['lon'] - bounds['min_lon']) / cell_lon)

        # Clamp to grid bounds
        cell_y = max(0, min(grid_size - 1, cell_y))
        cell_x = max(0, min(grid_size - 1, cell_x))

        key = (cell_y, cell_x)
        if key not in grid:
            grid[key] = []
        grid[key].append(station)

    # Select best station from each cell
    selected = []
    for cell_stations in grid.values():
        if cell_stations:
            # Pick station with most extreme AQI (furthest from 50 = "good")
            # This highlights both very good and very bad air quality
            best = max(cell_stations, key=lambda s: abs(s['aqi'] - 50))
            selected.append(best)

    # Add extreme outliers that weren't selected (very bad or very good AQI)
    selected_set = {(s['lat'], s['lon']) for s in selected}
    for station in stations:
        if (station['lat'], station['lon']) not in selected_set:
            if station['aqi'] > 150 or station['aqi'] < 20:  # Extreme values
                selected.append(station)
                if len(selected) >= max_stations:
                    break

    # If we still need more stations, add closest to center
    if len(selected) < max_stations:
        center_lat = (bounds['min_lat'] + bounds['max_lat']) / 2
        center_lon = (bounds['min_lon'] + bounds['max_lon']) / 2

        remaining = [s for s in stations if (s['lat'], s['lon']) not in selected_set]
        remaining.sort(key=lambda s: (s['lat'] - center_lat) ** 2 + (s['lon'] - center_lon) ** 2)

        for station in remaining:
            selected.append(station)
            if len(selected) >= max_stations:
                break

    # Final limit
    return selected[:max_stations]


async def fetch_nearby_stations(lat, lon, zoom=9):
    """Fetch all AQI stations in the visible map area - cached for 1 hour"""
    # Round coordinates for cache key
    lat_rounded = round(lat, 2)
    lon_rounded = round(lon, 2)

    # Check cache first (15 minute TTL for fresh data)
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT stations_json, cached_at FROM stations_cache WHERE lat = $1 AND lon = $2 AND zoom = $3',
            lat_rounded, lon_rounded, zoom
        )
        if row:
            age = (datetime.now() - row['cached_at']).total_seconds() / 60
            if age < 15:
                logger.info(f"Stations cache hit for ({lat_rounded}, {lon_rounded}, zoom {zoom}), age: {age:.1f}m")
                return json.loads(row['stations_json'])
            else:
                logger.info(f"Stations cache expired for ({lat_rounded}, {lon_rounded}, zoom {zoom}), age: {age:.1f}m")

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

                    logger.info(f"Fetched {len(filtered_stations)} valid stations (filtered from {len(raw_stations)})")

                    # Apply grid-based clustering for better spatial distribution
                    # This spreads out clustered stations but doesn't enforce a hard limit
                    bounds = {
                        'min_lat': se_lat,
                        'max_lat': nw_lat,
                        'min_lon': nw_lon,
                        'max_lon': se_lon
                    }

                    # Use a higher grid size for better distribution without aggressive reduction
                    clustered_stations = cluster_stations_grid(
                        filtered_stations,
                        bounds,
                        max_stations=len(filtered_stations),  # No hard limit, just redistribute
                        grid_size=4  # 4x4 grid for finer distribution
                    )

                    if len(clustered_stations) < len(filtered_stations):
                        logger.info(
                            f"Redistributed {len(filtered_stations)} stations to {len(clustered_stations)} (removed overlaps)")
                    else:
                        logger.info(f"Kept all {len(filtered_stations)} stations (good distribution)")

                    # Cache the result
                    async with pool.acquire() as conn:
                        await conn.execute(
                            '''INSERT INTO stations_cache (lat, lon, zoom, stations_json, cached_at)
                               VALUES ($1, $2, $3, $4, $5)
                               ON CONFLICT (lat, lon, zoom) DO UPDATE SET
                                   stations_json = EXCLUDED.stations_json,
                                   cached_at = EXCLUDED.cached_at''',
                            lat_rounded, lon_rounded, zoom, json.dumps(clustered_stations), datetime.now()
                        )

                    logger.info(f"Cached stations for ({lat_rounded}, {lon_rounded}, zoom {zoom})")
                    return clustered_stations
                else:
                    logger.info(f"API error: {data}")
                    return []
            else:
                logger.info(f"Failed to fetch stations: {response.status_code}")
                return []
    except Exception as e:
        logger.info(f"Error fetching nearby stations: {e}")
        return []


async def get_cached_aqi(lat, lon):
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT * FROM aqi_cache WHERE lat = $1 AND lon = $2 AND fetched_at > $3',
            lat, lon, datetime.now() - timedelta(minutes=CACHE_MINUTES)
        )
        if row:
            logger.info(f"Cache hit for {lat},{lon}")
            city = row['city_name'] or ''
            country = None
            if ', ' in city:
                parts = city.split(', ')
                city = parts[0]
                country = parts[-1]
            aqi = row['aqi']
            dominentpol = row['dominentpol']
            return {
                'lat': row['lat'],
                'lon': row['lon'],
                'aqi': aqi,
                'dominentpol': dominentpol,
                'pollutant_name': get_pollutant_name(dominentpol),
                'city': city,
                'country': country,
                'status': row['status'],
                'health_advice': get_health_advice(aqi),
                'pm25': row['pm25'],
                'pm10': row['pm10'],
                'temperature': row['temperature'],
                'wind_speed': row['wind_speed'],
                'fetched_at': row['fetched_at']
            }
        logger.info(f"Cache miss for {lat},{lon}")
        return None


async def cache_aqi(lat, lon, aqi_data):
    city_full = aqi_data.get('city')
    if aqi_data.get('country'):
        city_full = f"{aqi_data.get('city')}, {aqi_data.get('country')}"
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO aqi_cache
               (lat, lon, aqi, dominentpol, city_name, status, pm25, pm10, data_json, fetched_at, temperature, wind_speed)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
               ON CONFLICT (lat, lon) DO UPDATE SET
                   aqi = EXCLUDED.aqi, dominentpol = EXCLUDED.dominentpol,
                   city_name = EXCLUDED.city_name, status = EXCLUDED.status,
                   pm25 = EXCLUDED.pm25, pm10 = EXCLUDED.pm10,
                   data_json = EXCLUDED.data_json, fetched_at = EXCLUDED.fetched_at,
                   temperature = EXCLUDED.temperature, wind_speed = EXCLUDED.wind_speed''',
            lat, lon, aqi_data.get('aqi'), aqi_data.get('dominentpol'),
            city_full, aqi_data.get('status'), aqi_data.get('pm25'), aqi_data.get('pm10'),
            str(aqi_data), datetime.now(), aqi_data.get('temperature'), aqi_data.get('wind_speed')
        )
    logger.info(f"Cached AQI data for {lat},{lon} (temp: {aqi_data.get('temperature')}°C, wind: {aqi_data.get('wind_speed')} km/h)")


async def fetch_aqi_data(lat, lon, zoom=9, locale='en'):
    """Fetch AQI data for a location"""
    # Check cache first
    cached = await get_cached_aqi(lat, lon)
    if cached:
        # Re-translate cached data with current locale
        cached['status'] = get_aqi_status(cached['aqi'], locale)
        cached['pollutant_name'] = get_pollutant_name(cached['dominentpol'], locale)
        cached['health_advice'] = get_health_advice(cached['aqi'], locale)

        # Add tile coordinates
        tile_x, tile_y = lat_lon_to_tile(lat, lon, zoom)
        cached['tile_x'] = tile_x
        cached['tile_y'] = tile_y
        cached['zoom'] = zoom
        # Stations will be fetched separately (in parallel at endpoint level)
        cached['stations'] = []
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
                logger.info(f"API error: {data}")
                return None

            aqi_info = data['data']
            try:
                aqi = int(aqi_info.get('aqi', 0))
            except (ValueError, TypeError):
                logger.info(f"Invalid AQI value from API: {aqi_info.get('aqi')!r}")
                return None

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

            # Get temperature and wind speed from iaqi (STORED in Celsius and km/h)
            temp_c = iaqi.get('t', {}).get('v')  # Temperature in Celsius
            wind_speed = iaqi.get('w', {}).get('v')  # Wind speed in km/h

            # Get dominant pollutant
            dominentpol = aqi_info.get('dominentpol', 'N/A')

            result = {
                'lat': lat,
                'lon': lon,
                'aqi': aqi,
                'status': get_aqi_status(aqi, locale),
                'city': city,
                'country': country,
                'dominentpol': dominentpol,
                'pollutant_name': get_pollutant_name(dominentpol, locale),
                'health_advice': get_health_advice(aqi, locale),
                'pm25': pm25,
                'pm10': pm10,
                'temperature': temp_c,  # Store in Celsius
                'wind_speed': wind_speed,  # Store in km/h
                'tile_x': None,
                'tile_y': None,
                'zoom': zoom,
                'stations': []
            }

            # Add tile coordinates
            tile_x, tile_y = lat_lon_to_tile(lat, lon, zoom)
            result['tile_x'] = tile_x
            result['tile_y'] = tile_y

            # Stations will be fetched separately (in parallel)
            result['stations'] = []

            # Cache the result (in Celsius and km/h)
            await cache_aqi(lat, lon, result)

            return result

    except Exception as e:
        logger.info(f"Error fetching AQI data: {e}")
        return None


@app.route('/health')
def health():
    """Health check endpoint"""
    health_data = {
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    }

    # Add IP whitelist status if enabled
    if ENABLE_IP_WHITELIST:
        health_data['ip_whitelist'] = {
            'enabled': True,
            'ips_loaded': len(TRMNL_IPS),
            'last_refresh': last_ip_refresh.isoformat() if last_ip_refresh else None
        }

    return jsonify(health_data)


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
      - locale: language code (default: en, supports: en, fr, nl, de, es)
      - temp_unit: celsius or fahrenheit (default: celsius)
      - wind_unit: kmh, mph, ms, knots, or beaufort (default: kmh)
    """
    lat = request.args.get('lat', type=float)
    lon = request.args.get('lon', type=float)
    address = request.args.get('address', type=str)
    zoom = request.args.get('zoom', default=9, type=int)
    locale = request.args.get('locale', default='en', type=str)
    temp_unit = request.args.get('temp_unit', default='celsius', type=str)
    wind_unit = request.args.get('wind_unit', default='kmh', type=str)

    # If address provided, geocode it (unless it's already a bare "lat,lon" pair)
    if address:
        coord_match = COORD_ADDRESS_RE.match(address)
        if coord_match:
            addr_lat, addr_lon = float(coord_match.group(1)), float(coord_match.group(2))
            if not (-90 <= addr_lat <= 90) or not (-180 <= addr_lon <= 180):
                return jsonify({
                    'error': f'Invalid coordinates in address "{address}": latitude must be between '
                             f'-90 and 90, longitude between -180 and 180'
                })
            lat, lon = addr_lat, addr_lon
        else:
            geocode_result = await geocode_address(address)
            if not geocode_result:
                return jsonify({
                    'error': f'Could not find location for "{address}" — check the spelling or try a nearby city name'
                })
            lat = geocode_result['lat']
            lon = geocode_result['lon']
            logger.info(f"Using geocoded coordinates: {lat}, {lon}")

    # Check we have coordinates
    if lat is None or lon is None:
        return jsonify({'error': 'Missing required parameters: (lat, lon) or address'})

    # Fetch AQI data, forecast, and stations in parallel
    aqi_data, forecast, stations = await asyncio.gather(
        fetch_aqi_data(lat, lon, zoom, locale),
        fetch_openweather_forecast(lat, lon),
        fetch_nearby_stations(lat, lon, zoom),
        return_exceptions=True
    )

    # Handle exceptions
    if isinstance(aqi_data, Exception):
        logger.error(f"Error fetching AQI data: {aqi_data}")
        aqi_data = None
    if isinstance(forecast, Exception):
        logger.error(f"Error fetching forecast: {forecast}")
        forecast = None
    if isinstance(stations, Exception):
        logger.error(f"Error fetching stations: {stations}")
        stations = []

    if not aqi_data:
        return jsonify({'error': 'Failed to fetch AQI data'})

    # Add stations to AQI data
    if stations and not isinstance(stations, Exception):
        aqi_data['stations'] = stations

    # Add forecast if available
    if forecast:
        aqi_data['forecast'] = forecast

    # Add minimal translations (UI labels only, not data translations)
    aqi_data['translations'] = get_minimal_translations(locale)

    # Convert temperature if needed (cache stores Celsius)
    if aqi_data.get('temperature') is not None and temp_unit == 'fahrenheit':
        aqi_data['temperature'] = round((aqi_data['temperature'] * 9 / 5) + 32, 1)

    # Convert wind speed if needed (cache stores km/h)
    if aqi_data.get('wind_speed') is not None:
        wind_kmh = aqi_data['wind_speed']
        if wind_unit == 'mph':
            aqi_data['wind_speed'] = round(wind_kmh * 0.621371, 1)
        elif wind_unit == 'ms':
            aqi_data['wind_speed'] = round(wind_kmh * 0.277778, 1)
        elif wind_unit == 'knots':
            aqi_data['wind_speed'] = round(wind_kmh * 0.539957, 1)
        elif wind_unit == 'beaufort':
            aqi_data['wind_speed'] = kmh_to_beaufort(wind_kmh)
        # else kmh - no conversion needed

    # Add units to response
    aqi_data['temp_unit'] = temp_unit
    aqi_data['wind_unit'] = wind_unit

    return jsonify(aqi_data)


@app.before_serving
async def startup():
    global _pool, TRMNL_IPS, last_ip_refresh

    logger.info("Starting TRMNL AQI Plugin...")
    logger.info(f"AQICN API Key: {'*' * 10}{AQICN_API_KEY[-4:] if AQICN_API_KEY else 'NOT SET'}")
    logger.info(f"Cache duration: {CACHE_MINUTES} minutes")
    logger.info(f"IP Whitelist enabled: {ENABLE_IP_WHITELIST}")

    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with _pool.acquire() as conn:
        await init_db_schema(conn)

    if ENABLE_IP_WHITELIST:
        TRMNL_IPS = await fetch_trmnl_ips() or set()
        last_ip_refresh = datetime.now()
        start_ip_refresh_scheduler()