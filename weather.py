import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def create_session_with_retries():
    """Create a requests session with retry logic"""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


session = create_session_with_retries()


def get_coords_from_zipcode(zipcode, country_code, api_key):
    url = f"http://api.openweathermap.org/geo/1.0/zip?zip={zipcode},{country_code}&appid={api_key}"
    try:
        r = session.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data['lat'], data['lon']
    except Exception as e:
        print(f"Error getting coordinates: {e}")
        raise


def get_current_weather(lat, lon, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=imperial"
    try:
        r = session.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Error getting weather data: {e}")
        return None
