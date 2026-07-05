import os
from dotenv import load_dotenv

# Load .env file if present
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

# Security
FUEL_API_TOKEN = os.getenv("FUEL_API_TOKEN", "028c992c-dc6a-4509-a94b-db707308841d")
AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY", "")
ADVANCED_PASSWORD = os.getenv("ADVANCED_PASSWORD", "txcrypt")
ADVANCED_SESSION_SECRET = os.getenv("ADVANCED_SESSION_SECRET", "")
ADVANCED_SESSION_HOURS = int(os.getenv("ADVANCED_SESSION_HOURS", "8"))

# Geolocation & Regional Config
STATES = {
    "QLD": {"id": 1, "name": "Queensland", "center": [-27.470, 153.020], "capital": "BRISBANE"},
    "NSW": {"id": 2, "name": "New South Wales", "center": [-33.868, 151.209], "capital": "SYDNEY"},
    "VIC": {"id": 3, "name": "Victoria", "center": [-37.813, 144.963], "capital": "MELBOURNE"},
    "SA":  {"id": 4, "name": "South Australia", "center": [-34.928, 138.600], "capital": "ADELAIDE"},
    "WA":  {"id": 5, "name": "Western Australia", "center": [-31.950, 115.860], "capital": "PERTH"},
    "ACT": {"id": 6, "name": "Australian Capital Territory", "center": [-35.280, 149.130], "capital": "CANBERRA"},
    "TAS": {"id": 7, "name": "Tasmania", "center": [-42.882, 147.327], "capital": "HOBART"},
    "NT":  {"id": 8, "name": "Northern Territory", "center": [-12.463, 130.844], "capital": "DARWIN"}
}

ACTIVE_STATES = ["QLD", "WA"]

FUEL_TYPES = {
    "unleaded": {"name": "Unleaded 91", "fpp_id": 2, "fuelwatch_product": 1},
    "premium": {"name": "Premium 95/98", "fpp_id": 3, "fuelwatch_product": 2},
    "diesel": {"name": "Diesel", "fpp_id": 4, "fuelwatch_product": 4},
    "lpg": {"name": "LPG", "fpp_id": 7, "fuelwatch_product": 5},
}

DEFAULT_STATE = "QLD"
DEFAULT_FUEL_TYPE = "unleaded"
DEFAULT_CENTER_LAT = STATES[DEFAULT_STATE]["center"][0]
DEFAULT_CENTER_LON = STATES[DEFAULT_STATE]["center"][1]

# Brisbane Metro Bounds (Legacy/Specific filtering)
BOUNDS = {
    'lat_min': -27.70, 'lat_max': -27.00,
    'lng_min': 152.70
}

# File Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COLLECTION_FILE = os.path.join(BASE_DIR, "brisbane_fuel_live_collection.csv")
METADATA_FILE = os.path.join(BASE_DIR, "station_metadata.csv")
RATINGS_FILE = os.path.join(BASE_DIR, "station_ratings.csv")
DB_FILE = os.path.join(BASE_DIR, "fuel_data.db")

# Constants
OSRM_BASE_URL = "http://router.project-osrm.org/route/v1/driving"
NOMINATIM_BASE_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "AustralianFuelAI/3.0"

# Allowed CORS origins. Same-origin browser use does not require CORS.
CORS_ORIGINS = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ORIGINS",
        "http://localhost:8000,http://127.0.0.1:8000",
    ).split(",")
    if origin.strip()
]
