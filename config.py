import os

# Security
FUEL_API_TOKEN = os.getenv("FUEL_API_TOKEN", "028c992c-dc6a-4509-a94b-db707308841d") # Default fallback for dev

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

DEFAULT_STATE = "QLD"
DEFAULT_CENTER_LAT = STATES[DEFAULT_STATE]["center"][0]
DEFAULT_CENTER_LON = STATES[DEFAULT_STATE]["center"][1]

# Brisbane Defaults (Legacy/Specific filtering)
BOUNDS = {
    'lat_min': -27.70, 'lat_max': -27.00,
    'lng_min': 152.70
}

# File Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COLLECTION_FILE = os.path.join(BASE_DIR, "brisbane_fuel_live_collection.csv")
METADATA_FILE = os.path.join(BASE_DIR, "station_metadata.csv")
RATINGS_FILE = os.path.join(BASE_DIR, "station_ratings.csv")

# Constants
OSRM_BASE_URL = "http://router.project-osrm.org/route/v1/driving"
NOMINATIM_BASE_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "BrisbaneFuelAI/2.0"
