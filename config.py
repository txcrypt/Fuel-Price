import os

# Security
FUEL_API_TOKEN = os.getenv("FUEL_API_TOKEN") # Must be set in environment

# Geolocation (Brisbane Defaults)
DEFAULT_CENTER_LAT = -27.470
DEFAULT_CENTER_LON = 153.020
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
