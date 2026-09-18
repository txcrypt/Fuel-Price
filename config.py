"""Local Brisbane U91 settings. Secrets belong in .env, never source code."""
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / '.env')
FUEL_API_TOKEN = os.getenv('FUEL_API_TOKEN', '')
ORS_API_KEY = os.getenv('ORS_API_KEY', '')
# TMR publishes this shared key for unregistered consumers (API specification v1.10).
QLD_TRAFFIC_API_KEY = os.getenv('QLD_TRAFFIC_API_KEY', '3e83add325cbb69ac4d8e5bf433d770b')
DB_FILE = BASE_DIR / 'fuel_data.db'
COLLECTION_FILE = BASE_DIR / 'brisbane_fuel_live_collection.csv'
SNAPSHOT_FILE = BASE_DIR / 'live_snapshot.csv'
REFRESH_SECONDS = max(60, int(os.getenv('REFRESH_SECONDS', '1800')))
CSV_INTERVAL_SECONDS = 3600
BOUNDS = {'lat_min': -27.70, 'lat_max': -27.00, 'lng_min': 152.70, 'lng_max': 153.50}
