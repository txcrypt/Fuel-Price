"""Local Brisbane U91 settings. Secrets belong in .env, never source code."""
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / '.env')
FUEL_API_TOKEN = os.getenv('FUEL_API_TOKEN', '')
ORS_API_KEY = os.getenv('ORS_API_KEY', '')
# Free registered key from https://www.eia.gov/opendata/ ; optional context only.
EIA_API_KEY = os.getenv('EIA_API_KEY', '')
# TMR publishes this shared key for unregistered consumers (API specification v1.10).
QLD_TRAFFIC_API_KEY = os.getenv('QLD_TRAFFIC_API_KEY', '3e83add325cbb69ac4d8e5bf433d770b')
DB_FILE = BASE_DIR / 'fuel_data.db'
COLLECTION_FILE = BASE_DIR / 'brisbane_fuel_live_collection.csv'
SNAPSHOT_FILE = BASE_DIR / 'live_snapshot.csv'
REFRESH_SECONDS = max(60, int(os.getenv('REFRESH_SECONDS', '1800')))
CSV_INTERVAL_SECONDS = 3600
BRISBANE_BOUNDS = {'lat_min': -27.70, 'lat_max': -27.00, 'lng_min': 152.70, 'lng_max': 153.50}
# Whole-of-Queensland extent. Observed site range on 2026-09-28 was lat -29.00..-9.75,
# lng 138.12..153.52; this adds margin without reaching into another state's sites,
# which are excluded by state anyway.
QUEENSLAND_BOUNDS = {'lat_min': -29.50, 'lat_max': -9.00, 'lng_min': 137.90, 'lng_max': 154.00}


def _bounds(prefix, default):
    """Env override, e.g. COLLECT_LAT_MIN. Any missing value keeps the default."""
    try:
        return {key: float(os.getenv(f'{prefix}_{key.upper()}', default[key])) for key in default}
    except ValueError:
        return dict(default)


# What is collected, stored and drawn on the map: all of Queensland.
BOUNDS = _bounds('COLLECT', QUEENSLAND_BOUNDS)
# What the headline median, daily history and forecast are computed over. Deliberately
# narrower: the recorded series and the official backfill are both Brisbane-only, so a
# statewide median here would put a structural break in the middle of the history.
# Widening it starts a new, non-comparable series - see docs/data-sources.md.
ANALYSIS_BOUNDS = _bounds('ANALYSIS', BRISBANE_BOUNDS)
ANALYSIS_AREA_NAME = os.getenv('ANALYSIS_AREA_NAME', 'Brisbane metro')
