#!/usr/bin/env python3
"""
fuel_prices_qld.py

Simple script to retrieve and inspect fuel prices from the FuelPricesQLD Direct API,
then save the data into a pandas DataFrame and export it to CSV.
"""

import requests
import sys
import pandas as pd

# ─── CONFIG ─────────────────────────────────────────────────────────────────────

# 1) Data Consumer Token
TOKEN = "028c992c-dc6a-4509-a94b-db707308841d"

# 2) Base URL for the Production API
BASE_URL = "https://fppdirectapi-prod.fuelpricesqld.com.au"

# 3) Endpoint and query parameters for site prices
ENDPOINT = "/Price/GetSitesPrices"
PARAMS = {
    "countryId": 21,         # Australia
    "geoRegionLevel": 3,     # 3 = state-level
    "geoRegionId": 1,        # 1 = Queensland
}

# ─── FUNCTIONS ──────────────────────────────────────────────────────────────────

def fetch_prices():
    """
    Calls the GetSitesPrices endpoint and returns a Python list of price-record dicts.
    """
    url = BASE_URL + ENDPOINT
    headers = {
        "Authorization": f"FPDAPI SubscriberToken={TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        resp = requests.get(url, params=PARAMS, headers=headers, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        print("Network error:", e, file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    # Robustly handle dict vs list vs nested list
    if isinstance(data, dict):
        list_val = next((v for v in data.values() if isinstance(v, list)), None)
        prices = list_val if list_val is not None else [data]
    elif isinstance(data, list):
        prices = data
    else:
        print("Unexpected JSON structure:", type(data), file=sys.stderr)
        sys.exit(1)

    return prices

# ─── MAIN SCRIPT ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Fetch data
    prices = fetch_prices()
    print(f"Retrieved {len(prices)} price records.\n")

    # Load into pandas DataFrame
    df = pd.DataFrame(prices)

    # Save DataFrame to CSV
    output_file = 'fuel_prices_qld.csv'
    try:
        df.to_csv(output_file, index=False)
        print(f"Data successfully saved to {output_file}")
    except Exception as e:
        print(f"Error saving to CSV: {e}", file=sys.stderr)

    # Display first few rows
    print("\nSample of the data:")
    print(df.head())
