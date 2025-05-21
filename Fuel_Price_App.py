import requests
import pandas as pd
import schedule
import time


# Replace with the actual API endpoint
API_URL = "https://api.fuelpricesqld.com.au/v1/fuel/prices"

# Your data consumer token
TOKEN = "028c992c-dc6a-4509-a94b-db707308841d"

# Request headers
headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

# Fetch fuel prices
response = requests.get(API_URL, headers=headers)

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data)  # Convert to DataFrame for analysis
    print(df.head())  # Preview data
else:
    print(f"Error: {response.status_code} - {response.text}")


def fetch_fuel_prices():
    response = requests.get(API_URL, headers=headers)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        df.to_csv("fuel_prices.csv", mode='a', index=False, header=False)  # Append to CSV
        print("Data saved.")
    else:
        print(f"Error: {response.status_code}")

# Schedule task to run every hour
schedule.every(1).hours.do(fetch_fuel_prices)

while True:
    schedule.run_pending()
    time.sleep(1)


