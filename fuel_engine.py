import requests
import pandas as pd
import numpy as np
from datetime import datetime
import config

class FuelEngine:
    def __init__(self, token=None, state="QLD"):
        self.token = token if token else config.FUEL_API_TOKEN
        self.state = state
        self.state_id = config.STATES.get(self.state, config.STATES["QLD"])["id"]
        self.base_url = "https://fppdirectapi-prod.fuelpricesqld.com.au"
        self.headers = {
            "Authorization": f"FPDAPI SubscriberToken={self.token}",
            "Content-Type": "application/json"
        }
        self.RIVER_LAT = config.STATES.get(self.state, config.STATES["QLD"])["center"][0]
        self.BOUNDS = config.BOUNDS

    def fetch_sites(self):
        """Get static site data (Location, Name, Brand)"""
        try:
            endpoint = f"{self.base_url}/Subscriber/GetFullSiteDetails"
            params = {"countryId": 21, "geoRegionLevel": 3, "geoRegionId": self.state_id}
            
            r = requests.get(endpoint, headers=self.headers, params=params, timeout=30)
            r.raise_for_status()
            
            df = pd.DataFrame(r.json().get("S", []))
            if df.empty: return df
            
            # Rename to friendly columns
            df = df.rename(columns={
                "S": "site_id", 
                "N": "name", 
                "Lat": "latitude", 
                "Lng": "longitude", 
                "B": "brand_id",
                "P": "postcode",
                "GPI": "google_place_id",  
                "M": "metadata_updated_at" 
            })
            return df[['site_id', 'name', 'brand_id', 'latitude', 'longitude', 'postcode', 'google_place_id']]
        except Exception as e:
            print(f"❌ Site Fetch Error: {e}")
            return pd.DataFrame()

    def fetch_prices(self):
        """Get live prices for Unleaded 91 (ID 2)"""
        try:
            endpoint = f"{self.base_url}/Price/GetSitesPrices"
            params = {"countryId": 21, "geoRegionLevel": 3, "geoRegionId": self.state_id}
            
            r = requests.get(endpoint, headers=self.headers, params=params, timeout=30)
            r.raise_for_status()
            
            data = r.json().get("SitePrices", [])
            if not data: return pd.DataFrame()
            
            df = pd.DataFrame(data)
            
            # FILTER: Only keep Unleaded 91 (FuelId == 2)
            df = df[df['FuelId'] == 2].copy()
            
            # CLEAN: Normalize Price (1799 -> 179.9)
            if 'Price' in df.columns:
                df['price_cpl'] = df['Price'] / 10.0
                
            # CLEAN: Remove outliers (e.g. 999.9 or 0)
            df = df[(df['price_cpl'] > 100) & (df['price_cpl'] < 300)]
            
            df = df.rename(columns={"SiteId": "site_id", "TransactionDateUtc": "reported_at"})
            return df[['site_id', 'price_cpl', 'reported_at']]
            
        except Exception as e:
            print(f"❌ Price Fetch Error: {e}")
            return pd.DataFrame()

    def get_market_snapshot(self):
        """Orchestrates the full pull and merge"""
        sites = self.fetch_sites()
        prices = self.fetch_prices()
        
        if sites.empty or prices.empty:
            return None
            
        # Segment by state bounds if QLD (Brisbane specific legacy), else use all
        if self.state == "QLD":
            state_sites = sites[
                (sites['latitude'] > self.BOUNDS['lat_min']) & 
                (sites['latitude'] < self.BOUNDS['lat_max']) & 
                (sites['longitude'] > self.BOUNDS['lng_min'])
            ].copy()
        else:
            state_sites = sites.copy()
        
        # Apply North/South Logic
        state_sites['region'] = np.where(
            state_sites['latitude'] > self.RIVER_LAT, 'North', 'South'
        )
        
        # Merge
        merged = pd.merge(prices, state_sites, on='site_id', how='inner')
        
        # Add Timestamp and state
        merged['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        merged['state'] = self.state
        
        return merged