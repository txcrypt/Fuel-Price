import pandas as pd
import numpy as np
import os
import pgeocode
import geopandas as gpd
from libpysal.weights import DistanceBand
from esda.moran import Moran, Moran_Local
from shapely.geometry import Point
import tgp_forecast # Added import

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_FILE = os.path.join(BASE_DIR, "brisbane_fuel_live_collection.csv")
METADATA_FILE = os.path.join(BASE_DIR, "station_metadata.csv") # Fixed path reference
OUTPUT_FILE = os.path.join(BASE_DIR, "station_ratings.csv")

def load_and_prep_data():
    """
    Loads data. Prefers the live collection file.
    """
    if not os.path.exists(MASTER_FILE):
        print(f"❌ Error: Data file not found at {MASTER_FILE}")
        return None
        
    print("📂 Loading station data...")
    df = pd.read_csv(MASTER_FILE)
    
    # Standardize columns
    if 'reported_at' in df.columns:
        df['date'] = pd.to_datetime(df['reported_at'], format='mixed', errors='coerce').dt.normalize()
    elif 'scraped_at' in df.columns:
        df['date'] = pd.to_datetime(df['scraped_at'], format='mixed', errors='coerce').dt.normalize()
        
    if 'brand' not in df.columns:
        df['brand'] = None
    
    # Drop rows with invalid dates
    df = df.dropna(subset=['date']).copy()
    
    # Filter recent data (last 7 days to be relevant for current ratings)
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=7)
    df = df[df['date'] > cutoff].copy()
    
    # Keep only the LATEST price per station
    df = df.sort_values('date').groupby('site_id').tail(1)
    
    return df

def load_metadata(df):
    """
    Ensures every station in the DF has metadata.
    """
    print("🗺️  Augmenting Metadata...")
    
    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        df['latitude'] = np.nan
        df['longitude'] = np.nan
        
    if 'suburb' not in df.columns: df['suburb'] = "Unknown"
        
    nomi = pgeocode.Nominatim('au')
    
    if 'name' not in df.columns:
        if 'brand' in df.columns and 'suburb' in df.columns:
             df['name'] = df['brand'].fillna('Station') + " " + df['suburb'].fillna(df['site_id'].astype(str))
        else:
             df['name'] = "Station " + df['site_id'].astype(str)
        
    # Fill missing coords
    if 'postcode' in df.columns:
        missing_coords = df[df['latitude'].isna() | df['longitude'].isna()]
        for idx, row in missing_coords.iterrows():
            try:
                pc = str(int(float(row['postcode'])))
                loc = nomi.query_postal_code(pc)
                if not np.isnan(loc.latitude):
                    df.at[idx, 'latitude'] = loc.latitude
                    df.at[idx, 'longitude'] = loc.longitude
                    df.at[idx, 'suburb'] = loc.place_name
            except: pass
            
    df_clean = df.dropna(subset=['latitude', 'longitude']).copy()
    print(f"   Retained {len(df_clean)} stations with valid coordinates.")
    return df_clean

def analyze_spatial_clustering(stations_df):
    """
    Performs Local Moran's I.
    """
    print("🛰️  Running Spatial Econometrics (Moran's I)...")
    
    # Group by site_id to ensure uniqueness (though we already filtered to latest)
    gdf_data = stations_df.copy()
    
    geometry = [Point(xy) for xy in zip(gdf_data.longitude, gdf_data.latitude)]
    gdf = gpd.GeoDataFrame(gdf_data, geometry=geometry)
    
    try:
        w = DistanceBand.from_dataframe(gdf, threshold=0.05, binary=True, silence_warnings=True) 
        w.transform = 'r'
        
        y = gdf['price_cpl'].values
        moran_loc = Moran_Local(y, w)
        
        sig = moran_loc.p_sim < 0.05
        quadrant = moran_loc.q
        
        labels = []
        for i in range(len(gdf)):
            if not sig[i]:
                labels.append("Neutral")
            else:
                if quadrant[i] == 1: labels.append("🔴 Hot Spot (High-High)")
                elif quadrant[i] == 2: labels.append("🔵 Cold outlier (Low-High)")
                elif quadrant[i] == 3: labels.append("🟢 Cold Spot (Low-Low)")
                elif quadrant[i] == 4: labels.append("🟠 Hot outlier (High-Low)")
                else: labels.append("Neutral")
                
        gdf['spatial_cluster'] = labels
        gdf['moran_p'] = moran_loc.p_sim
        return gdf
        
    except Exception as e:
        print(f"⚠️ Spatial analysis failed: {e}")
        gdf['spatial_cluster'] = "Neutral"
        gdf['moran_p'] = 1.0
        return gdf

def main():
    # 1. Load Data
    raw_df = load_and_prep_data()
    if raw_df is None or raw_df.empty: return
    
    # 2. Prep Metadata
    clean_df = load_metadata(raw_df)
    
    # 3. Spatial Analysis
    spatial_df = analyze_spatial_clustering(clean_df)
    
    # 4. New Fairness Logic (Margin Based)
    # Fetch TGP
    trend = tgp_forecast.analyze_trend()
    current_tgp = trend.get('current_tgp', 165.0)
    print(f"⚖️  Calculating Fairness based on TGP: {current_tgp}c")
    
    # Fairness Score = Price - TGP
    # If < 5c, it's Fair.
    spatial_df['fairness_score'] = spatial_df['price_cpl'] - current_tgp
    
    def get_rating(row):
        margin = row['fairness_score']
        cluster = row['spatial_cluster']
        
        # Strict Rule: Fair if Price < TGP + 5c
        if margin <= 5.0:
            if "Cold Spot" in cluster: return "🌟 SUPER VALUE"
            return "✅ Fair Price"
        elif margin <= 15.0:
            return "⚪ Market Price"
        else:
            if "Hot Spot" in cluster: return "❌ PRICE GOUGE"
            return "⚠️ Expensive"
            
    spatial_df['rating'] = spatial_df.apply(get_rating, axis=1)
    
    # 5. Save
    final_cols = ['site_id', 'name', 'suburb', 'price_cpl', 'fairness_score', 'rating', 'spatial_cluster', 'latitude', 'longitude', 'moran_p']
    
    spatial_df['name'] = spatial_df['name'].fillna(spatial_df['site_id'])
    spatial_df['suburb'] = spatial_df['suburb'].fillna("Unknown")
    
    spatial_df[final_cols].to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Ratings Updated: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
