import pandas as pd
import numpy as np
import os
import pgeocode
import geopandas as gpd
from libpysal.weights import DistanceBand
from esda.moran import Moran, Moran_Local
from scipy.spatial import Voronoi
from shapely.geometry import Point, Polygon

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_FILE = os.path.join(BASE_DIR, "brisbane_fuel_live_collection.csv") # Switch to live file for coordinate availability
METADATA_FILE = os.path.join(BASE_DIR, "..", "Data", "SiteID_List_QLD.xlsx")
OUTPUT_FILE = os.path.join(BASE_DIR, "station_ratings.csv")

def load_and_prep_data():
    """
    Loads data. Prefers the live collection file because it likely contains
    Lat/Lon coordinates which are essential for spatial analysis.
    """
    if not os.path.exists(MASTER_FILE):
        print(f"❌ Error: Data file not found at {MASTER_FILE}")
        return None
        
    print("📂 Loading station data...")
    df = pd.read_csv(MASTER_FILE)
    
    # Standardize columns
    # Expects: site_id, price_cpl, latitude, longitude
    if 'reported_at' in df.columns:
        df['date'] = pd.to_datetime(df['reported_at'], format='mixed', errors='coerce').dt.normalize()
    elif 'scraped_at' in df.columns:
        df['date'] = pd.to_datetime(df['scraped_at'], format='mixed', errors='coerce').dt.normalize()
        
    # Ensure brand is available for naming
    if 'brand' not in df.columns:
        df['brand'] = None
    
    # Drop rows with invalid dates
    df = df.dropna(subset=['date']).copy()
    
    # Filter recent data for relevance (last 30 days)
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=30)
    df = df[df['date'] > cutoff].copy()
    
    return df

def load_metadata(df):
    """
    Ensures every station in the DF has metadata (Name, Suburb, Lat, Lon).
    If Lat/Lon are missing in DF, tries to fill from pgeocode.
    """
    print("🗺️  Augmenting Metadata...")
    
    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        df['latitude'] = np.nan
        df['longitude'] = np.nan
        
    # Initialize suburb column if missing
    if 'suburb' not in df.columns:
        df['suburb'] = "Unknown"
        
    nomi = pgeocode.Nominatim('au')
    
    # If names are missing, try to construct from Brand + Suburb
    if 'name' not in df.columns:
        if 'brand' in df.columns and 'suburb' in df.columns:
             df['name'] = df['brand'].fillna('Station') + " " + df['suburb'].fillna(df['site_id'].astype(str))
        else:
             df['name'] = "Station " + df['site_id'].astype(str)
        
    # Fill missing coords using postcode if available
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
            
    # Drop rows where we still have no location (cannot do spatial analysis)
    df_clean = df.dropna(subset=['latitude', 'longitude']).copy()
    print(f"   Retained {len(df_clean)} stations with valid coordinates.")
    return df_clean

def analyze_spatial_clustering(stations_df):
    """
    Performs Local Moran's I to identify Hot Spots (Expensive clusters) 
    and Cold Spots (Cheap clusters).
    """
    print("🛰️  Running Spatial Econometrics (Moran's I)...")
    
    # Need unique list of stations with their AVG price
    # Group by site_id
    gdf_data = stations_df.groupby(['site_id', 'latitude', 'longitude']).agg({
        'price_cpl': 'mean',
        'name': 'first',
        'suburb': 'first'
    }).reset_index()
    
    # Create GeoDataFrame
    geometry = [Point(xy) for xy in zip(gdf_data.longitude, gdf_data.latitude)]
    gdf = gpd.GeoDataFrame(gdf_data, geometry=geometry)
    
    try:
        # 1. Create Spatial Weights Matrix (Distance Based)
        # Use K-Nearest Neighbors or Distance Band? Distance Band is better for "Hot Spots"
        # Auto-calculate threshold distance (e.g., 5km)
        w = DistanceBand.from_dataframe(gdf, threshold=0.05, binary=True, silence_warnings=True) 
        # Note: 0.05 degrees is roughly 5km. Adjust as needed.
        
        w.transform = 'r' # Row-standardize
        
        # 2. Calculate Local Moran's I
        y = gdf['price_cpl'].values
        moran_loc = Moran_Local(y, w)
        
        # 3. Classify Results
        # Quadrants: 1=HH (Hot Spot), 2=LH, 3=LL (Cold Spot), 4=HL
        # Significant if p_sim < 0.05
        
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
        print(f"⚠️ Spatial analysis failed (likely disconnected points): {e}")
        gdf['spatial_cluster'] = "Neutral"
        gdf['moran_p'] = 1.0
        return gdf

def generate_voronoi_regions(stations_df):
    """
    Calculates Voronoi Polygons for visualization (Catchment Areas).
    """
    # Only needs lat/long
    coords = stations_df[['longitude', 'latitude']].values
    
    # This is complex to return as a DF column. 
    # Typically we just return the objects for plotting later.
    # For this CLI tool, we will just skip visual generation here
    # and rely on the clustering labels.
    return None

def main():
    # 1. Load Raw Data
    raw_df = load_and_prep_data()
    if raw_df is None or raw_df.empty: return
    
    # 2. Prep Metadata & Coords
    clean_df = load_metadata(raw_df)
    
    # 3. Run Spatial Analysis
    spatial_df = analyze_spatial_clustering(clean_df)
    
    # 4. Fairness Scoring (Simplified integration)
    avg_price = spatial_df['price_cpl'].mean()
    spatial_df['fairness_score'] = spatial_df['price_cpl'] - avg_price
    
    def get_rating(row):
        if "Cold Spot" in row['spatial_cluster']: return "✅ Super Value"
        if "Hot Spot" in row['spatial_cluster']: return "❌ Rip-off Zone"
        if row['fairness_score'] < -2: return "✅ Great Value"
        if row['fairness_score'] > 2: return "⚠️ Expensive"
        return "⚪ Fair Market"
        
    spatial_df['rating'] = spatial_df.apply(get_rating, axis=1)
    
    # 5. Save
    # Ensure we have all columns expected by dashboard
    # Schema: site_id, name, suburb, price_cpl, fairness_score, rating, spatial_cluster, latitude, longitude
    final_cols = ['site_id', 'name', 'suburb', 'price_cpl', 'fairness_score', 'rating', 'spatial_cluster', 'latitude', 'longitude', 'moran_p']
    
    # Fill missing name/suburb
    spatial_df['name'] = spatial_df['name'].fillna(spatial_df['site_id'])
    spatial_df['suburb'] = spatial_df['suburb'].fillna("Unknown")
    spatial_df['data_points'] = 100 # Dummy for backward compatibility
    
    spatial_df[final_cols].to_csv(OUTPUT_FILE, index=False)
    
    print(f"\n✅ Spatial Analysis Complete. Saved {len(spatial_df)} stations to {OUTPUT_FILE}")
    print(f"\n🗺️  Cluster Sample:")
    print(spatial_df[['suburb', 'price_cpl', 'spatial_cluster']].head(10).to_string(index=False))

if __name__ == "__main__":
    main()
