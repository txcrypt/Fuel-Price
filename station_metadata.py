import pandas as pd
import os
import numpy as np

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LIVE_FILE = os.path.join(BASE_DIR, "brisbane_fuel_live_collection.csv")
RATINGS_FILE = os.path.join(BASE_DIR, "station_ratings.csv")
METADATA_FILE = os.path.join(BASE_DIR, "station_metadata.csv")

def generate_metadata():
    """
    Consolidates station metadata (ID, Name, Brand, Suburb, Postcode, Lat, Lon)
    into a single lookup file for the dashboard.
    """
    import pgeocode # Lazy import to avoid loading large dataset on startup
    print("🗺️ Generating Station Metadata Map...")
    
    # 1. Load Live Data (Primary Source for IDs)
    if not os.path.exists(LIVE_FILE):
        print("❌ Live data missing.")
        return
        
    live_df = pd.read_csv(LIVE_FILE)
    
    # 2. Load Ratings (Secondary Source for Suburbs/Names)
    ratings_df = pd.DataFrame()
    if os.path.exists(RATINGS_FILE):
        ratings_df = pd.read_csv(RATINGS_FILE)
        
    # 2b. Load Static Excel List (Tertiary Source - High Quality)
    excel_path = os.path.join(BASE_DIR, "..", "Data", "SiteID_List_QLD.xlsx")
    excel_df = pd.DataFrame()
    if os.path.exists(excel_path):
        try:
            print("📊 Loading Static Excel List...")
            excel_df = pd.read_excel(excel_path)
            # Normalize columns
            excel_df.columns = [str(c).lower().strip() for c in excel_df.columns]
            print(f"Debug: Excel Columns found: {excel_df.columns.tolist()}")
            
            # Map known variations
            rename_map = {
                'siteid (s)': 'site_id', 'siteid': 'site_id', 
                'address (a)': 'name', 'address': 'name',
                'postcode (p)': 'postcode', 'postcode': 'postcode',
                'brandid (b)': 'brand', 'brand': 'brand'
            } 
            excel_df.rename(columns=rename_map, inplace=True)
            
            if 'site_id' in excel_df.columns:
                excel_df['site_id'] = excel_df['site_id'].astype(str)
        except Exception as e:
            print(f"⚠️ Could not read Excel: {e}")

    # 3. Merge Logic
    # We want a unique list of stations
    stations = live_df[['site_id', 'latitude', 'longitude']].drop_duplicates(subset='site_id').copy()
    
    # Convert site_id to string for merging
    stations['site_id'] = stations['site_id'].astype(str).str.replace('.0', '', regex=False)
    
    print(f"Debug: Live IDs sample: {stations['site_id'].head().tolist()}")
    if not excel_df.empty and 'site_id' in excel_df.columns:
        print(f"Debug: Excel IDs sample: {excel_df['site_id'].head().tolist()}")

    # Helper to merge from source
    def merge_source(target_df, source_df, source_name):
        if source_df.empty: return target_df
        
        # Ensure ID match
        if 'site_id' not in source_df.columns: return target_df
        source_df['site_id'] = source_df['site_id'].astype(str).str.replace('.0', '', regex=False)
        
        cols_to_use = [c for c in ['name', 'brand', 'postcode', 'suburb'] if c in source_df.columns]
        if not cols_to_use: return target_df
        
        before_na = target_df['suburb'].isna().sum() if 'suburb' in target_df.columns else 0
        merged = target_df.merge(source_df[['site_id'] + cols_to_use], on='site_id', how='left', suffixes=('', '_new'))
        
        for col in cols_to_use:
            col_new = col + '_new'
            
            # Case 1: Column existed in both, so suffix applied
            if col_new in merged.columns:
                merged[col] = merged[col].fillna(merged[col_new])
                merged.drop(columns=[col_new], inplace=True)
            # Case 2: Column only existed in source (new column), so no suffix
            elif col in merged.columns and col not in target_df.columns:
                pass # It's already there as 'col'
            
        after_na = merged['suburb'].isna().sum() if 'suburb' in merged.columns else 0
        print(f"   Merged {source_name}: Filled {before_na - after_na} missing suburbs.")
        return merged

    # Merge sequence: Live -> Ratings -> Excel
    stations = merge_source(stations, live_df, "Live")
    stations = merge_source(stations, ratings_df, "Ratings")
    stations = merge_source(stations, excel_df, "Excel")

    # 4. Fill Gaps (Normalization & Postcode Lookup)
    if 'name' not in stations.columns: stations['name'] = "Station " + stations['site_id']
    if 'suburb' not in stations.columns: stations['suburb'] = np.nan # Use nan for now
    if 'postcode' not in stations.columns: stations['postcode'] = ""
    
    # Normalize Postcode
    def clean_postcode(x):
        try:
            val = int(float(x))
            return f"{val:04d}" # Ensure 4 digits for AU
        except:
            return str(x) if pd.notnull(x) and str(x).strip() != "" else ""
            
    stations['postcode'] = stations['postcode'].apply(clean_postcode)
    
    # Lookup Suburbs from Postcodes using pgeocode
    print("🔍 reverse-geocoding suburbs from postcodes...")
    nomi = pgeocode.Nominatim('au')
    
    # Filter unique postcodes to query
    unique_pcs = stations[stations['suburb'].isna() | (stations['suburb'] == "Unknown")]['postcode'].unique()
    unique_pcs = [pc for pc in unique_pcs if pc and pc.isdigit()]
    
    pc_map = {}
    if unique_pcs:
        # pgeocode query_postal_code expects just the code, returns dataframe
        # We can loop or query batch? query_postal_code handles list/series?
        # Documentation says it handles list/array.
        res = nomi.query_postal_code(unique_pcs)
        if not res.empty:
            # pgeocode returns 'place_name' for suburb, 'postal_code'
            for idx, row in res.iterrows():
                pc = str(row['postal_code'])
                sub = row['place_name']
                if pd.notnull(sub):
                    pc_map[pc] = sub
    
    # Apply mapping
    def fill_suburb(row):
        if pd.notnull(row['suburb']) and row['suburb'] != "Unknown":
            return row['suburb']
        pc = row['postcode']
        return pc_map.get(pc, "Unknown")
        
    stations['suburb'] = stations.apply(fill_suburb, axis=1)
    stations['suburb'] = stations['suburb'].fillna("Unknown").astype(str).str.title()
    
    # 5. Enrich Brand Data
    def get_brand_icon(name):
        name = str(name).lower()
        if "bp" in name: return "🟢 BP"
        if "shell" in name: return "🟡 Shell"
        if "caltex" in name or "ampol" in name: return "🔴 Ampol"
        if "7-eleven" in name or "7 eleven" in name: return "🟠 7-Eleven"
        if "costco" in name: return "🔵 Costco"
        if "united" in name: return "🔵 United"
        if "puma" in name: return "🟢 Puma"
        return "⛽ Independent"

    stations['display_brand'] = stations['name'].apply(get_brand_icon)
    
    # Save
    stations.to_csv(METADATA_FILE, index=False)
    print(f"✅ Metadata generated for {len(stations)} stations: {METADATA_FILE}")

if __name__ == "__main__":
    generate_metadata()