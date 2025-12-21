import sys
import os
import pandas as pd
import numpy as np
import requests
import urllib.parse
import polyline  
from math import radians, cos, sin, asin, sqrt

try:
    from fuel_engine import FuelEngine 
except ImportError:
    print("❌ Error: Could not import FuelEngine. Check path.")
    sys.exit(1)

TOKEN = "028c992c-dc6a-4509-a94b-db707308841d"

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance in kilometers between two points 
    on the earth (specified in decimal degrees)
    """
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers
    return c * r

def get_coords_from_address(address):
    """
    Resolves a specific address to coordinates using OpenStreetMap Nominatim API.
    """
    base_url = "https://nominatim.openstreetmap.org/search"
    params = {
        'q': f"{address}, Australia",
        'format': 'json',
        'limit': 1
    }
    headers = {
        'User-Agent': 'BrisbaneFuelAI/1.0'
    }
    
    try:
        r = requests.get(base_url, params=params, headers=headers, timeout=5)
        if r.status_code == 200 and r.json():
            data = r.json()[0]
            return float(data['lat']), float(data['lon']), data['display_name']
    except Exception as e:
        print(f"⚠️ Geocoding Error: {e}")
        
    return None, None, None

def get_osrm_route(lat1, lon1, lat2, lon2):
    """
    Fetches driving route from OSRM public API.
    Returns: 
        - route_coords: List of [lat, lon] points for the path
        - distance_km: Total driving distance
    """
    # OSRM expects lon,lat
    url = f"http://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=full&geometries=geojson"
    
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data['code'] == 'Ok' and len(data['routes']) > 0:
                route = data['routes'][0]
                # GeoJSON is [lon, lat], we want [lat, lon] for Folium/Math
                geometry = route['geometry']['coordinates']
                path = [[p[1], p[0]] for p in geometry]
                distance = route['distance'] / 1000.0 # Meters to km
                return path, distance
    except Exception as e:
        print(f"⚠️ OSRM Routing Error: {e}")
        
    return None, None

def calculate_detour_utility(station, route_dist_km, market_avg_price, 
                             tank_capacity=50, current_fuel=10, km_per_liter=10, hourly_wage=30.0):
    """
    Calculates the Net Utility ($) of stopping at a specific station versus driving past.
    
    Formula: U = Savings - (TimeCost + FuelBurnCost)
    """
    price_at_station = station['price_cpl']
    
    # 1. Detour Distance (Approximate)
    # We assume the detour is roughly 2x the distance from the route path (there and back)
    # plus a small penalty for entering/exiting.
    # 'dist_score' is in degrees. 1 deg ~= 111km.
    detour_km = (station['dist_score'] * 111.0) * 2.0 
    
    # HARD FILTER: Max Detour 5km
    if detour_km > 5.0: return -999.0
    
    # 2. Fill Volume
    fill_volume = tank_capacity - current_fuel
    if fill_volume <= 0: return -999.0 # No utility if tank full
    
    # 3. Gross Savings vs Market Average
    # How much do we save by filling here vs filling at an "average" station later?
    # Convert cents to dollars
    gross_savings = (market_avg_price - price_at_station) / 100.0 * fill_volume
    
    # 4. Costs
    avg_speed_kmh = 40.0 # Urban average
    time_hours = detour_km / avg_speed_kmh
    # Add 5 mins (0.08h) for the stop itself (transaction time penalty)
    time_hours += 0.08 
    
    time_cost = time_hours * hourly_wage
    
    fuel_burned_liters = detour_km / km_per_liter
    fuel_burn_cost = fuel_burned_liters * (price_at_station / 100.0)
    
    total_cost = time_cost + fuel_burn_cost
    
    net_utility = gross_savings - total_cost
    
    return net_utility

def analyze_commute_route(start_address, end_address):
    """
    Analyzes price difference between Start and End locations.
    Returns strategic advice: "Fill at Home" vs "Fill at Work".
    """
    # 1. Resolve Addresses
    lat1, lon1, name1 = get_coords_from_address(start_address)
    lat2, lon2, name2 = get_coords_from_address(end_address)
    
    if lat1 is None or lat2 is None: return None
    
    # 2. Fetch Live Data
    engine = FuelEngine(TOKEN)
    df = engine.get_market_snapshot()
    if df is None or df.empty: return None
    
    # 3. Filter Radius (approx 0.1 deg ~ 10km)
    rad = 0.1
    start_zone = df[
        (df['latitude'].between(lat1-rad, lat1+rad)) & 
        (df['longitude'].between(lon1-rad, lon1+rad))
    ]
    end_zone = df[
        (df['latitude'].between(lat2-rad, lat2+rad)) & 
        (df['longitude'].between(lon2-rad, lon2+rad))
    ]
    
    if start_zone.empty or end_zone.empty:
        return {"error": "Insufficient data at one or both ends."}
        
    p1 = start_zone['price_cpl'].mean()
    p2 = end_zone['price_cpl'].mean()
    
    diff = p1 - p2
    
    res = {
        'start_name': name1.split(',')[0],
        'end_name': name2.split(',')[0],
        'start_price': p1,
        'end_price': p2,
        'diff': diff
    }
    
    if diff > 10.0:
        res['advice'] = f"Wait until you reach {res['end_name']}."
        res['action'] = "WAIT"
    elif diff < -10.0:
        res['advice'] = f"Fill up here in {res['start_name']} before you leave."
        res['action'] = "FILL_NOW"
    else:
        res['advice'] = "Prices are similar at both ends. Fill whenever convenient."
        res['action'] = "NEUTRAL"
        
    return res

def optimize_route(start_address, end_address, 
                   tank_capacity=50, current_fuel=10, km_per_liter=10, hourly_wage=0.0):
    
    # 1. Resolve Addresses
    print(f"🔍 Resolving: '{start_address}'")
    lat1, lon1, name1 = get_coords_from_address(start_address)
    
    print(f"🔍 Resolving: '{end_address}'")
    lat2, lon2, name2 = get_coords_from_address(end_address)
    
    if lat1 is None or lat2 is None:
        print("❌ Error: Could not resolve one or both addresses.")
        return None

    print(f"\n🚗 Planning Route:\n   A: {name1}\n   B: {name2}")
    
    # 2. Get Real Driving Route
    print("🛣️  Fetching driving path from OSRM...")
    route_path, route_dist = get_osrm_route(lat1, lon1, lat2, lon2)
    
    if not route_path:
        print("⚠️ Could not get driving route. Falling back to straight line.")
        route_path = [[lat1, lon1], [lat2, lon2]]
        route_dist = 0

    # 3. Fetch Live Data
    print("📡 Fetching Live Prices...")
    engine = FuelEngine(TOKEN)
    df = engine.get_market_snapshot()
    if df is None or df.empty: return None
    
    market_avg = df['price_cpl'].median()

    # 4. Filter Stations along the Route
    path_lats = [p[0] for p in route_path]
    path_lons = [p[1] for p in route_path]
    
    min_lat, max_lat = min(path_lats) - 0.05, max(path_lats) + 0.05
    min_lon, max_lon = min(path_lons) - 0.05, max(path_lons) + 0.05
    
    # Initial Bbox Filter
    candidates = df[
        (df['latitude'] >= min_lat) & (df['latitude'] <= max_lat) &
        (df['longitude'] >= min_lon) & (df['longitude'] <= max_lon)
    ].copy()
    
    if candidates.empty:
        return {
            'start': {'lat': lat1, 'lon': lon1, 'name': name1},
            'end': {'lat': lat2, 'lon': lon2, 'name': name2},
            'stations': pd.DataFrame(),
            'route_path': route_path
        }

    # 5. Geometric Filter (Distance from Polyline)
    def min_dist_to_route(row):
        station_loc = np.array([row['latitude'], row['longitude']])
        # Subsample route path to every 5th point to speed up
        route_arr = np.array(route_path[::5]) 
        dists = np.sum((route_arr - station_loc)**2, axis=1)
        return np.sqrt(np.min(dists))

    # Threshold: 0.05 degrees (~5km)
    candidates['dist_score'] = candidates.apply(min_dist_to_route, axis=1)
    best_candidates = candidates[candidates['dist_score'] < 0.05].copy()
    
    # 6. Commercial Fleet Utility Calculation
    if not best_candidates.empty:
        best_candidates['net_utility'] = best_candidates.apply(
            lambda row: calculate_detour_utility(
                row, route_dist, market_avg, 
                tank_capacity, current_fuel, km_per_liter, hourly_wage
            ), axis=1
        )
        
        # Sort by Utility (High to Low)
        best_candidates = best_candidates.sort_values('net_utility', ascending=False).head(15)
    
    return {
        'start': {'lat': lat1, 'lon': lon1, 'name': name1},
        'end': {'lat': lat2, 'lon': lon2, 'name': name2},
        'stations': best_candidates,
        'route_path': route_path,
        'distance_km': route_dist,
        'market_avg': market_avg
    }

def find_stations_on_route(start_address, end_address):
    # Wrapper for backward compatibility
    result = optimize_route(start_address, end_address)
    if result and not result['stations'].empty:
        print(f"\n⛽ BEST FUEL ON YOUR ROUTE (Top 10 by Net Utility):")
        print("-" * 90)
        print(f"{'Price':<8} | {'Utility ($)':<12} | {'Station Name':<35}")
        print("-" * 90)
        
        for _, row in result['stations'].iterrows():
            print(f"{row['price_cpl']:.1f}c | ${row['net_utility']:<11.2f} | {row['name']:<35}")
        print("-" * 90)
    elif result:
        print("⚠️ No stations found in the corridor.")

if __name__ == "__main__":
    print("🛣️  FUEL ROUTE OPTIMIZER (Fleet Edition)")
    s = input("📍 Start Address: ")
    e = input("🏁 End Address:   ")
    find_stations_on_route(s, e)
