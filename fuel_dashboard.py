"""
🚀 BRISBANE FUEL AI DASHBOARD
-----------------------------
Combined Edition: Standard Consumer Features + Advanced Commercial Analytics
"""

import streamlit as st
import pandas as pd
import sys
import os
import json
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
import streamlit.components.v1 as components
from datetime import datetime, timedelta

# --- Configuration & Setup ---
st.set_page_config(
    page_title="Brisbane Fuel AI",
    page_icon="⛽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Import Modules
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import station_fairness
import cycle_prediction
import tgp_forecast
import route_optimizer
import data_collector
import station_metadata
import market_news
import backtester

# --- Styling & Assets ---
def load_css():
    try:
        with open("assets/style.css") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except: pass

load_css()



# --- Data Loading & Caching ---
@st.cache_data(ttl=3600)
def get_cached_trend():
    return tgp_forecast.analyze_trend()

@st.cache_data(ttl=3600)
def get_cached_daily_data():
    return cycle_prediction.load_daily_data()

@st.cache_data(ttl=3600)
def get_cached_cycle_prediction():
    file_path = os.path.join(os.path.dirname(__file__), "cycle_prediction.json")
    
    # Auto-run analysis if missing
    if not os.path.exists(file_path):
        try:
            cycle_prediction.main()
        except: pass

    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            try:
                return json.load(f)
            except: return None
    return None

@st.cache_data(ttl=3600)
def get_cached_ratings():
    ratings_file = os.path.join(os.path.dirname(__file__), "station_ratings.csv")
    if os.path.exists(ratings_file):
        return pd.read_csv(ratings_file)
    return None

@st.cache_data(ttl=300)
def get_cached_live_data():
    if os.path.exists(data_collector.COLLECTION_FILE):
        try:
            return pd.read_csv(data_collector.COLLECTION_FILE)
        except:
            return pd.DataFrame()
    return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_cached_metadata():
    file_path = station_metadata.METADATA_FILE
    if not os.path.exists(file_path):
        station_metadata.generate_metadata()
    
    if os.path.exists(file_path):
        # Force types to string for matching
        return pd.read_csv(file_path, dtype={'site_id': str, 'postcode': str})
    return None

def render_live_ticker():
    """
    Renders a scrolling ticker with live/simulated market data.
    """
    try:
        # Fetch trend data for ticker - USE CACHED VERSION
        trend = get_cached_trend()
        if not trend: return # Handle empty cache gracefully

        oil = trend.get('current_oil', 75.42)
        mogas = trend.get('current_mogas', oil + 12)
        tgp = trend.get('current_tgp', 172.5)
        
        # Get dynamic news
        news_item = market_news.get_latest_headline()
        
        ticker_html = f"""
        <div style="
            width: 100%;
            background-color: #1e293b;
            color: #e2e8f0;
            padding: 8px 0;
            font-family: monospace;
            font-size: 14px;
            border-bottom: 1px solid #334155;
            margin-bottom: 20px;
            white-space: nowrap;
            overflow: hidden;
            box-sizing: border-box;
        ">
            <div style="display: inline-block; padding-left: 100%; animation: ticker 45s linear infinite;">
                <span style="margin-right: 50px;">🛢️ Mogas 95: <b>${mogas:.2f}</b></span>
                <span style="margin-right: 50px;">🏭 TGP (Bne): <b>{tgp:.2f}c</b></span>
                <span style="margin-right: 50px;">🛢️ Brent: <b>${oil:.2f}</b></span>
                <span style="margin-right: 50px;">🏛️ Govt Excise: <b>49.6c/L</b></span>
                <span style="margin-right: 50px;">⚓ Wharfage: <b>~3.5c/L</b></span>
                <span style="margin-right: 50px;">⛽ Market Avg: <b>180.0c</b></span>
                <span style="margin-right: 50px;">{news_item}</span>
            </div>
        </div>
        <style>
        @keyframes ticker {{
            0% {{ transform: translate3d(0, 0, 0); }}
            100% {{ transform: translate3d(-100%, 0, 0); }}
        }}
        </style>
        """
        st.markdown(ticker_html, unsafe_allow_html=True)
    except: pass

def get_live_retail_avg():
    """Calculate real-time average retail price."""
    df = get_cached_live_data()
    if not df.empty:
        ts_col = 'scraped_at' if 'scraped_at' in df.columns else 'reported_at'
        if ts_col in df.columns:
            df['ts'] = pd.to_datetime(df[ts_col], errors='coerce')
            cutoff = pd.Timestamp.now() - pd.Timedelta(hours=48)
            recent = df[df['ts'] > cutoff]
            if not recent.empty:
                return recent['price_cpl'].median(), "Live (48h)"
    
    daily_df = get_cached_daily_data()
    if daily_df is not None and not daily_df.empty:
        return daily_df['price_cpl'].iloc[-1], "History (Fallback)"
    return 0.0, "None"

# --- UI Components ---

def render_metric_card(title, value, delta=None, help_text=None, color="white"):
    """Custom styled metric card using new CSS."""
    delta_color = '#94a3b8' # Default neutral
    delta_display = ''

    if delta is not None:
        delta_str = str(delta)
        if '+' in delta_str:
            delta_color = '#10b981' # Green
            delta_display = f"▲ {delta_str}"
        elif '-' in delta_str:
            delta_color = '#ef4444' # Red
            delta_display = f"▼ {delta_str}"
        else:
            delta_display = delta_str
    
    html = f"""
    <div class="card-container">
        <div class="card-title">{title}</div>
        <div class="card-value" style="color: {color}">{value}</div>
        <div class="card-delta" style="color: {delta_color}">{delta_display}</div>
        <div style="font-size: 0.75rem; color: #64748b; margin-top: 8px;">{help_text if help_text else ''}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def render_hero_header():
    st.markdown("""
    <div class="hero-container">
        <div class="hero-title">Brisbane Fuel AI</div>
        <div class="hero-subtitle">Real-time market intelligence, predictive analytics, and fleet optimization for Queensland fuel markets.</div>
    </div>
    """, unsafe_allow_html=True)

# --- View Functions ---

def view_live_market():
    st.markdown("## 📊 Live Market Analysis")
    
    col1, col2 = st.columns(2)
    
    daily_df = get_cached_daily_data()
    trend = get_cached_trend()
    avg_retail, source = get_live_retail_avg()
    live_df = get_cached_live_data()

    # Check for Leader Hike
    hike_status = "STABLE"
    if not live_df.empty:
        hike_status = cycle_prediction.detect_leader_hike(live_df)
    
    if hike_status == "HIKE_STARTED":
        st.error("⚠️ ALERT: Prices are spiking! Market leaders have hiked. Fill up immediately.")

    with col1:
        st.subheader("🏭 Market Vital Signs")
        if trend:
            # Main KPIs
            kpi1, kpi2, kpi3 = st.columns(3)
            
            forecast_gap = trend['forecast_tgp'] - trend['current_tgp']
            wholesale_margin = avg_retail - trend['current_tgp']
            
            with kpi1:
                mogas_val = trend.get('current_mogas', trend['current_oil'] + 12)
                render_metric_card("Mogas 95 (Sing)", f"${mogas_val:.2f}", f"{trend['oil_trend_pct']:.2f}%", color="#3b82f6")
            with kpi2:
                render_metric_card("Wholesale (TGP)", f"{trend['current_tgp']:.2f}c", f"{forecast_gap:.2f}c", color="#8b5cf6")
            with kpi3:
                color = "#ef4444" if wholesale_margin < 5 else "#10b981"
                render_metric_card("Retail Margin", f"{wholesale_margin:.1f}c", None, color=color)
            
            # Context
            if wholesale_margin < 5.0:
                st.warning("⚠️ Retail margins are razor thin. A price HIKE is highly likely.")
            elif wholesale_margin > 25.0:
                st.success("✅ Margins are healthy. Prices have room to drop further.")
            
            # Savings Calculator
            st.markdown("---")
            st.write("💰 **Savings Calculator**")
            
            tank_opt = st.radio("Select Tank Size:", ["Small (40L)", "Medium (50L)", "Large (80L)"], horizontal=True)
            tank_map = {"Small (40L)": 40, "Medium (50L)": 50, "Large (80L)": 80}
            tank_size = tank_map[tank_opt]
            
            potential_save = (40.0 * tank_size) / 100
            
            if wholesale_margin < 10:
                 st.info(f"If you fill up NOW instead of post-hike, you save approx **${potential_save:.2f}**.")
            else:
                 st.info(f"Prices are okay. Filling now vs the absolute bottom might cost you ~${(wholesale_margin * tank_size / 100):.2f}.")

    with col2:
        st.subheader("⏳ Cycle Countdown")
        if daily_df is not None and not daily_df.empty:
            avg_len, avg_relent, last_hike = cycle_prediction.analyze_cycles(daily_df)
            status = cycle_prediction.predict_status(avg_relent, last_hike)
            
            if status:
                # Recommendation logic upgrade
                rec_msg = "Unknown"
                rec_type = "info"
                
                # Override if hike started
                if hike_status == "HIKE_STARTED":
                    status['status'] = 'HIKE_STARTED'
                
                if status['status'] == 'HIKE' or status['status'] == 'HIKE_STARTED':
                    rec_msg = "🔴 Status: FILL IMMEDIATELY (Prices Rising)"
                    rec_type = "error"
                elif status['status'] == 'OVERDUE':
                    rec_msg = "🟠 Status: WARNING (Hike Imminent)"
                    rec_type = "warning"
                elif status['status'] == 'BOTTOM':
                    rec_msg = "🟢 Status: BUY NOW (Good Price)"
                    rec_type = "success"
                elif status['status'] == 'DROPPING':
                     rec_msg = "🔵 Status: WAIT (Prices Falling)"
                     rec_type = "info"
                
                if rec_type == "error": st.error(rec_msg)
                elif rec_type == "warning": st.warning(rec_msg)
                elif rec_type == "success": st.success(rec_msg)
                else: st.info(rec_msg)

                # Metrics Row 1
                m1, m2 = st.columns(2)
                with m1:
                    st.metric("Cycle Phase", status['status'])
                with m2:
                    st.metric("Avg Cycle Length", f"{int(avg_len)} Days")
                
                # Metrics Row 2
                m3, m4 = st.columns(2)
                with m3:
                    next_hike_est = last_hike + timedelta(days=avg_len)
                    days_left = (next_hike_est - datetime.now()).days
                    st.metric("Est. Next Hike", next_hike_est.strftime("%d %b"))
                with m4:
                     st.metric("Days Remaining", f"{max(0, days_left)} Days")
                    
                # Progress
                prog = min(1.0, max(0.0, status['days_elapsed'] / avg_len))
                st.progress(prog)
                st.caption(f"Day {status['days_elapsed']} of ~{int(avg_len)} Day Cycle")
                
                # Cycle Countdown Graph
                if 'last_hike' in status:
                    try:
                        last_hike_dt = pd.to_datetime(status['last_hike'])
                        dates = [last_hike_dt + timedelta(days=i) for i in range(int(avg_len) + 5)]
                        y_vals = []
                        for i, d in enumerate(dates):
                            day_num = (d - last_hike_dt).days
                            if day_num < 2: val = 100
                            elif day_num < avg_len * 0.8: val = 100 - (day_num * (80 / (avg_len * 0.8)))
                            else: val = 20
                            y_vals.append(val)
                            
                        cycle_fig = go.Figure()
                        cycle_fig.add_trace(go.Scatter(x=dates, y=y_vals, mode='lines', line=dict(color='#f59e0b', width=2)))
                        today_val = y_vals[min(len(y_vals)-1, status['days_elapsed'])]
                        cycle_fig.add_trace(go.Scatter(x=[datetime.now()], y=[today_val], mode='markers', marker=dict(color='white', size=10)))
                        cycle_fig.update_layout(height=200, margin=dict(l=20, r=20, t=30, b=20), showlegend=False, template="plotly_dark", yaxis=dict(showticklabels=False))
                        st.plotly_chart(cycle_fig, use_container_width=True)
                    except: pass

    # --- Map Section ---
    st.markdown("### 🗺️ Live Price Map")
    
    if not live_df.empty:
        # Map Controls
        mc1, mc2 = st.columns([3, 1])
        with mc1:
            search_query = st.text_input("Search Suburb or Postcode", placeholder="e.g. 4000 or Brisbane City")
        with mc2:
            show_deals = st.checkbox("Show Deals Only", value=True)

        metadata = get_cached_metadata()
        if metadata is not None:
            def clean_id(x):
                try: return str(int(float(x)))
                except: return str(x)
            live_df['site_id'] = live_df['site_id'].apply(clean_id)
            metadata['site_id'] = metadata['site_id'].apply(clean_id)
            map_df = live_df.merge(metadata, on='site_id', how='left', suffixes=('', '_meta')) 
            if 'latitude_meta' in map_df.columns: map_df['latitude'] = map_df['latitude'].fillna(map_df['latitude_meta'])
            if 'longitude_meta' in map_df.columns: map_df['longitude'] = map_df['longitude'].fillna(map_df['longitude_meta'])
        else:
            map_df = live_df.copy()
        
        # Filter Deals
        if show_deals:
            cutoff_price = avg_retail - 5.0
            map_df = map_df[map_df['price_cpl'] < cutoff_price]
            st.caption(f"Showing stations below {cutoff_price:.1f}c")

        # Filter Search
        if search_query and not map_df.empty:
            mask = pd.Series(False, index=map_df.index)
            if 'suburb' in map_df.columns: mask |= map_df['suburb'].astype(str).str.contains(search_query, case=False, na=False)
            if 'postcode' in map_df.columns:
                map_df['postcode_str'] = map_df['postcode'].apply(lambda x: str(int(x)) if pd.notnull(x) and isinstance(x, (int, float)) else str(x))
                mask |= map_df['postcode_str'].str.contains(search_query, case=False, na=False)
            map_df = map_df[mask]

        # Render
        if 'latitude' in map_df.columns and 'longitude' in map_df.columns:
            map_df = map_df.dropna(subset=['latitude', 'longitude', 'price_cpl'])
            if not map_df.empty:
                # Use Display Brand if available
                if 'display_brand' in map_df.columns:
                    map_df['display_name'] = map_df['display_brand'] + " " + map_df['site_id'].astype(str)
                elif 'name' in map_df.columns:
                    map_df['display_name'] = map_df['name']
                else:
                    map_df['display_name'] = map_df['site_id'].astype(str)

                fig_map = px.scatter_mapbox(
                    map_df, lat="latitude", lon="longitude", color="price_cpl",
                    color_continuous_scale="RdYlGn_r", size_max=15, zoom=10,
                    hover_name="display_name", hover_data={"price_cpl": True}
                )
                fig_map.update_traces(marker=dict(size=12))
                fig_map.update_layout(mapbox_style="open-street-map", margin={"r":0,"t":0,"l":0,"b":0}, height=500)
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.warning("No stations found matching criteria.")
    else:
        st.warning("No live data available.")

def view_global_sentiment():
    st.markdown("## 🌍 Global Market Sentiment")
    
    # 1. Fetch Data
    sentiment = market_news.get_market_sentiment()
    
    # 2. Hero Stats
    col1, col2, col3 = st.columns(3)
    with col1:
        render_metric_card("Market Mood", sentiment['mood'], None, "Derived from global news analysis", color=sentiment['color'])
    with col2:
        render_metric_card("Sentiment Score", f"{sentiment['score']}/10", None, "Positive = Bearish (Low Prices), Negative = Bullish")
    with col3:
        render_metric_card("Articles Analyzed", len(sentiment['articles']), None, "Source: Yahoo Finance (Energy)")

    # 3. Gauge Chart
    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = sentiment['score'],
        title = {'text': "Fear & Greed Index (Energy)"},
        gauge = {
            'axis': {'range': [-10, 10]},
            'bar': {'color': "white"},
            'steps': [
                {'range': [-10, -2], 'color': "#ef4444"},
                {'range': [-2, 2], 'color': "#f59e0b"},
                {'range': [2, 10], 'color': "#10b981"}
            ],
            'threshold': {
                'line': {'color': "white", 'width': 4},
                'thickness': 0.75,
                'value': sentiment['score']
            }
        }
    ))
    fig.update_layout(height=300, margin=dict(l=20, r=20, t=50, b=20), paper_bgcolor="rgba(0,0,0,0)", font={'color': "white"})
    st.plotly_chart(fig, use_container_width=True)
    
    # 4. News Feed
    st.subheader("📰 Latest Energy Headlines")
    for article in sentiment['articles']:
        st.markdown(f"""
        <div class="news-item">
            <div class="news-title"><a href="{article['link']}" target="_blank" style="text-decoration:none; color:inherit;">{article['title']}</a></div>
            <div class="news-meta">
                <span>{article['publisher']}</span>
                <span>{article['sentiment']}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

def view_ratings_simple():
    st.markdown("# ⚖️ Station Fairness Ratings")
    st.write("Identifying stations that are consistently cheaper or expensive relative to the market.")
    
    ratings = get_cached_ratings()
    metadata = get_cached_metadata() # Get names if needed
    
    col_actions, _ = st.columns([1, 3])
    with col_actions:
        if st.button("🔄 Recalculate Ratings"):
            with st.spinner("Running Analysis (this may take a moment)..."):
                try:
                    station_fairness.main()
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Analysis failed: {e}")

    if ratings is None:
        st.info("No ratings found. Click 'Recalculate Ratings' to generate them.")
    else:
        col1, col2 = st.columns(2)
        
        # Ensure we have good names using Metadata
        if metadata is not None:
            ratings['site_id'] = ratings['site_id'].astype(str)
            metadata['site_id'] = metadata['site_id'].astype(str)
            ratings = ratings.merge(metadata[['site_id', 'name', 'suburb']], on='site_id', how='left', suffixes=('_old', ''))
            if 'name' not in ratings.columns: ratings['name'] = ratings['name_old']
            if 'suburb' not in ratings.columns: ratings['suburb'] = ratings['suburb_old']
        
        # Use simple columns for V2 view
        cols = ['name', 'suburb', 'fairness_score', 'rating']
        if 'name' not in ratings.columns: ratings['name'] = ratings['site_id']
        if 'suburb' not in ratings.columns: ratings['suburb'] = "Unknown"
        
        with col1:
            st.subheader("🏆 Best Value")
            
            df_best = ratings.sort_values('fairness_score').head(15)[cols]
            st.dataframe(
                df_best.style.background_gradient(subset=['fairness_score'], cmap='Greens_r'),
                hide_index=True,
                use_container_width=True
            )
        
        with col2:
            st.subheader("💸 Most Expensive")
            df_exp = ratings.sort_values('fairness_score', ascending=False).head(15)[cols]
            st.dataframe(
                df_exp.style.background_gradient(subset=['fairness_score'], cmap='Reds'),
                hide_index=True,
                use_container_width=True
            )

def view_planner_simple():
    st.markdown("# 🛣️ Smart Route Planner")
    
    tab_commute, tab_trip = st.tabs(["🚗 Daily Commute", "🚛 Long Trip"])
    
    with tab_commute:
        st.subheader("Start vs End: Where should I fill?")
        c1, c2 = st.columns(2)
        home = c1.text_input("Home / Start", "Brisbane City", key="c_start")
        work = c2.text_input("Work / End", "Ipswich", key="c_end")
        
        if st.button("Analyze Commute"):
            with st.spinner("Analyzing price zones..."):
                res = route_optimizer.analyze_commute_route(home, work)
                if res and "error" not in res:
                    st.info(f"Price Difference: {abs(res['diff']):.1f}c")
                    
                    if res['action'] == 'FILL_NOW':
                        st.success(f"✅ **ADVICE: {res['advice']}**")
                    elif res['action'] == 'WAIT':
                        st.warning(f"🛑 **ADVICE: {res['advice']}**")
                    else:
                        st.info(f"⚖️ **ADVICE: {res['advice']}**")
                        
                    m1, m2 = st.columns(2)
                    m1.metric(res['start_name'], f"{res['start_price']:.1f}c")
                    m2.metric(res['end_name'], f"{res['end_price']:.1f}c")
                elif res:
                    st.error(res['error'])
                else:
                    st.error("Could not resolve locations.")

    with tab_trip:
        st.write("Find the best station along a route.")
        col1, col2 = st.columns(2)
        start = col1.text_input("Start Address", "Brisbane Airport", key="t_start")
        end = col2.text_input("End Address", "Gold Coast", key="t_end")
        
        if 'simple_route_res' not in st.session_state: st.session_state.simple_route_res = None

        if st.button("Find Stations on Route"):
            with st.spinner("Calculating..."):
                try:
                    res = route_optimizer.optimize_route(start, end)
                    st.session_state.simple_route_res = res
                except Exception as e:
                    st.error(f"Error: {e}")

        if st.session_state.simple_route_res:
            result = st.session_state.simple_route_res
            if result and not result['stations'].empty:
                stations = result['stations']
                st.subheader("⛽ Recommended Stations")
                st.dataframe(stations[['price_cpl', 'name', 'postcode']].sort_values('price_cpl'), hide_index=True)
                
                # Simple Map
                mid_lat = (result['start']['lat'] + result['end']['lat']) / 2
                mid_lon = (result['start']['lon'] + result['end']['lon']) / 2
                
                # Centering Layout
                c_map1, c_map2, c_map3 = st.columns([1, 6, 1])
                with c_map2:
                    m = folium.Map(location=[mid_lat, mid_lon], zoom_start=10)
                    if result.get('route_path'): folium.PolyLine(result['route_path'], color="blue", weight=5).add_to(m)
                    for _, row in stations.iterrows():
                        folium.Marker([row['latitude'], row['longitude']], popup=f"{row['name']}: {row['price_cpl']}c").add_to(m)
                    st_folium(m, height=400, use_container_width=True)
            else:
                st.error("No stations found.")

def view_advanced_analytics():
    st.markdown("# 📈 Advanced Commercial Analytics")
    
    live_df = get_cached_live_data()
    
    # Moved charts here
    if not live_df.empty:
        st.subheader("Market Distribution")
        ac1, ac2 = st.columns(2)
        with ac1:
            # CLEAN DATA FOR HISTOGRAM
            # Force numeric, drop errors
            clean_prices = pd.to_numeric(live_df['price_cpl'], errors='coerce')
            clean_df = live_df.copy()
            clean_df['price_cpl'] = clean_prices
            clean_df = clean_df.dropna(subset=['price_cpl'])
            clean_df = clean_df[clean_df['price_cpl'] > 10.0] # Remove placeholders like 0
            
            if not clean_df.empty:
                fig_hist = px.histogram(clean_df, x='price_cpl', nbins=30, title="Price Histogram", color_discrete_sequence=['#3b82f6'])
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.info("Insufficient valid price data for histogram.")
                
        with ac2:
            metadata = get_cached_metadata()
            if metadata is not None:
                # Robust ID Cleaning
                def clean_id(x):
                    try: 
                        # Handle float-as-string "123.0" -> "123"
                        return str(int(float(x)))
                    except: 
                        return str(x).strip()

                live_df['site_id'] = live_df['site_id'].apply(clean_id)
                metadata['site_id'] = metadata['site_id'].apply(clean_id)
                
                analysis_df = live_df.merge(metadata[['site_id', 'suburb']], on='site_id', how='left')
                
                if 'suburb' in analysis_df.columns:
                    # Filter out bad suburbs
                    valid_subs = analysis_df[analysis_df['suburb'].notna() & (analysis_df['suburb'] != "Unknown")]
                    
                    if not valid_subs.empty:
                        cheap_subs = valid_subs.groupby('suburb')['price_cpl'].mean().sort_values().head(10).reset_index()
                        cheap_subs.columns = ['Suburb', 'Avg Price (c/L)']
                        st.write("**Cheapest Suburbs**")
                        st.dataframe(cheap_subs.style.format({'Avg Price (c/L)': '{:.1f}'}), use_container_width=True, hide_index=True)
                    else:
                        st.info("No suburb data available after merge.")
                else:
                    st.warning("Suburb column missing after merge.")
            else:
                st.warning("Metadata unavailable for suburb ranking.")

    tab1, tab2, tab3, tab4 = st.tabs([
        "🧠 Econometric Forecast", 
        "🛰️ Spatial Intelligence", 
        "🚛 Fleet Optimizer", 
        "♟️ Game Theory"
    ])
    
    # --- Tab 1: Econometrics ---
    with tab1:
        st.subheader("SARIMAX & Regime Switching Models")
        trend = get_cached_trend()
        
        if trend:
            c1, c2 = st.columns(2)
            with c1:
                fcst = trend.get('forecast_tgp', 0)
                curr = trend.get('current_tgp', 0)
                render_metric_card("14-Day TGP Target", f"{fcst:.2f}c", f"{(fcst-curr):+.2f}c", color="#8b5cf6")
            with c2:
                regime = trend.get('regime', 'Unknown')
                prob = trend.get('regime_prob', 0) * 100
                render_metric_card("Market Regime", regime.split(' ')[0], f"{prob:.0f}% Probability", color="#f59e0b")
            
            if 'sarimax' in trend and trend['sarimax']:
                sx = trend['sarimax']
                dates = pd.to_datetime(sx['forecast_dates'])
                
                fig = go.Figure()
                # History
                if 'history' in trend:
                    h_dates = pd.to_datetime(trend['history']['date'])
                    h_vals = trend['history']['tgp']
                    fig.add_trace(go.Scatter(x=h_dates, y=h_vals, name="Historical TGP", line=dict(color='gray')))
                
                # Forecast
                fig.add_trace(go.Scatter(x=dates, y=sx['forecast_mean'], name="Forecast Mean", line=dict(color='#3b82f6')))
                # CI
                fig.add_trace(go.Scatter(
                    x=dates.tolist() + dates.tolist()[::-1],
                    y=sx['upper_ci'] + sx['lower_ci'][::-1],
                    fill='toself', fillcolor='rgba(59, 130, 246, 0.2)',
                    line=dict(color='rgba(0,0,0,0)'), name='95% Confidence'
                ))
                fig.update_layout(template="plotly_dark", height=400, margin=dict(t=20, b=20, l=20, r=20))
                st.plotly_chart(fig, use_container_width=True)
    
    # --- Tab 2: Spatial ---
    with tab2:
        st.subheader("Geospatial Cluster Analysis (Local Moran's I)")
        ratings = get_cached_ratings()
        
        if ratings is not None and 'spatial_cluster' in ratings.columns:
            col1, col2 = st.columns([3, 1])
            with col2:
                st.info("Hot Spots (Red) indicate statistically significant clusters of high prices.")
                view_type = st.radio("Filter", ["All", "Hot Spots", "Cold Spots"])
            
            with col1:
                m = folium.Map(location=[-27.47, 153.02], zoom_start=11, tiles="CartoDB dark_matter")
                
                plot_df = ratings.copy()
                if view_type == "Hot Spots": plot_df = plot_df[plot_df['spatial_cluster'].str.contains("Hot")]
                if view_type == "Cold Spots": plot_df = plot_df[plot_df['spatial_cluster'].str.contains("Cold")]
                
                if len(plot_df) > 500: plot_df = plot_df.head(500)
                
                for _, row in plot_df.iterrows():
                    color = "gray"
                    if "Hot" in row['spatial_cluster']: color = "red"
                    elif "Cold" in row['spatial_cluster']: color = "green"
                    
                    folium.CircleMarker(
                        [row['latitude'], row['longitude']], radius=5, color=color, fill=True,
                        popup=f"{row['name']}: {row['spatial_cluster']}"
                    ).add_to(m)
                st_folium(m, height=500, width="100%")
        else:
            st.warning("No spatial data. Go to Station Ratings and click 'Analyze History' first.")

    # --- Tab 3: Fleet ---
    with tab3:
        st.subheader("Utility-Based Fleet Routing")
        c1, c2, c3, c4 = st.columns(4)
        s = c1.text_input("Start", "Brisbane Airport", key="f_s")
        e = c2.text_input("End", "Ipswich", key="f_e")
        tank = c3.number_input("Tank (L)", 50, 1000, 100)
        wage = c4.number_input("Wage ($/hr)", 0, 200, 45)
        
        if 'fleet_res' not in st.session_state: st.session_state.fleet_res = None
        
        if st.button("Optimize Fleet Route"):
            with st.spinner("Calculating Utility..."):
                st.session_state.fleet_res = route_optimizer.optimize_route(
                    s, e, tank_capacity=tank, hourly_wage=wage
                )
        
        if st.session_state.fleet_res:
            res = st.session_state.fleet_res
            if not res['stations'].empty:
                best = res['stations'].iloc[0]
                st.success(f"Recommended Stop: **{best['name']}** (Net Utility: ${best['net_utility']:.2f})")
                st.dataframe(res['stations'][['name', 'price_cpl', 'net_utility', 'dist_score']].head(10))

    # --- Tab 4: Game Theory ---
    with tab4:
        st.subheader("Competitor Profiling")
        cycle = get_cached_cycle_prediction()
        if cycle and 'market_leaders' in cycle:
            leaders = cycle['market_leaders']
            st.write("#### Granger Causality: Market Leaders")
            if leaders:
                for k, v in leaders.items():
                    st.markdown(f"- **{k}**: {v}")
            else:
                st.info("No clear leaders found.")
        else:
            st.warning("Cycle data missing. Run analysis.")

def view_collector():
    st.markdown("# 💾 Data Collector")
    st.write("Fetch real-time data from the external API.")

    last_run_time = None
    if os.path.exists(data_collector.COLLECTION_FILE):
        try:
            mtime = os.path.getmtime(data_collector.COLLECTION_FILE)
            last_run_time = datetime.fromtimestamp(mtime)
        except: pass
    
    can_run = True
    msg = "Ready to collect."
    if last_run_time:
        diff = datetime.now() - last_run_time
        # 15 minute cooldown (900 seconds)
        cooldown = 900
        if diff.total_seconds() < cooldown:
            can_run = False
            wait_mins = int((cooldown - diff.total_seconds()) / 60) + 1
            msg = f"⚠️ Cooldown active. Last run: {last_run_time.strftime('%H:%M')}. Please wait {wait_mins} min(s)."
        else:
             msg = f"Last run: {last_run_time.strftime('%Y-%m-%d %H:%M')}"
    
    st.caption(msg)

    if can_run:
        if st.button("Trigger Snapshot"):
            with st.spinner("Collecting live data..."):
                count = data_collector.collect_live_data()
                if count > 0:
                    st.success(f"Done. Collected {count} records.")
                else:
                    st.warning("No records collected (check logs or API status).")
                st.rerun()
    else:
        st.button("Trigger Snapshot", disabled=True, help="Rate limit active to protect API quota.")
        st.warning(msg)

def view_sandbox():
    st.markdown("## 🧪 Algorithm Sandbox")
    st.write("Upload a historical or test CSV file to validate the prediction engine.")
    
    uploaded_file = st.file_uploader("Upload CSV (Required columns: price_cpl, reported_at/scraped_at)", type=["csv"])
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            
            # Normalization
            rename_map = {
                'TransactionDateutc': 'reported_at', 'Price': 'price_cpl', 
                'SiteId': 'site_id', 'Brand': 'brand'
            }
            df.rename(columns=rename_map, inplace=True)
            
            if 'price_cpl' not in df.columns:
                st.error("❌ CSV must contain a 'price_cpl' column.")
                return

            # Basic Stats
            st.success(f"Loaded {len(df)} records.")
            c1, c2, c3 = st.columns(3)
            c1.metric("Avg Price", f"{df['price_cpl'].mean():.2f}c")
            c2.metric("Min Price", f"{df['price_cpl'].min():.2f}c")
            c3.metric("Max Price", f"{df['price_cpl'].max():.2f}c")
            
            # 1. Standard Analysis
            st.markdown("### ⚙️ Current Status Analysis")
            ts_col = 'reported_at' if 'reported_at' in df.columns else 'scraped_at'
            
            if ts_col in df.columns:
                df['date'] = pd.to_datetime(df[ts_col], errors='coerce').dt.normalize()
                daily_df = df.groupby('date')['price_cpl'].median().reset_index().sort_values('date')
                
                avg_len, avg_relent, last_hike = cycle_prediction.analyze_cycles(daily_df)
                status = cycle_prediction.predict_status(avg_relent, last_hike)
                
                k1, k2 = st.columns(2)
                k1.info(f"**Predicted Phase:** {status.get('status', 'Unknown')}")
                k2.write(f"**Est. Cycle Length:** {avg_len:.1f} days")
                
                # 2. Backtest Section
                st.markdown("---")
                st.markdown("### 🔙 Historical Backtest")
                st.caption("Simulate the algorithm day-by-day to verify accuracy.")
                
                ground_truth_thresh = st.number_input(
                    "Backtest Ground Truth Threshold (c/L)", 
                    min_value=3.0, max_value=20.0, value=5.0, step=0.5,
                    help="The price jump required to define an ACTUAL hike in the historical data."
                )
                
                if st.button("🔄 Run Backtest Validation"):
                    with st.spinner("Running historical simulation..."):
                        metrics, res_df = backtester.run_backtest(df, ground_truth_threshold=ground_truth_thresh)
                        
                        if metrics:
                            # Metrics Grid
                            m1, m2, m3, m4 = st.columns(4)
                            with m1: render_metric_card("Accuracy", f"{metrics['accuracy']:.1%}", None, "Overall Correctness")
                            with m2: render_metric_card("Precision", f"{metrics['precision']:.1%}", None, "False Alarm Rate (High is Good)")
                            with m3: render_metric_card("Recall", f"{metrics['recall']:.1%}", None, "Hikes Caught (High is Good)")
                            with m4: render_metric_card("F1 Score", f"{metrics['f1_score']:.2f}", None, "Balanced Score")
                            
                            # Confusion Matrix
                            st.write("#### Confusion Matrix")
                            cm = metrics['confusion']
                            c_col1, c_col2 = st.columns(2)
                            with c_col1:
                                st.success(f"✅ True Positives (Correct Hikes): {cm['tp']}")
                                st.info(f"☑️ True Negatives (Correct Waits): {cm['tn']}")
                            with c_col2:
                                st.error(f"❌ False Positives (False Alarms): {cm['fp']}")
                                st.warning(f"⚠️ False Negatives (Missed Hikes): {cm['fn']}")
                                
                            # Visualisation
                            st.write("#### Prediction Visualization")
                            fig = go.Figure()
                            
                            # Base Price Line
                            fig.add_trace(go.Scatter(x=res_df['date'], y=res_df['price'], name='Price', line=dict(color='gray', width=1)))
                            
                            # True Positives (Green)
                            tp_df = res_df[(res_df['signal_hike'] == 1) & (res_df['actual_hike'] == 1)]
                            fig.add_trace(go.Scatter(x=tp_df['date'], y=tp_df['price'], mode='markers', name='Correct Hike Alert', marker=dict(color='#10b981', size=8, symbol='triangle-up')))
                            
                            # False Positives (Red)
                            fp_df = res_df[(res_df['signal_hike'] == 1) & (res_df['actual_hike'] == 0)]
                            fig.add_trace(go.Scatter(x=fp_df['date'], y=fp_df['price'], mode='markers', name='False Alarm', marker=dict(color='#ef4444', size=8, symbol='x')))
                            
                            fig.update_layout(template="plotly_dark", height=400, title="Algorithm Signals vs Reality")
                            st.plotly_chart(fig, use_container_width=True)
                            
                        else:
                            st.warning("Insufficient data for backtesting (Need >30 days).")
                            
                # 3. Optimization Section
                if st.button("🚀 Optimize Algorithm"):
                    with st.spinner("Grid searching optimal parameters..."):
                        best_config, msg = backtester.optimize_algorithm(df, ground_truth_threshold=ground_truth_thresh)
                        
                        if "optimal" in msg:
                            st.info(msg)
                        else:
                            st.success(msg)
                            st.session_state['pending_config'] = best_config
                
                if 'pending_config' in st.session_state:
                    st.write(f"Proposed Config: {st.session_state['pending_config']}")
                    if st.button("💾 Save New Configuration"):
                        cycle_prediction.save_config(st.session_state['pending_config'])
                        st.success("Configuration saved! Reloading...")
                        del st.session_state['pending_config']
                        st.rerun()
                        
            else:
                st.warning("No date column found.")
                
        except Exception as e:
            st.error(f"Error processing file: {e}")

# --- Main App Structure ---

# Inject Hero
render_hero_header()

# Restore Live Ticker (Without News, as requested)
try:
    # Fetch trend data for ticker
    trend = get_cached_trend()
    oil = trend.get('current_oil', 75.42)
    mogas = trend.get('current_mogas', oil + 12)
    tgp = trend.get('current_tgp', 172.5)
    
    ticker_html = f"""
    <div style="
        width: 100%;
        background-color: #1e293b;
        color: #e2e8f0;
        padding: 8px 0;
        font-family: monospace;
        font-size: 14px;
        border-bottom: 1px solid #334155;
        margin-bottom: 20px;
        white-space: nowrap;
        overflow: hidden;
        box-sizing: border-box;
    ">
        <div style="display: inline-block; padding-left: 100%; animation: ticker 45s linear infinite;">
            <span style="margin-right: 50px;">🛢️ Mogas 95: <b>${mogas:.2f}</b></span>
            <span style="margin-right: 50px;">🏭 TGP (Bne): <b>{tgp:.2f}c</b></span>
            <span style="margin-right: 50px;">🛢️ Brent: <b>${oil:.2f}</b></span>
            <span style="margin-right: 50px;">🏛️ Govt Excise: <b>49.6c/L</b></span>
            <span style="margin-right: 50px;">⚓ Wharfage: <b>~3.5c/L</b></span>
            <span style="margin-right: 50px;">⛽ Market Avg: <b>180.0c</b></span>
        </div>
    </div>
    <style>
    @keyframes ticker {{
        0% {{ transform: translate3d(0, 0, 0); }}
        100% {{ transform: translate3d(-100%, 0, 0); }}
    }}
    </style>
    """
    st.markdown(ticker_html, unsafe_allow_html=True)
except: pass

# Top Navigation Tabs
tabs = st.tabs(["📊 Live Market", "🌍 Global Sentiment", "⚖️ Station Ratings", "🛣️ Route Planner", "📈 Analytics", "💾 Data", "🧪 Sandbox"])

with tabs[0]:
    view_live_market()
    
with tabs[1]:
    view_global_sentiment()

with tabs[2]:
    view_ratings_simple()
    
with tabs[3]:
    view_planner_simple()

with tabs[4]:
    view_advanced_analytics()

with tabs[5]:
    view_collector()

with tabs[6]:
    view_sandbox()

# Removed Sidebar per request