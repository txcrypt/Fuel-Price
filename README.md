# Brisbane Fuel AI Dashboard ⛽

A comprehensive, AI-powered dashboard for monitoring, predicting, and analyzing fuel prices in the Brisbane market. This application combines real-time data collection, econometric forecasting, and geospatial analysis to help consumers and commercial fleets optimize their refueling strategies.

## 🚀 Features

*   **Live Market Analysis:** Real-time monitoring of fuel prices, TGP (Terminal Gate Price), and market trends.
*   **Cycle Prediction Engine:** Algorithmic prediction of price cycles (Hike, Stable, Drop, Bottom) with backtesting capabilities.
*   **Geospatial Intelligence:** Interactive maps showing price clusters ("Hot Spots" vs. "Cold Spots") and station ratings.
*   **Route Optimizer:**
    *   **Commuter Mode:** Compare prices at home vs. work.
    *   **Fleet Mode:** Optimize refueling stops along long-haul routes based on net utility (price vs. detour cost).
*   **Advanced Analytics:**
    *   SARIMAX Forecasts for wholesale prices.
    *   Competitor Profiling (Game Theory).
    *   Market Sentiment Analysis using Google News RSS.



## 📂 Project Structure

*   `fuel_dashboard.py`: The main entry point for the Streamlit application.
*   `data_collector.py`: Script to fetch live data from the fuel price API.
*   `cycle_prediction.py`: Logic for detecting and predicting market cycles.
*   `backtester.py`: Tools for validating prediction algorithms against historical data.
*   `route_optimizer.py`: Logic for pathfinding and utility-based station selection.
*   `tgp_forecast.py`: Econometric models for wholesale price forecasting.
*   `market_news.py`: Fetches and analyzes energy market sentiment.

## 📊 Data

The application uses a local CSV file (`brisbane_fuel_live_collection.csv`) to store historical price data. 

## ⚠️ Note

This project is configured for the Brisbane, QLD market. API tokens and specific logic (e.g., TGP scraping) are tailored to Australian data sources.
