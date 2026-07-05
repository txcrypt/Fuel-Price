# Australian Fuel Intelligence Dashboard

FastAPI + SQLite + Leaflet + Chart.js dashboard for Australian fuel-price decisions and market analysis.

## Current Product Shape

- **Today:** fill/wait recommendation, confidence, expected saving, cheapest visible station, alerts, and evidence cards.
- **Map:** searchable and filterable live station map.
- **Drive:** permission-gated nearby station lookup for moving use.
- **Intelligence:** market context, forecasts, supply/news views, technical data quality, source explorer, and password-gated Advanced AI.
- **More:** local profile settings for fuel type, tank size, suburbs, brands, and alert threshold.

## Setup

1. Install Python dependencies:

   ```powershell
   pip install -r requirements.txt
   ```

2. Configure environment variables in `.env` or the host environment:

   ```text
   FUEL_API_TOKEN=your_qld_fuel_api_token
   GEMINI_API_KEY=optional_backend_only_key
   ADVANCED_PASSWORD=txcrypt
   ADVANCED_SESSION_SECRET=change_me
   CORS_ORIGINS=http://localhost:8000,http://127.0.0.1:8000
   AISSTREAM_API_KEY=optional
   ```

3. Run the app:

   ```powershell
   uvicorn fuel_dashboard:app --host 0.0.0.0 --port 8000
   ```

4. Open `http://localhost:8000`.

## Data Sources And Fallbacks

- Live station prices are loaded from SQLite first, then `live_snapshot.csv` / `brisbane_fuel_live_collection.csv`.
- QLD live refresh requires `FUEL_API_TOKEN`; WA uses FuelWatch RSS.
- TGP uses AIP first, Viva fallback, then emergency fallback if both fail.
- Market data uses Frankfurter/yfinance with synthetic fallback.
- Gemini is backend-only. If `GEMINI_API_KEY` is missing, Advanced mode returns deterministic local summaries.

## Key API Endpoints

- `GET /api/bootstrap`
- `GET /api/recommendation`
- `GET /api/data-health`
- `GET /api/technical/summary`
- `GET /api/technical/source-explorer`
- `GET /api/technical/export`
- `POST /api/advanced/verify`
- `POST /api/advanced/ask`
- `GET /api/advanced/briefing`
- `POST /api/advanced/shock`

## Verification

Run syntax checks:

```powershell
python -m py_compile fuel_dashboard.py config.py fuel_engine.py advanced_ai.py data_store.py tgp_forecast.py
node --check static\app.js
```
