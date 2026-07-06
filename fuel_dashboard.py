import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')
logger = logging.getLogger("fuel_dashboard")

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

import os
import json
import time
import base64
import hashlib
import hmac
import secrets
import asyncio
import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
from datetime import datetime, timedelta
from functools import lru_cache
import threading
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel
import config
from fuel_engine import FuelEngine

# --- Import Local Modules ---
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import market_physics, tgp_forecast, route_optimizer, market_news
from advanced_ai import AdvancedAIService

# Import new modules (graceful fallback if not yet created)
try:
    from data_store import DataStore
    db = DataStore()
    logger.info("✅ DataStore (SQLite) loaded")
except Exception as e:
    db = None
    logger.warning(f"⚠️ DataStore not available: {e}")

try:
    from predictive_core import DeepCycleModel
    ai_model = DeepCycleModel()
    MODEL_LOADED = ai_model.load("brisbane")
    logger.info(f"{'✅' if MODEL_LOADED else '⚠️'} DeepCycle AI Model {'Loaded' if MODEL_LOADED else 'in fallback mode'}")
except Exception as e:
    ai_model = None
    MODEL_LOADED = False
    logger.warning(f"⚠️ Predictive model not available: {e}")

try:
    from market_context import MarketContextEngine
    market_context = MarketContextEngine()
    logger.info("✅ MarketContext engine loaded")
except Exception as e:
    market_context = None
    logger.warning(f"⚠️ MarketContext not available: {e}")

try:
    from supply_data import SupplyDataEngine
    supply_engine = SupplyDataEngine()
    logger.info("✅ SupplyData engine loaded")
except Exception as e:
    supply_engine = None
    logger.warning(f"⚠️ SupplyData not available: {e}")

try:
    from tanker_tracker import TankerTracker
    tanker_tracker = TankerTracker()
    logger.info("✅ TankerTracker loaded")
except Exception as e:
    tanker_tracker = None
    logger.warning(f"⚠️ TankerTracker not available: {e}")

try:
    from cycle_detector import CycleDetector
    cycle_detector = CycleDetector()
    logger.info("✅ CycleDetector loaded")
except Exception as e:
    cycle_detector = None
    logger.warning(f"⚠️ CycleDetector not available: {e}")

try:
    import station_metadata
except Exception:
    station_metadata = None

advanced_ai = AdvancedAIService()

_advanced_session_secret = config.ADVANCED_SESSION_SECRET or secrets.token_urlsafe(32)
if not config.ADVANCED_SESSION_SECRET:
    logger.warning("ADVANCED_SESSION_SECRET not set; Advanced sessions reset on restart")

# --- FastAPI App ---
app = FastAPI(title="Australian Fuel Intelligence API", version="4.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

_rate_limit_buckets: dict[str, list[float]] = {}


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    detail = exc.detail if isinstance(exc.detail, str) else "Request failed"
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "ok": False,
            "code": f"HTTP_{exc.status_code}",
            "message": detail,
            "details": exc.detail if not isinstance(exc.detail, str) else None,
            "generated_at": datetime.now().isoformat(),
        },
    )

# --- File Paths ---
SNAPSHOT_FILE = os.path.join(config.BASE_DIR, "live_snapshot.csv")
HISTORY_FILE = config.COLLECTION_FILE
CLEAN_HISTORY_FILE = os.path.join(config.BASE_DIR, "brisbane_fuel_history_clean.csv")

PREDICTION_RESEARCH_SOURCES = [
    {
        "label": "ACCC petrol price cycles",
        "url": "https://www.accc.gov.au/consumers/petrol-diesel-lpg/petrol-price-cycles",
        "note": "Retail cycle markets can rise sharply and then discount gradually.",
    },
    {
        "label": "ACCC fuel price drivers",
        "url": "https://www.accc.gov.au/consumers/petrol-and-fuel/what-affects-fuel-prices",
        "note": "International refined fuel costs, exchange rates, taxes and retail cycles all matter.",
    },
    {
        "label": "AIP terminal gate prices",
        "url": "https://www.aip.com.au/pricing/terminal-gate-prices",
        "note": "Wholesale TGP is used as the cost anchor for the retail margin calculation.",
    },
]

# ============================================================
# DATA SYNC
# ============================================================

def fetch_snapshot():
    """Fetch live data from all active state APIs and save."""
    logger.info("📸 Fetching live snapshot...")
    try:
        all_dfs = []
        for state_code in config.ACTIVE_STATES:
            try:
                engine = FuelEngine(state=state_code)
                df = engine.get_market_snapshot()
                if df is not None and not df.empty:
                    all_dfs.append(df)
            except Exception as e:
                logger.error(f"Failed to fetch {state_code}: {e}")

        if not all_dfs:
            return False

        df = pd.concat(all_dfs, ignore_index=True)

        # Save to CSV (backward compat)
        df.to_csv(SNAPSHOT_FILE, index=False)

        # Save to SQLite if available
        if db:
            try:
                inserted = db.save_snapshot(df)
                logger.info(f"💾 SQLite: {inserted} new records inserted")
                # Aggregate daily stats for each state
                for state in df['state'].unique():
                    state_prices = df[df['state'] == state]['price_cpl']
                    db.save_daily_stats(
                        datetime.now().strftime('%Y-%m-%d'),
                        state,
                        state_prices
                    )
            except Exception as e:
                logger.error(f"SQLite save error: {e}")

        # Append to history CSV (rate-limited to 1h)
        _append_to_history(df)

        logger.info(f"✅ Snapshot saved: {len(df)} records across {df['state'].nunique()} states")
        return True
    except Exception as e:
        logger.error(f"❌ Snapshot failed: {e}")
    return False


def _append_to_history(df):
    """Append snapshot to history CSV, rate-limited to once per hour."""
    should_append = False
    if not os.path.exists(HISTORY_FILE):
        should_append = True
    else:
        try:
            last_rows = pd.read_csv(HISTORY_FILE, nrows=0)
            # Read just the last line for timestamp check
            import subprocess
            should_append = True  # Default to append
            try:
                hist_df = pd.read_csv(HISTORY_FILE)
                if not hist_df.empty and 'scraped_at' in hist_df.columns:
                    last_ts = pd.to_datetime(hist_df['scraped_at'].iloc[-1])
                    if (datetime.now() - last_ts).total_seconds() < 3600:
                        should_append = False
            except Exception:
                should_append = True
        except Exception:
            should_append = True

    if should_append:
        cols_to_save = ['site_id', 'price_cpl', 'reported_at', 'region', 'state', 'latitude', 'longitude', 'scraped_at']
        available_cols = [c for c in cols_to_save if c in df.columns]
        if available_cols:
            header = not os.path.exists(HISTORY_FILE)
            df[available_cols].to_csv(HISTORY_FILE, mode='a', header=header, index=False)
            logger.info(f"📜 History appended at {datetime.now().strftime('%H:%M')}")


async def background_refresher():
    """Background task to refresh data every 30 minutes."""
    while True:
        try:
            await asyncio.sleep(1800)
            await run_in_threadpool(fetch_snapshot)
            # Also store TGP and market data
            try:
                for state_code in config.ACTIVE_STATES:
                    capital = config.STATES.get(state_code, {}).get("capital", "BRISBANE")
                    trend = tgp_forecast.analyze_trend(city=capital)
                    if db and trend.get('current_tgp'):
                        db.save_tgp(datetime.now().strftime('%Y-%m-%d'), capital, trend['current_tgp'])
                    oil = _trend_value(trend, "current_oil", "oil_price_usd", fallback=0)
                    fx = _trend_value(trend, "current_fx", "aud_usd", fallback=0.65)
                    if db and oil:
                        db.save_market_data(
                            datetime.now().strftime('%Y-%m-%d'),
                            oil,
                            fx,
                            trend.get('current_mogas', 0)
                        )
            except Exception as e:
                logger.error(f"Market data save error: {e}")
        except Exception:
            await asyncio.sleep(60)


@app.on_event("startup")
async def startup_event():
    logger.info("🚀 Starting Australian Fuel Intelligence API...")
    await run_in_threadpool(fetch_snapshot)

    # Backfill SQLite from CSV if DB is fresh
    if db:
        try:
            existing = db.get_daily_stats("QLD", days=1)
            if existing is None or existing.empty:
                if os.path.exists(HISTORY_FILE):
                    logger.info("📥 Backfilling SQLite from CSV history...")
                    db.backfill_from_csv(HISTORY_FILE)
        except Exception as e:
            logger.warning(f"Backfill skipped: {e}")

    asyncio.create_task(background_refresher())
    logger.info("✅ System Ready.")


# ============================================================
# UTILITIES
# ============================================================

def clean_nan(obj):
    """Recursively clean NaN/Inf values from nested data structures."""
    if isinstance(obj, np.floating):
        return None if (np.isnan(obj) or np.isinf(obj)) else float(obj)
    if isinstance(obj, float):
        return None if (np.isnan(obj) or np.isinf(obj)) else obj
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    return obj


def haversine(lon1, lat1, lon2, lat2):
    """Calculate distance in km between two lat/lng points."""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    a = sin((lat2 - lat1) / 2) ** 2 + cos(lat1) * cos(lat2) * sin((lon2 - lon1) / 2) ** 2
    return 6371 * 2 * asin(sqrt(a))


def _safe_float(value, fallback=None):
    try:
        if value is None:
            return fallback
        number = float(value)
        if np.isnan(number) or np.isinf(number):
            return fallback
        return number
    except (TypeError, ValueError):
        return fallback


def _rate_limit(request: Request, bucket: str, max_requests: int, window_seconds: int) -> None:
    client_host = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
    if not client_host and request.client:
        client_host = request.client.host
    client_host = client_host or "unknown"
    key = f"{bucket}:{client_host}"
    now = time.time()
    window_start = now - window_seconds
    recent = [ts for ts in _rate_limit_buckets.get(key, []) if ts >= window_start]
    if len(recent) >= max_requests:
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Try again shortly.")
    recent.append(now)
    _rate_limit_buckets[key] = recent


def _snapshot_age_minutes() -> float | None:
    if not os.path.exists(SNAPSHOT_FILE):
        return None
    return round((time.time() - os.path.getmtime(SNAPSHOT_FILE)) / 60, 1)


def _freshness_status(age_minutes: float | None, stale_after: int = 90, degraded_after: int = 240) -> str:
    if age_minutes is None:
        return "unavailable"
    if age_minutes > degraded_after:
        return "degraded"
    if age_minutes > stale_after:
        return "stale"
    return "fresh"


@lru_cache(maxsize=1)
def get_cached_metadata():
    """Load station metadata from CSV."""
    try:
        if os.path.exists(config.METADATA_FILE):
            return pd.read_csv(config.METADATA_FILE, dtype={'site_id': str, 'postcode': str})
    except Exception as e:
        logger.error(f"Metadata load error: {e}")
    return pd.DataFrame()


def load_live_data_latest(state="QLD"):
    """Load the most recent live snapshot data."""
    if db and state:
        try:
            db_df = db.get_latest_snapshot(state.upper())
            if db_df is not None and not db_df.empty:
                db_df = db_df.rename(columns={"lat": "latitude", "lng": "longitude"})
                db_df["site_id"] = db_df["site_id"].astype(str)
                return db_df
        except Exception as e:
            logger.debug("SQLite latest snapshot fallback to CSV: %s", e)

    if os.path.exists(SNAPSHOT_FILE):
        try:
            df = pd.read_csv(SNAPSHOT_FILE)
            if not df.empty:
                df['site_id'] = df['site_id'].astype(str)
                if state and 'state' in df.columns:
                    df = df[df['state'] == state].copy()
                return df
        except Exception:
            pass

    if os.path.exists(HISTORY_FILE):
        try:
            df = pd.read_csv(HISTORY_FILE)
            if not df.empty:
                df['site_id'] = df['site_id'].astype(str)
                if state and 'state' in df.columns:
                    df = df[df['state'] == state]
                last_scrape = df['scraped_at'].max()
                return df[df['scraped_at'] == last_scrape].copy()
        except Exception:
            pass
    return pd.DataFrame()


def _create_advanced_token() -> tuple[str, str]:
    expires_at = datetime.utcnow() + timedelta(hours=config.ADVANCED_SESSION_HOURS)
    payload = {
        "scope": "advanced",
        "exp": int(expires_at.timestamp()),
    }
    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_b64 = base64.urlsafe_b64encode(payload_json).decode("ascii").rstrip("=")
    signature = hmac.new(
        _advanced_session_secret.encode("utf-8"),
        payload_b64.encode("ascii"),
        hashlib.sha256,
    ).digest()
    sig_b64 = base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")
    return f"{payload_b64}.{sig_b64}", expires_at.isoformat() + "Z"


def _require_advanced_token(authorization: str | None) -> None:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Advanced authorization required")

    token = authorization.replace("Bearer ", "", 1).strip()
    try:
        payload_b64, sig_b64 = token.split(".", 1)
        expected = hmac.new(
            _advanced_session_secret.encode("utf-8"),
            payload_b64.encode("ascii"),
            hashlib.sha256,
        ).digest()
        actual = base64.urlsafe_b64decode(sig_b64 + "=" * (-len(sig_b64) % 4))
        if not hmac.compare_digest(expected, actual):
            raise ValueError("bad signature")
        payload_raw = base64.urlsafe_b64decode(payload_b64 + "=" * (-len(payload_b64) % 4))
        payload = json.loads(payload_raw.decode("utf-8"))
        if payload.get("scope") != "advanced" or int(payload.get("exp", 0)) < int(time.time()):
            raise ValueError("expired token")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired Advanced session")


def _normalise_api_state(state: str) -> str:
    state_upper = (state or config.DEFAULT_STATE).upper()
    return state_upper if state_upper in config.STATES else config.DEFAULT_STATE


def _summarise_daily_prices(daily_df: pd.DataFrame) -> dict:
    if daily_df is None or daily_df.empty:
        return {"available": False}

    df = daily_df.sort_values("day").tail(90).copy()
    values = pd.to_numeric(df["price_cpl"], errors="coerce").dropna()
    if values.empty:
        return {"available": False}

    latest = float(values.iloc[-1])
    previous = float(values.iloc[-2]) if len(values) > 1 else latest
    seven_ago = float(values.iloc[-8]) if len(values) > 7 else previous

    return {
        "available": True,
        "days": int(len(values)),
        "start_date": df["day"].iloc[0].strftime("%Y-%m-%d"),
        "end_date": df["day"].iloc[-1].strftime("%Y-%m-%d"),
        "latest_median_cpl": round(latest, 1),
        "delta_1d_cpl": round(latest - previous, 1),
        "delta_7d_cpl": round(latest - seven_ago, 1),
        "min_cpl": round(float(values.min()), 1),
        "max_cpl": round(float(values.max()), 1),
        "mean_cpl": round(float(values.mean()), 1),
    }


def _summarise_live_stations(live_df: pd.DataFrame) -> dict:
    if live_df is None or live_df.empty or "price_cpl" not in live_df.columns:
        return {"available": False}

    prices = pd.to_numeric(live_df["price_cpl"], errors="coerce").dropna()
    if prices.empty:
        return {"available": False}

    summary = {
        "available": True,
        "station_count": int(len(prices)),
        "average_cpl": round(float(prices.mean()), 1),
        "median_cpl": round(float(prices.median()), 1),
        "min_cpl": round(float(prices.min()), 1),
        "max_cpl": round(float(prices.max()), 1),
        "dispersion_cpl": round(float(prices.max() - prices.min()), 1),
    }

    if "brand" in live_df.columns:
        brand_stats = (
            live_df.assign(price_cpl=pd.to_numeric(live_df["price_cpl"], errors="coerce"))
            .dropna(subset=["price_cpl"])
            .groupby("brand")["price_cpl"]
            .agg(["mean", "count"])
            .reset_index()
        )
        brand_stats = brand_stats[brand_stats["count"] >= 2].sort_values("mean").head(5)
        summary["cheapest_brands"] = [
            {"brand": str(row["brand"]), "avg_cpl": round(float(row["mean"]), 1), "count": int(row["count"])}
            for _, row in brand_stats.iterrows()
        ]

    if "suburb" in live_df.columns:
        suburb_stats = (
            live_df.assign(price_cpl=pd.to_numeric(live_df["price_cpl"], errors="coerce"))
            .dropna(subset=["price_cpl"])
            .groupby("suburb")["price_cpl"]
            .agg(["mean", "count"])
            .reset_index()
        )
        suburb_stats = suburb_stats[suburb_stats["count"] >= 2].sort_values("mean").head(5)
        summary["cheapest_suburbs"] = [
            {"suburb": str(row["suburb"]), "avg_cpl": round(float(row["mean"]), 1), "count": int(row["count"])}
            for _, row in suburb_stats.iterrows()
        ]

    return summary


def _profile_live_collection(state: str) -> dict:
    """Summarise the live collection file using scrape time as the market observation time."""
    state = _normalise_api_state(state)
    if not os.path.exists(HISTORY_FILE):
        return {"available": False, "reason": "collection_file_missing"}

    try:
        header = pd.read_csv(HISTORY_FILE, nrows=0).columns.tolist()
        wanted = ["site_id", "price_cpl", "reported_at", "scraped_at", "state"]
        usecols = [col for col in wanted if col in header]
        df = pd.read_csv(HISTORY_FILE, usecols=usecols)
        if df.empty or "price_cpl" not in df.columns:
            return {"available": False, "reason": "collection_file_empty"}

        if "state" in df.columns:
            states_present = sorted(df["state"].dropna().astype(str).str.upper().unique().tolist())
            df = df[df["state"].astype(str).str.upper() == state].copy()
        else:
            states_present = ["QLD"]
            if state != "QLD":
                return {"available": False, "reason": "legacy_qld_only_collection"}

        raw_rows = int(len(df))
        df["price_cpl"] = pd.to_numeric(df["price_cpl"], errors="coerce")
        df = df[(df["price_cpl"] > 100) & (df["price_cpl"] < 300)].copy()

        date_basis = "scraped_at" if "scraped_at" in df.columns else "reported_at"
        df["date"] = pd.to_datetime(df[date_basis], errors="coerce")
        df = df.dropna(subset=["date", "price_cpl"])
        if df.empty:
            return {"available": False, "reason": "no_valid_collection_rows"}

        df["day"] = df["date"].dt.normalize()
        daily = (
            df.groupby("day")["price_cpl"]
            .agg(["median", "mean", "min", "max", "count"])
            .reset_index()
            .sort_values("day")
        )

        latest = daily.iloc[-1]
        recent = daily.tail(min(4, len(daily)))
        latest_delta = float(recent["median"].iloc[-1] - recent["median"].iloc[0]) if len(recent) > 1 else 0.0
        if latest_delta >= 3.0:
            trend_label = "rising from the recent trough"
        elif latest_delta <= -3.0:
            trend_label = "discounting lower"
        else:
            trend_label = "mostly flat"

        reported_days = None
        if "reported_at" in df.columns:
            reported_days = int(pd.to_datetime(df["reported_at"], errors="coerce").dt.date.nunique())

        return {
            "available": True,
            "file_name": os.path.basename(HISTORY_FILE),
            "states_present": states_present,
            "state_rows": raw_rows,
            "usable_rows": int(len(df)),
            "station_count": int(df["site_id"].nunique()) if "site_id" in df.columns else None,
            "date_basis": date_basis,
            "scrape_days": int(len(daily)),
            "reported_days": reported_days,
            "latest_date": latest["day"].strftime("%Y-%m-%d"),
            "latest_median_cpl": round(float(latest["median"]), 1),
            "latest_mean_cpl": round(float(latest["mean"]), 1),
            "latest_min_cpl": round(float(latest["min"]), 1),
            "latest_max_cpl": round(float(latest["max"]), 1),
            "latest_window_days": int(len(recent)),
            "latest_window_delta_cpl": round(latest_delta, 1),
            "trend_label": trend_label,
            "finding": (
                f"{state} rows are aggregated by {date_basis}; latest median is "
                f"{float(latest['median']):.1f} cpl and the latest {len(recent)} "
                f"scrape-day move is {latest_delta:+.1f} cpl."
            ),
        }
    except Exception as e:
        logger.debug("Live collection profile failed: %s", e)
        return {"available": False, "reason": "profile_failed"}


@lru_cache(maxsize=1)
def _historical_validation_summary() -> dict:
    """Backtest a transparent daily-cycle baseline on the clean Brisbane history."""
    if not os.path.exists(CLEAN_HISTORY_FILE):
        return {"available": False, "reason": "clean_history_missing"}

    try:
        df = pd.read_csv(CLEAN_HISTORY_FILE, usecols=["price_cpl", "reported_at"])
        df["date"] = pd.to_datetime(df["reported_at"], errors="coerce")
        df["price_cpl"] = pd.to_numeric(df["price_cpl"], errors="coerce")
        df = df.dropna(subset=["date", "price_cpl"])
        df = df[(df["price_cpl"] > 100) & (df["price_cpl"] < 300)]
        daily = (
            df.groupby(df["date"].dt.normalize())["price_cpl"]
            .median()
            .reset_index()
            .sort_values("date")
            .reset_index(drop=True)
        )
        if len(daily) < 75:
            return {"available": False, "reason": "insufficient_clean_history"}

        prices = daily["price_cpl"].to_numpy(dtype=float)
        rows = []
        for idx in range(35, len(prices) - 1):
            current = prices[idx]
            window = prices[max(0, idx - 44): idx + 1]
            recent = prices[max(0, idx - 7): idx + 1]
            diffs = np.diff(recent)
            median_drift = float(np.median(diffs)) if len(diffs) else 0.0
            position = (current - float(np.min(window))) / max(1.0, float(np.max(window) - np.min(window)))

            delta = median_drift
            if position < 0.18 and idx >= 4 and current - prices[idx - 3] >= -1.0:
                delta = max(delta, 5.0)
            elif position > 0.80:
                delta = min(delta, -2.0)
            delta = float(np.clip(delta, -8.0, 22.0))
            rows.append({
                "predicted": current + delta,
                "naive": current,
                "actual": prices[idx + 1],
                "actual_delta": prices[idx + 1] - current,
            })

        result = pd.DataFrame(rows).tail(60)
        if result.empty:
            return {"available": False, "reason": "no_validation_window"}

        error = result["predicted"] - result["actual"]
        naive_error = result["naive"] - result["actual"]
        hike_days = int((result["actual_delta"] > 5.0).sum())
        return {
            "available": True,
            "window_days": int(len(result)),
            "history_days": int(len(daily)),
            "start_date": daily["date"].iloc[-len(result)].strftime("%Y-%m-%d"),
            "end_date": daily["date"].iloc[-1].strftime("%Y-%m-%d"),
            "cycle_baseline_mae_cpl": round(float(error.abs().mean()), 2),
            "naive_mae_cpl": round(float(naive_error.abs().mean()), 2),
            "within_5c_percent": round(float((error.abs() <= 5.0).mean() * 100), 1),
            "hike_days": hike_days,
            "note": "Price-level forecasts are usually tighter than exact restoration-day timing.",
        }
    except Exception as e:
        logger.debug("Historical validation failed: %s", e)
        return {"available": False, "reason": "validation_failed"}


def _build_prediction_method(evidence: dict, daily_df: pd.DataFrame | None, live_df: pd.DataFrame | None) -> dict:
    status = evidence.get("market_status", {})
    ticker = status.get("ticker", {})
    cycle = status.get("cycle", {}) or {}
    collection = _profile_live_collection(evidence.get("state", config.DEFAULT_STATE))
    validation = _historical_validation_summary()

    current_price = _safe_float(status.get("current_median"), None)
    if current_price is None and collection.get("available"):
        current_price = _safe_float(collection.get("latest_median_cpl"), None)
    current_tgp = _safe_float(ticker.get("tgp"), None)
    spread = round(current_price - current_tgp, 1) if current_price is not None and current_tgp is not None else None

    restore_prob = None
    probabilities = cycle.get("probabilities") if isinstance(cycle, dict) else None
    if isinstance(probabilities, dict):
        restore_prob = _safe_float(probabilities.get("RESTORATION"), None)

    data_days = None
    if daily_df is not None and not daily_df.empty:
        data_days = int(len(daily_df))

    forecast_mode = "Hybrid ML + calibrated cycle physics" if MODEL_LOADED and (data_days or 0) >= 45 else "Calibrated cycle physics fallback"
    if MODEL_LOADED and (data_days or 0) < 45:
        history_note = "ML model is loaded, but the current daily series is shorter than the 45-day lag window."
    elif MODEL_LOADED:
        history_note = "ML model has enough lag history for recursive short-range forecasting."
    else:
        history_note = "ML model is unavailable, so the physics fallback owns the forecast."

    return clean_nan({
        "summary": (
            "The price forecast uses live QLD station medians for the current market state, "
            "cycle position for restoration risk, and TGP/import-parity data as the wholesale floor."
        ),
        "forecast_mode": forecast_mode,
        "history_note": history_note,
        "live_collection": collection,
        "validation": validation,
        "signals": [
            {
                "label": "Live collection",
                "value": f"{collection.get('usable_rows', 0):,} rows" if collection.get("available") else "Unavailable",
                "detail": collection.get("finding", "Live collection could not be profiled."),
            },
            {
                "label": "Timestamp basis",
                "value": collection.get("date_basis", "--"),
                "detail": "scraped_at is the market observation time; reported_at is a station update timestamp.",
            },
            {
                "label": "Retail/TGP spread",
                "value": f"{spread:+.1f} cpl" if spread is not None else "--",
                "detail": f"Retail median {current_price:.1f} cpl vs TGP {current_tgp:.1f} cpl." if current_price and current_tgp else "Spread unavailable.",
            },
            {
                "label": "Cycle state",
                "value": str(cycle.get("market_phase") or cycle.get("phase") or "UNKNOWN"),
                "detail": f"Restore probability {restore_prob * 100:.0f}%." if restore_prob is not None else "Cycle probability unavailable.",
            },
            {
                "label": "TGP anchor",
                "value": str(ticker.get("tgp_anchor_reason") or "source_tgp"),
                "detail": "Raw TGP is used unless it is invalid, an emergency fallback, or contradicts observed retail prices.",
            },
            {
                "label": "Forecast mode",
                "value": forecast_mode,
                "detail": history_note,
            },
        ],
        "formula_steps": [
            "Daily retail input = state-filtered median(price_cpl) grouped by scraped_at day for live collections.",
            "Cycle features = lag prices + 7/14/28/42 day movement + peak/trough distance + restoration probability.",
            "ML delta = XGBoost regressor(delta_cpl) with XGBoost hike probability added as a feature.",
            "Cycle physics = if retail margin approaches the calibrated floor above TGP, apply a restoration spike; otherwise decay by the calibrated undercut rate.",
            "Final day-n price = ML_delta weight * ML path + cycle weight * physics path, with ML weight fading from 70% to 10% across 14 days.",
            "Uncertainty band = forecast +/- 1.8 * sqrt(day_n), with the low bound clamped to the effective TGP anchor.",
            "TGP model = live AIP/Viva TGP when available; import-parity explains movement from Brent, AUD/USD, freight, excise and GST.",
        ],
        "research_sources": PREDICTION_RESEARCH_SOURCES,
    })


def _build_analyst_notes(evidence: dict) -> list[dict]:
    notes = []
    market_status = evidence.get("market_status", {})
    ticker = market_status.get("ticker", {})
    stations = evidence.get("live_stations", {})
    daily = evidence.get("daily_prices", {})
    snapshot_age = evidence.get("data_freshness", {}).get("snapshot_age_minutes")

    if snapshot_age is not None and snapshot_age > 90:
        notes.append({
            "title": "Snapshot age",
            "severity": "warning",
            "detail": f"Latest snapshot is {snapshot_age:.0f} minutes old.",
        })

    dispersion = stations.get("dispersion_cpl")
    if dispersion and dispersion > 35:
        notes.append({
            "title": "Wide station spread",
            "severity": "info",
            "detail": f"Current station spread is {dispersion:.1f} cpl between cheapest and most expensive sites.",
        })

    tgp = ticker.get("tgp")
    current_avg = market_status.get("current_avg")
    if tgp and current_avg:
        margin = round(float(current_avg) - float(tgp), 1)
        if margin < 6:
            notes.append({
                "title": "Retail margin squeeze",
                "severity": "warning",
                "detail": f"Average retail is only {margin:.1f} cpl above TGP.",
            })
        elif margin > 30:
            notes.append({
                "title": "Elevated retail premium",
                "severity": "alert",
                "detail": f"Average retail is {margin:.1f} cpl above TGP.",
            })

    delta_1d = daily.get("delta_1d_cpl")
    if delta_1d is not None and abs(delta_1d) >= 5:
        notes.append({
            "title": "Daily move",
            "severity": "alert" if delta_1d > 0 else "info",
            "detail": f"Daily median moved {delta_1d:+.1f} cpl.",
        })

    if not notes:
        notes.append({
            "title": "No major anomaly",
            "severity": "ok",
            "detail": "Current data does not show an obvious margin, freshness, or dispersion warning.",
        })

    return notes[:5]


def _build_advanced_evidence(state: str) -> dict:
    state = _normalise_api_state(state)
    capital = config.STATES[state]["capital"]

    live_df = load_live_data_latest(state=state)
    live_df = _enrich_live_df(live_df)
    daily_df = _get_daily_data(state)
    trend = tgp_forecast.analyze_trend(city=capital)

    current_avg = float(live_df["price_cpl"].mean()) if live_df is not None and not live_df.empty else 0.0
    current_median = float(live_df["price_cpl"].median()) if live_df is not None and not live_df.empty else 0.0
    raw_tgp = _safe_float(trend.get("current_tgp"), 165.0)
    effective_tgp, tgp_anchor_reason = _forecast_tgp_anchor(raw_tgp, live_df, daily_df, trend)

    cycle_info = None
    if cycle_detector and daily_df is not None and len(daily_df) > 5:
        try:
            cycle_info = cycle_detector.detect_current_regime(daily_df["price_cpl"])
        except Exception as e:
            logger.debug("Advanced cycle detection failed: %s", e)

    market_context_payload = None
    if market_context:
        try:
            market_context_payload = market_context.generate_context(
                state=state,
                current_avg=current_avg or current_median,
                tgp=effective_tgp,
                brent_usd=trend.get("oil_price_usd", 75.0) or 75.0,
                aud_usd=trend.get("aud_usd", 0.65) or 0.65,
                cycle_info=cycle_info,
                news_items=None,
            )
        except Exception as e:
            logger.debug("Advanced market context failed: %s", e)

    news_summary = {"global": [], "domestic": []}
    try:
        news_data = market_news.get_market_news()
        for key in ["global", "domestic"]:
            articles = news_data.get(key, {}).get("articles", []) if isinstance(news_data.get(key), dict) else []
            news_summary[key] = [
                {
                    "title": item.get("title"),
                    "sentiment_tag": item.get("sentiment_tag"),
                    "impact_vector": item.get("impact_vector"),
                    "analysis": item.get("analysis"),
                    "publisher": item.get("publisher"),
                }
                for item in articles[:3]
            ]
            if isinstance(news_data.get(key), dict):
                news_summary[f"{key}_overall_sentiment"] = news_data[key].get("overall_sentiment")
                news_summary[f"{key}_summary"] = news_data[key].get("summary")
    except Exception as e:
        logger.debug("Advanced news fetch failed: %s", e)

    snapshot_age = None
    if os.path.exists(SNAPSHOT_FILE):
        snapshot_age = round((time.time() - os.path.getmtime(SNAPSHOT_FILE)) / 60, 1)

    evidence = clean_nan({
        "state": state,
        "capital": capital,
        "generated_at": datetime.now().isoformat(),
        "data_freshness": {
            "snapshot_age_minutes": snapshot_age,
            "db_available": db is not None,
            "gemini_available": advanced_ai.available,
        },
        "market_status": {
            "current_avg": round(current_avg, 1) if current_avg else None,
            "current_median": round(current_median, 1) if current_median else None,
            "station_count": int(len(live_df)) if live_df is not None else 0,
            "hike_probability": None,
            "advice": None,
            "savings_insight": None,
            "cycle": cycle_info or {"phase": "UNKNOWN"},
            "ticker": {
                "tgp": effective_tgp,
                "raw_tgp": raw_tgp,
                "tgp_anchor_reason": tgp_anchor_reason,
                "tgp_source": trend.get("tgp_source"),
                "oil": trend.get("oil_price_usd"),
                "fx": trend.get("aud_usd"),
                "mogas": trend.get("current_mogas"),
                "import_parity_lag": trend.get("import_parity_lag"),
            },
        },
        "daily_prices": _summarise_daily_prices(daily_df),
        "live_stations": _summarise_live_stations(live_df),
        "tgp_history": trend.get("history", {}),
        "market_context": market_context_payload,
        "news": news_summary,
    })
    evidence["analyst_notes"] = _build_analyst_notes(evidence)
    return evidence


def _advanced_source_cards(evidence: dict) -> list[dict]:
    status = evidence.get("market_status", {})
    ticker = status.get("ticker", {})
    daily = evidence.get("daily_prices", {})
    stations = evidence.get("live_stations", {})

    def cpl(value):
        return f"{value} cpl" if value is not None else "--"

    cards = [
        {
            "label": "Retail",
            "value": cpl(status.get("current_avg")),
            "detail": f"{status.get('station_count', 0)} stations in latest snapshot",
        },
        {
            "label": "Wholesale",
            "value": cpl(ticker.get("tgp")),
            "detail": f"Parity signal: {ticker.get('import_parity_lag', 'UNKNOWN')}",
        },
        {
            "label": "Cycle",
            "value": str(status.get("cycle", {}).get("phase", "UNKNOWN")),
            "detail": f"{status.get('cycle', {}).get('confidence', 0)} confidence",
        },
        {
            "label": "Daily",
            "value": cpl(daily.get("delta_1d_cpl")),
            "detail": f"7-day move {daily.get('delta_7d_cpl', '--')} cpl",
        },
        {
            "label": "Spread",
            "value": cpl(stations.get("dispersion_cpl")),
            "detail": "Cheapest to most expensive station",
        },
    ]
    return cards


def _get_daily_data(state):
    """Get daily price data, preferring SQLite, falling back to market_physics."""
    if db:
        try:
            daily = db.get_daily_stats(state, days=180)
            if daily is not None and not daily.empty:
                return daily
        except Exception:
            pass
    return market_physics.load_daily_data(state=state)


def _enrich_live_df(live_df):
    """Merge station metadata into live data."""
    meta = get_cached_metadata()
    if not meta.empty and not live_df.empty:
        # Drop existing metadata columns to avoid conflicts
        for col in ['name', 'brand', 'suburb']:
            if col in live_df.columns and col in meta.columns:
                live_df = live_df.drop(columns=[col])
        merge_cols = [c for c in ['site_id', 'name', 'brand', 'suburb'] if c in meta.columns]
        live_df = live_df.merge(meta[merge_cols], on='site_id', how='left')
    return live_df


def _trend_value(trend: dict, *keys: str, fallback=None):
    for key in keys:
        value = trend.get(key)
        if value is not None:
            return value
    return fallback


def _forecast_tgp_anchor(raw_tgp: float, live_df: pd.DataFrame | None, daily_df: pd.DataFrame | None, trend: dict) -> tuple[float, str]:
    """Choose a TGP anchor that is safe for retail-price forecasting."""
    raw_tgp = _safe_float(raw_tgp, 0.0)
    source = str(trend.get("tgp_source", "") or "").lower()

    observed_prices = []
    if live_df is not None and not live_df.empty and "price_cpl" in live_df.columns:
        live_prices = pd.to_numeric(live_df["price_cpl"], errors="coerce").dropna()
        if not live_prices.empty:
            observed_prices.extend([float(live_prices.median()), float(live_prices.min())])
    if daily_df is not None and not daily_df.empty and "price_cpl" in daily_df.columns:
        daily_prices = pd.to_numeric(daily_df["price_cpl"], errors="coerce").dropna().tail(21)
        if not daily_prices.empty:
            observed_prices.extend([float(daily_prices.iloc[-1]), float(daily_prices.min())])

    if not observed_prices:
        return raw_tgp or 165.0, "raw_tgp_no_observed_retail"

    retail_median = float(np.median(observed_prices))
    observed_floor = max(90.0, min(observed_prices) - 4.0)

    invalid_tgp = raw_tgp < 100 or raw_tgp > 250
    emergency_tgp = "emergency" in source or "unknown" in source
    contradicts_retail = raw_tgp > retail_median + 12.0

    if invalid_tgp or emergency_tgp or contradicts_retail:
        return round(observed_floor, 1), "observed_retail_floor"

    return round(raw_tgp, 1), "source_tgp"


def _latest_scrape_time(live_df: pd.DataFrame) -> datetime | None:
    if live_df is None or live_df.empty or "scraped_at" not in live_df.columns:
        return None
    parsed = pd.to_datetime(live_df["scraped_at"], errors="coerce").dropna()
    if parsed.empty:
        return None
    value = parsed.max()
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def _serialise_station(row: pd.Series | None) -> dict | None:
    if row is None:
        return None
    return clean_nan({
        "site_id": str(row.get("site_id", "")),
        "name": str(row.get("name", "Station")),
        "brand": str(row.get("brand", "")),
        "suburb": str(row.get("suburb", "")),
        "price_cpl": round(_safe_float(row.get("price_cpl"), 0.0), 1),
        "latitude": _safe_float(row.get("latitude")),
        "longitude": _safe_float(row.get("longitude")),
        "reported_at": row.get("reported_at"),
    })


def _build_recommendation_payload(
    state: str,
    fuel: str = "unleaded",
    suburb: str | None = None,
    tank_size_l: float = 50.0,
) -> dict:
    state = _normalise_api_state(state)
    capital = config.STATES[state]["capital"]
    tank_size_l = max(5.0, min(200.0, _safe_float(tank_size_l, 50.0)))

    live_df = _enrich_live_df(load_live_data_latest(state=state))
    daily_df = _get_daily_data(state)
    trend = tgp_forecast.analyze_trend(city=capital)
    snapshot_age = _snapshot_age_minutes()
    freshness = _freshness_status(snapshot_age)

    if live_df is None or live_df.empty or "price_cpl" not in live_df.columns:
        return {
            "ok": True,
            "state": state,
            "fuel": fuel,
            "decision": "CHECK_DATA",
            "decision_label": "Check Data",
            "confidence": 0,
            "expected_saving": {"amount": 0.0, "basis": "No live station data available."},
            "cheapest_option": None,
            "reason": "Live station prices are unavailable, so Fuel AI cannot make a reliable fill/wait recommendation.",
            "answer_state": "cannot answer from available data",
            "evidence": [],
            "freshness": {"status": freshness, "snapshot_age_minutes": snapshot_age},
            "generated_at": datetime.now().isoformat(),
        }

    live_df = live_df.copy()
    live_df["price_cpl"] = pd.to_numeric(live_df["price_cpl"], errors="coerce")
    live_df = live_df.dropna(subset=["price_cpl"])
    if live_df.empty:
        return {
            "ok": True,
            "state": state,
            "fuel": fuel,
            "decision": "CHECK_DATA",
            "decision_label": "Check Data",
            "confidence": 0,
            "expected_saving": {"amount": 0.0, "basis": "No valid live station prices available."},
            "cheapest_option": None,
            "reason": "Live station prices failed validation, so Fuel AI cannot make a reliable recommendation.",
            "answer_state": "cannot answer from available data",
            "evidence": [],
            "freshness": {"status": freshness, "snapshot_age_minutes": snapshot_age},
            "generated_at": datetime.now().isoformat(),
        }

    local_df = live_df
    suburb_clean = (suburb or "").strip()
    if suburb_clean and "suburb" in live_df.columns:
        matched = live_df[live_df["suburb"].astype(str).str.lower() == suburb_clean.lower()]
        if not matched.empty:
            local_df = matched

    cheapest_row = local_df.sort_values(["price_cpl"]).iloc[0]
    cheapest = _serialise_station(cheapest_row)
    current_avg = float(live_df["price_cpl"].mean())
    current_median = float(live_df["price_cpl"].median())
    local_median = float(local_df["price_cpl"].median())
    spread_cpl = float(live_df["price_cpl"].max() - live_df["price_cpl"].min())
    cheapest_delta_cpl = max(0.0, current_median - float(cheapest_row["price_cpl"]))

    raw_tgp = _safe_float(_trend_value(trend, "current_tgp"), 165.0)
    current_tgp, tgp_anchor_reason = _forecast_tgp_anchor(raw_tgp, live_df, daily_df, trend)
    margin_cpl = current_median - current_tgp
    oil = _trend_value(trend, "current_oil", "oil_price_usd", fallback=0)
    fx = _trend_value(trend, "current_fx", "aud_usd", fallback=0)
    mogas = _trend_value(trend, "current_mogas", fallback=0)

    cycle_info = None
    if cycle_detector and daily_df is not None and len(daily_df) > 5:
        try:
            cycle_info = cycle_detector.detect_current_regime(daily_df["price_cpl"])
        except Exception as e:
            logger.debug("Recommendation cycle detection failed: %s", e)
    cycle_info = cycle_info or {"phase": "UNKNOWN", "confidence": 0, "estimated_days_remaining": 0}

    hike_prob = 0.0
    forecast_min = None
    forecast_max = None
    forecast_delta_7d = 0.0
    if daily_df is not None and not daily_df.empty and ai_model:
        try:
            ai_input = daily_df.rename(columns={"day": "date"}).copy()
            today = pd.Timestamp.now().normalize()
            if ai_input["date"].max() < today and current_median > 0:
                ai_input = pd.concat(
                    [ai_input, pd.DataFrame({"date": [today], "price_cpl": [current_median]})],
                    ignore_index=True,
                )
            future_df = ai_model.predict_horizon(ai_input, days=14, tgp=current_tgp)
            if future_df is not None and not future_df.empty:
                first = future_df.iloc[0]
                hike_prob = _safe_float(first.get("hike_probability"), 0.0) * 100
                prices = pd.to_numeric(future_df["predicted_price"], errors="coerce").dropna()
                if not prices.empty:
                    forecast_min = float(prices.min())
                    forecast_max = float(prices.max())
                    forecast_delta_7d = float(prices.iloc[min(6, len(prices) - 1)] - current_median)
        except Exception as e:
            logger.debug("Recommendation forecast failed: %s", e)

    phase = str(cycle_info.get("phase", "UNKNOWN")).upper()
    cycle_confidence = _safe_float(cycle_info.get("confidence"), 0.0)
    decision = "COMPARE"
    decision_label = "Compare Nearby"

    if hike_prob >= 65 or (phase == "RESTORATION" and cycle_confidence >= 0.55):
        decision = "FILL_NOW"
        decision_label = "Fill Now"
    elif forecast_min is not None and forecast_min <= current_median - 2.0 and hike_prob < 50:
        decision = "WAIT"
        decision_label = "Wait"
    elif margin_cpl <= 10 or cheapest_delta_cpl >= 8:
        decision = "FILL_NOW"
        decision_label = "Fill Now"

    if decision == "FILL_NOW":
        upside_cpl = max(cheapest_delta_cpl, (forecast_max or current_median) - float(cheapest_row["price_cpl"]), 0)
        saving_amount = round(upside_cpl * tank_size_l / 100, 2)
        saving_basis = "Estimated against forecast high or the current market median."
    elif decision == "WAIT":
        downside_cpl = max(current_median - (forecast_min or current_median), 0)
        saving_amount = round(downside_cpl * tank_size_l / 100, 2)
        saving_basis = "Estimated from the forecast low over the next 14 days."
    else:
        saving_amount = round(cheapest_delta_cpl * tank_size_l / 100, 2)
        saving_basis = "Estimated from choosing the cheapest visible station now."

    confidence = 45
    if MODEL_LOADED:
        confidence += 20
    if freshness == "fresh":
        confidence += 15
    elif freshness == "stale":
        confidence -= 10
    if len(live_df) >= 50:
        confidence += 10
    if cycle_confidence:
        confidence += int(cycle_confidence * 10)
    confidence = max(10, min(95, confidence))
    if freshness in {"degraded", "unavailable"}:
        confidence = min(confidence, 45)

    reason_parts = []
    if decision == "FILL_NOW":
        reason_parts.append(f"Hike risk is {hike_prob:.0f}% and the best visible price is {float(cheapest_row['price_cpl']):.1f} cpl.")
    elif decision == "WAIT":
        reason_parts.append(f"The 14-day forecast shows prices could ease toward {forecast_min:.1f} cpl.")
    else:
        reason_parts.append(f"The market signal is mixed, but the station spread is {spread_cpl:.1f} cpl.")
    reason_parts.append(f"Retail median is {margin_cpl:.1f} cpl above TGP and cycle phase is {phase}.")
    if freshness != "fresh":
        reason_parts.append(f"Data is {freshness}, so verify the station price before driving.")

    answer_state = "confident" if confidence >= 70 and freshness == "fresh" else "limited evidence"
    if freshness in {"stale", "degraded"}:
        answer_state = "data stale"

    evidence = [
        {
            "label": "Hike probability",
            "value": f"{hike_prob:.0f}%",
            "detail": "Forecast probability for the next movement.",
            "source": "DeepCycle model" if ai_model else "Fallback heuristic",
            "freshness": freshness,
        },
        {
            "label": "TGP margin",
            "value": f"{margin_cpl:.1f} cpl",
            "detail": f"Retail median {current_median:.1f} cpl vs TGP {current_tgp:.1f} cpl.",
            "source": "AIP/Viva TGP plus live station data",
            "freshness": freshness,
        },
        {
            "label": "Station spread",
            "value": f"{spread_cpl:.1f} cpl",
            "detail": f"Cheapest visible station is {cheapest_delta_cpl:.1f} cpl below median.",
            "source": "Latest station snapshot",
            "freshness": freshness,
        },
        {
            "label": "Cycle phase",
            "value": phase,
            "detail": f"Confidence {cycle_confidence:.2f}; estimated days remaining {cycle_info.get('estimated_days_remaining', 0)}.",
            "source": "Cycle detector",
            "freshness": freshness,
        },
    ]

    return clean_nan({
        "ok": True,
        "state": state,
        "fuel": fuel,
        "suburb": suburb_clean,
        "tank_size_l": tank_size_l,
        "decision": decision,
        "decision_label": decision_label,
        "confidence": confidence,
        "expected_saving": {"amount": saving_amount, "basis": saving_basis},
        "cheapest_option": cheapest,
        "reason": " ".join(reason_parts),
        "answer_state": answer_state,
        "evidence": evidence,
        "metrics": {
            "current_avg_cpl": round(current_avg, 1),
            "current_median_cpl": round(current_median, 1),
            "local_median_cpl": round(local_median, 1),
            "current_tgp_cpl": round(current_tgp, 1),
            "raw_tgp_cpl": round(raw_tgp, 1),
            "tgp_anchor_reason": tgp_anchor_reason,
            "tgp_source": trend.get("tgp_source"),
            "margin_cpl": round(margin_cpl, 1),
            "hike_probability": round(hike_prob, 1),
            "forecast_min_cpl": round(forecast_min, 1) if forecast_min is not None else None,
            "forecast_max_cpl": round(forecast_max, 1) if forecast_max is not None else None,
            "forecast_delta_7d_cpl": round(forecast_delta_7d, 1),
            "station_count": int(len(live_df)),
            "oil_usd": oil,
            "aud_usd": fx,
            "mogas": mogas,
        },
        "cycle": cycle_info,
        "freshness": {
            "status": freshness,
            "snapshot_age_minutes": snapshot_age,
            "latest_scrape_time": _latest_scrape_time(live_df).isoformat() if _latest_scrape_time(live_df) else None,
        },
        "generated_at": datetime.now().isoformat(),
    })


def _count_csv_rows(path: str) -> int | None:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            rows = sum(1 for _ in handle)
        return max(rows - 1, 0)
    except Exception:
        return None


def _build_data_health_payload(state: str = "QLD") -> dict:
    state = _normalise_api_state(state)
    live_df = _enrich_live_df(load_live_data_latest(state=state))
    daily_df = _get_daily_data(state)
    snapshot_age = _snapshot_age_minutes()
    snapshot_status = _freshness_status(snapshot_age)
    latest_scrape = _latest_scrape_time(live_df)

    daily_status = "unavailable"
    latest_daily = None
    if daily_df is not None and not daily_df.empty:
        latest_day = pd.to_datetime(daily_df["day"], errors="coerce").dropna().max()
        if isinstance(latest_day, pd.Timestamp):
            latest_daily = latest_day.strftime("%Y-%m-%d")
            age_days = (pd.Timestamp.now().normalize() - latest_day.normalize()).days
            daily_status = "fresh" if age_days <= 1 else "stale" if age_days <= 7 else "degraded"

    live_count = int(len(live_df)) if live_df is not None else 0
    metadata = get_cached_metadata()
    fallbacks = []
    if snapshot_status != "fresh":
        fallbacks.append("latest_snapshot_stale_or_missing")
    if not config.FUEL_API_TOKEN and state != "WA":
        fallbacks.append("fuel_api_token_missing")
    if not advanced_ai.available:
        fallbacks.append("gemini_disabled")
    if daily_status != "fresh":
        fallbacks.append("daily_stats_not_fresh")

    overall = "live"
    if snapshot_status in {"stale", "degraded"} or daily_status in {"stale", "degraded"}:
        overall = "degraded"
    if live_count == 0:
        overall = "offline"

    sources = [
        {
            "name": "Live station snapshot",
            "status": snapshot_status,
            "rows": live_count,
            "latest_observation": latest_scrape.isoformat() if latest_scrape else None,
            "age_minutes": snapshot_age,
            "source": "SQLite latest snapshot with CSV fallback",
        },
        {
            "name": "Daily price aggregates",
            "status": daily_status,
            "rows": int(len(daily_df)) if daily_df is not None else 0,
            "latest_observation": latest_daily,
            "source": "SQLite daily_stats with CSV/cache fallback",
        },
        {
            "name": "Station metadata",
            "status": "fresh" if not metadata.empty else "unavailable",
            "rows": int(len(metadata)) if metadata is not None else 0,
            "source": os.path.basename(config.METADATA_FILE),
        },
        {
            "name": "Gemini analyst",
            "status": "fresh" if advanced_ai.available else "unavailable",
            "rows": None,
            "source": "Backend-only GEMINI_API_KEY",
            "fallback_reason": "" if advanced_ai.available else advanced_ai.disabled_reason,
        },
    ]

    return clean_nan({
        "ok": True,
        "state": state,
        "overall_status": overall,
        "generated_at": datetime.now().isoformat(),
        "sources": sources,
        "fallback_usage": fallbacks,
        "row_counts": {
            "latest_snapshot": live_count,
            "daily_stats": int(len(daily_df)) if daily_df is not None else 0,
            "station_metadata": int(len(metadata)) if metadata is not None else 0,
            "snapshot_csv": _count_csv_rows(SNAPSHOT_FILE),
            "history_csv": _count_csv_rows(HISTORY_FILE),
        },
        "dependencies": {
            "fuel_api_token_configured": bool(config.FUEL_API_TOKEN),
            "database_available": db is not None,
            "ai_model_available": ai_model is not None,
            "model_loaded": MODEL_LOADED,
            "cycle_detector_available": cycle_detector is not None,
            "market_context_available": market_context is not None,
            "gemini_available": advanced_ai.available,
            "cors_origins": config.CORS_ORIGINS,
        },
        "flags": {
            "stale": snapshot_status == "stale" or daily_status == "stale",
            "degraded": overall in {"degraded", "offline"},
            "offline": overall == "offline",
        },
    })


def _build_status_chips(health: dict, recommendation: dict) -> list[dict]:
    chips = []
    overall = health.get("overall_status", "degraded")
    chips.append({"label": overall.title(), "tone": "ok" if overall == "live" else "warning" if overall == "degraded" else "alert"})
    freshness = recommendation.get("freshness", {}).get("status", "unavailable")
    chips.append({"label": freshness.title(), "tone": "ok" if freshness == "fresh" else "warning"})
    chips.append({"label": "AI Ready" if advanced_ai.available else "AI Disabled", "tone": "ok" if advanced_ai.available else "warning"})
    chips.append({"label": "Advanced Locked", "tone": "neutral"})
    return chips


def _build_alerts(recommendation: dict, health: dict) -> list[dict]:
    alerts = []
    if recommendation.get("decision") == "FILL_NOW" and recommendation.get("confidence", 0) >= 65:
        alerts.append({
            "type": "price_hike_likely",
            "title": "Price hike likely",
            "detail": recommendation.get("reason", "Fuel prices may rise soon."),
        })
    cheapest = recommendation.get("cheapest_option")
    if cheapest:
        alerts.append({
            "type": "cheap_fuel_nearby",
            "title": "Cheap visible station",
            "detail": f"{cheapest.get('name')} is showing {cheapest.get('price_cpl')} cpl in {cheapest.get('suburb')}.",
        })
    if health.get("flags", {}).get("stale"):
        alerts.append({
            "type": "data_stale",
            "title": "Data is stale",
            "detail": "Recommendation confidence is capped until the next successful refresh.",
        })
    if not alerts:
        alerts.append({
            "type": "morning_briefing_ready",
            "title": "Morning briefing ready",
            "detail": "Open Intelligence for market context and technical evidence.",
        })
    return alerts[:4]


def _build_technical_summary(state: str = "QLD") -> dict:
    state = _normalise_api_state(state)
    evidence = _build_advanced_evidence(state)
    health = _build_data_health_payload(state)
    status = evidence.get("market_status", {})
    ticker = status.get("ticker", {})
    current_avg = _safe_float(status.get("current_avg"), 0.0)
    current_tgp = _safe_float(ticker.get("tgp"), 0.0)
    spread = evidence.get("live_stations", {}).get("dispersion_cpl")
    return clean_nan({
        "ok": True,
        "state": state,
        "generated_at": datetime.now().isoformat(),
        "market_internals": {
            "retail_avg_cpl": current_avg,
            "tgp_cpl": current_tgp,
            "retail_tgp_spread_cpl": round(current_avg - current_tgp, 1) if current_avg and current_tgp else None,
            "cycle_phase": status.get("cycle", {}).get("phase", "UNKNOWN"),
            "cycle_confidence": status.get("cycle", {}).get("confidence", 0),
            "station_dispersion_cpl": spread,
            "model_loaded": MODEL_LOADED,
        },
        "data_quality": health,
        "analyst_notes": evidence.get("analyst_notes", []),
        "source_cards": _advanced_source_cards(evidence),
        "prediction_method": _build_prediction_method(evidence, _get_daily_data(state), load_live_data_latest(state=state)),
    })


# ============================================================
# API ENDPOINTS
# ============================================================

class AdvancedVerifyRequest(BaseModel):
    password: str


class AdvancedAskRequest(BaseModel):
    state: str = "QLD"
    question: str
    history: list[dict] | None = None


class AdvancedShockRequest(BaseModel):
    state: str = "QLD"
    scenario: str


@app.get("/api/recommendation")
async def get_recommendation(
    request: Request,
    state: str = "QLD",
    fuel: str = "unleaded",
    suburb: str | None = None,
    tank_size_l: float = 50.0,
):
    """Normal-user fill/wait recommendation with local evidence cards."""
    _rate_limit(request, "recommendation", 80, 60)
    try:
        return await run_in_threadpool(_build_recommendation_payload, state, fuel, suburb, tank_size_l)
    except Exception as e:
        logger.error("recommendation error: %s", e, exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "code": "RECOMMENDATION_ERROR",
                "message": "Unable to build a fuel recommendation.",
                "details": str(e),
                "generated_at": datetime.now().isoformat(),
            },
        )


@app.get("/api/data-health")
async def get_data_health(request: Request, state: str = "QLD"):
    """Source freshness, fallback usage, row counts, and dependency checks."""
    _rate_limit(request, "data_health", 80, 60)
    try:
        return await run_in_threadpool(_build_data_health_payload, state)
    except Exception as e:
        logger.error("data-health error: %s", e, exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "code": "DATA_HEALTH_ERROR",
                "message": "Unable to inspect data health.",
                "details": str(e),
                "generated_at": datetime.now().isoformat(),
            },
        )


@app.get("/api/bootstrap")
async def get_bootstrap(
    request: Request,
    state: str = "QLD",
    fuel: str = "unleaded",
    suburb: str | None = None,
    tank_size_l: float = 50.0,
):
    """Initial dashboard payload for the Today experience."""
    _rate_limit(request, "bootstrap", 80, 60)
    try:
        recommendation = await run_in_threadpool(_build_recommendation_payload, state, fuel, suburb, tank_size_l)
        health = await run_in_threadpool(_build_data_health_payload, state)
        live_df = await run_in_threadpool(load_live_data_latest, state=_normalise_api_state(state))
        live_df = await run_in_threadpool(_enrich_live_df, live_df)
        return clean_nan({
            "ok": True,
            "state": _normalise_api_state(state),
            "generated_at": datetime.now().isoformat(),
            "recommendation": recommendation,
            "data_health": health,
            "latest_stations_summary": _summarise_live_stations(live_df),
            "status_chips": _build_status_chips(health, recommendation),
            "alerts": _build_alerts(recommendation, health),
        })
    except Exception as e:
        logger.error("bootstrap error: %s", e, exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "code": "BOOTSTRAP_ERROR",
                "message": "Unable to build bootstrap payload.",
                "details": str(e),
                "generated_at": datetime.now().isoformat(),
            },
        )


@app.get("/api/technical/summary")
async def get_technical_summary(request: Request, state: str = "QLD"):
    """Technical-user workbench summary: internals, health, and analyst notes."""
    _rate_limit(request, "technical_summary", 60, 60)
    try:
        return await run_in_threadpool(_build_technical_summary, state)
    except Exception as e:
        logger.error("technical summary error: %s", e, exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "code": "TECHNICAL_SUMMARY_ERROR",
                "message": "Unable to build technical summary.",
                "details": str(e),
                "generated_at": datetime.now().isoformat(),
            },
        )


def _source_explorer_frame(state: str, dataset: str, limit: int) -> pd.DataFrame:
    state = _normalise_api_state(state)
    limit = max(1, min(int(limit), 500))
    dataset = (dataset or "snapshot").lower()
    if dataset == "snapshot":
        return _enrich_live_df(load_live_data_latest(state=state)).head(limit)
    if dataset == "daily_stats":
        return _get_daily_data(state).tail(limit)
    if dataset == "tgp_history":
        city = config.STATES[state]["capital"]
        if db:
            try:
                frame = db.get_tgp_history(city, days=365).tail(limit)
                if frame is not None and not frame.empty:
                    return frame
            except Exception:
                pass
        trend = tgp_forecast.analyze_trend(city=city).get("history", {})
        return pd.DataFrame({"date": trend.get("dates", []), "tgp": trend.get("values", [])}).tail(limit)
    if dataset == "market_data" and db:
        try:
            return db.get_market_history(days=365).tail(limit)
        except Exception:
            return pd.DataFrame()
    raise HTTPException(status_code=400, detail="Unknown dataset. Use snapshot, daily_stats, tgp_history, or market_data.")


@app.get("/api/technical/source-explorer")
async def get_source_explorer(
    request: Request,
    state: str = "QLD",
    dataset: str = "snapshot",
    limit: int = 50,
):
    """Inspect selected source datasets with bounded row counts."""
    _rate_limit(request, "source_explorer", 60, 60)
    frame = await run_in_threadpool(_source_explorer_frame, state, dataset, limit)
    return clean_nan({
        "ok": True,
        "state": _normalise_api_state(state),
        "dataset": dataset,
        "columns": list(frame.columns),
        "rows": frame.to_dict(orient="records"),
        "row_count": int(len(frame)),
        "generated_at": datetime.now().isoformat(),
    })


@app.get("/api/technical/export")
async def export_source_dataset(
    request: Request,
    state: str = "QLD",
    dataset: str = "snapshot",
    limit: int = 500,
):
    """CSV export for bounded technical source datasets."""
    _rate_limit(request, "source_export", 20, 60)
    frame = await run_in_threadpool(_source_explorer_frame, state, dataset, limit)
    csv_data = frame.to_csv(index=False)
    filename = f"{_normalise_api_state(state).lower()}_{dataset.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/advanced/verify")
async def verify_advanced(req: AdvancedVerifyRequest, request: Request):
    """Verify the single Advanced password and issue a session-only token."""
    _rate_limit(request, "advanced_verify", 10, 60)
    if not hmac.compare_digest(req.password, config.ADVANCED_PASSWORD):
        raise HTTPException(status_code=401, detail="Invalid Advanced password")

    token, expires_at = _create_advanced_token()
    return {"ok": True, "token": token, "expires_at": expires_at}


@app.post("/api/advanced/ask")
async def ask_advanced(req: AdvancedAskRequest, request: Request, authorization: str | None = Header(default=None)):
    """Ask the analyst a natural-language market question."""
    _rate_limit(request, "advanced_ask", 20, 60)
    _require_advanced_token(authorization)
    question = (req.question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question is required")

    state = _normalise_api_state(req.state)
    evidence = await run_in_threadpool(_build_advanced_evidence, state)
    result = await run_in_threadpool(advanced_ai.ask, question, evidence, req.history)
    return clean_nan({
        "answer": result.get("answer"),
        "disabled": result.get("disabled", False),
        "message": result.get("message", ""),
        "evidence": _advanced_source_cards(evidence),
        "analyst_notes": evidence.get("analyst_notes", []),
        "generated_at": datetime.now().isoformat(),
    })


@app.get("/api/advanced/briefing")
async def get_advanced_briefing(request: Request, state: str = "QLD", authorization: str | None = Header(default=None)):
    """Generate an executive morning briefing from the compact evidence pack."""
    _rate_limit(request, "advanced_briefing", 12, 60)
    _require_advanced_token(authorization)
    state = _normalise_api_state(state)
    evidence = await run_in_threadpool(_build_advanced_evidence, state)
    result = await run_in_threadpool(advanced_ai.briefing, evidence)
    return clean_nan({
        "title": result.get("title", "Morning Fuel Briefing"),
        "summary": result.get("summary", []),
        "action": result.get("action", ""),
        "risks": result.get("risks", []),
        "metrics": _advanced_source_cards(evidence),
        "evidence": _advanced_source_cards(evidence),
        "analyst_notes": evidence.get("analyst_notes", []),
        "disabled": result.get("disabled", False),
        "message": result.get("message", ""),
        "generated_at": datetime.now().isoformat(),
    })


@app.post("/api/advanced/shock")
async def run_advanced_shock(req: AdvancedShockRequest, request: Request, authorization: str | None = Header(default=None)):
    """Convert a natural-language shock into deterministic fuel-price impacts."""
    _rate_limit(request, "advanced_shock", 20, 60)
    _require_advanced_token(authorization)
    scenario = (req.scenario or "").strip()
    if not scenario:
        raise HTTPException(status_code=400, detail="Scenario is required")

    state = _normalise_api_state(req.state)
    evidence = await run_in_threadpool(_build_advanced_evidence, state)
    result = await run_in_threadpool(advanced_ai.shock, scenario, evidence)
    return clean_nan({
        "parsed_variables": result.get("parsed_variables", {}),
        "forecast_impact": result.get("forecast_impact", {}),
        "explanation": result.get("explanation", ""),
        "evidence": _advanced_source_cards(evidence),
        "analyst_notes": evidence.get("analyst_notes", []),
        "disabled": result.get("disabled", False),
        "message": result.get("message", ""),
        "generated_at": datetime.now().isoformat(),
    })


@app.get("/api/market-status")
async def get_market_status(state: str = "QLD"):
    """Main dashboard endpoint — status, forecast, ticker, advice."""
    try:
        state = state.upper()
        if state not in config.STATES:
            return {"status": "ERROR", "advice": "Invalid state"}

        # Load data
        daily_df = await run_in_threadpool(_get_daily_data, state)
        live_df = await run_in_threadpool(load_live_data_latest, state=state)

        current_median = float(live_df['price_cpl'].median()) if not live_df.empty else 0.0
        current_avg = float(live_df['price_cpl'].mean()) if not live_df.empty else 0.0
        station_count = len(live_df) if not live_df.empty else 0

        # Bootstrap daily_df from live data if history is missing
        if (daily_df is None or daily_df.empty) and current_median > 0:
            today = pd.Timestamp.now().normalize()
            daily_df = pd.DataFrame({'day': [today], 'price_cpl': [current_median]})

        # TGP / Market data
        capital = config.STATES.get(state, config.STATES["QLD"])["capital"]
        trend = await run_in_threadpool(tgp_forecast.analyze_trend, city=capital)
        raw_tgp = _safe_float(trend.get('current_tgp'), 165.0)
        current_tgp, tgp_anchor_reason = _forecast_tgp_anchor(raw_tgp, live_df, daily_df, trend)
        current_oil = _trend_value(trend, "current_oil", "oil_price_usd", fallback=0)
        current_fx = _trend_value(trend, "current_fx", "aud_usd", fallback=0.65)
        current_mogas = trend.get('current_mogas', 0)

        # Store TGP/market data in SQLite
        if db:
            try:
                today_str = datetime.now().strftime('%Y-%m-%d')
                if raw_tgp > 0:
                    db.save_tgp(today_str, capital, raw_tgp)
                if current_oil > 0:
                    db.save_market_data(today_str, current_oil, current_fx, current_mogas)
            except Exception:
                pass

        # --- Cycle Detection ---
        cycle_info = None
        if cycle_detector and daily_df is not None and len(daily_df) > 5:
            try:
                cycle_info = cycle_detector.detect_current_regime(daily_df['price_cpl'])
            except Exception as e:
                logger.debug(f"Cycle detection error: {e}")

        # --- AI Forecasting ---
        forecast_data = {"dates": [], "prices": [], "low": [], "high": []}
        hike_prob = 0.0
        status_label = "STABLE"
        advice = "Hold"

        if daily_df is not None and not daily_df.empty and ai_model:
            ai_input = daily_df.rename(columns={'day': 'date'}).copy()

            # Inject today's live price
            today = pd.Timestamp.now().normalize()
            if ai_input['date'].max() < today and current_median > 0:
                ai_input = pd.concat([ai_input, pd.DataFrame({'date': [today], 'price_cpl': [current_median]})], ignore_index=True)

            future_df = ai_model.predict_horizon(ai_input, days=14, tgp=current_tgp)

            if future_df is not None and not future_df.empty:
                tomorrow = future_df.iloc[0]
                hike_prob = float(tomorrow.get('hike_probability', 0)) * 100

                # Determine status
                if hike_prob > 70:
                    status_label = "HIKE_IMMINENT"
                    advice = "Buy Now"
                elif hike_prob > 50:
                    status_label = "WARNING"
                    advice = "Fill Up"
                elif current_median > 0 and current_median < (current_tgp + 10):
                    status_label = "BOTTOM"
                    advice = "Buy"
                else:
                    try:
                        delta_7d = future_df.iloc[min(6, len(future_df) - 1)]['predicted_price'] - current_median
                        if delta_7d < -2.0:
                            status_label = "DROPPING"
                            advice = "Wait"
                        else:
                            status_label = "STABLE"
                            advice = "Check App"
                    except Exception:
                        status_label = "STABLE"
                        advice = "Hold"

                # Use cycle info to override if available
                if cycle_info and cycle_info.get('phase') == 'RESTORATION' and cycle_info.get('confidence', 0) > 0.7:
                    if status_label not in ['HIKE_IMMINENT', 'WARNING']:
                        status_label = "RISING"
                        advice = "Buy Now"

                forecast_data = {
                    "dates": future_df['date'].astype(str).tolist(),
                    "prices": future_df['predicted_price'].tolist(),
                    "low": future_df['predicted_low'].tolist() if 'predicted_low' in future_df.columns else [],
                    "high": future_df['predicted_high'].tolist() if 'predicted_high' in future_df.columns else [],
                }

        # Savings insight
        savings_insight = "Market is stable."
        try:
            prices = forecast_data['prices']
            if prices and current_median > 0:
                min_f, max_f = min(prices), max(prices)
                if advice in ["Buy Now", "Fill Up"]:
                    savings_insight = f"⚡ Fill up now! Prices rising to {max_f:.0f}c soon."
                elif advice == "Wait":
                    save = (current_median - min_f) * 0.50
                    savings_insight = f"📉 Prices dropping. Wait to save ~${save:.2f}."
                elif advice == "Buy":
                    savings_insight = f"💰 Near cycle bottom. Good time to fill up."
        except Exception:
            pass

        # History
        history = {"dates": [], "prices": []}
        if daily_df is not None and not daily_df.empty:
            h = daily_df.sort_values('day').tail(90)
            history = {
                "dates": h['day'].dt.strftime('%Y-%m-%d').tolist(),
                "prices": h['price_cpl'].tolist()
            }

        return clean_nan({
            "status": status_label,
            "advice": advice,
            "advice_type": "success" if advice in ["Buy", "Buy Now", "Fill Up"] else ("warning" if advice == "Wait" else "info"),
            "hike_probability": round(hike_prob, 1),
            "current_avg": round(current_avg, 1),
            "current_median": round(current_median, 1),
            "station_count": station_count,
            "last_updated": datetime.now().strftime("%H:%M"),
            "savings_insight": savings_insight,
            "cycle": {
                "phase": cycle_info.get('phase', 'UNKNOWN') if cycle_info else 'UNKNOWN',
                "days_in_phase": cycle_info.get('days_in_phase', 0) if cycle_info else 0,
                "days_remaining": cycle_info.get('estimated_days_remaining', 0) if cycle_info else 0,
                "confidence": cycle_info.get('confidence', 0) if cycle_info else 0,
            },
            "ticker": {
                "tgp": round(current_tgp, 1),
                "raw_tgp": round(raw_tgp, 1),
                "forecast_tgp_anchor": round(current_tgp, 1),
                "tgp_anchor_reason": tgp_anchor_reason,
                "tgp_source": trend.get("tgp_source", "Unknown"),
                "oil": round(current_oil, 2) if current_oil else 0,
                "mogas": round(current_mogas, 1) if current_mogas else 0,
                "fx": round(current_fx, 4) if current_fx else 0,
                "import_parity_lag": trend.get('import_parity_lag', 'NEUTRAL')
            },
            "history": history,
            "forecast": forecast_data
        })

    except Exception as e:
        logger.error(f"market-status error: {e}", exc_info=True)
        return {"status": "ERROR", "advice": "Retry", "ticker": {}, "history": {"dates": [], "prices": []}, "forecast": {"dates": [], "prices": []}}


@app.get("/api/stations")
async def get_stations(state: str = "QLD"):
    """Station map data with prices."""
    try:
        live_df = await run_in_threadpool(load_live_data_latest, state=state.upper())
        if live_df.empty:
            return []

        live_df = _enrich_live_df(live_df)
        live_df = live_df.rename(columns={"price_cpl": "price", "latitude": "lat", "longitude": "lng"})
        live_df = live_df.dropna(subset=['lat', 'lng'])

        med = live_df['price'].median()
        live_df['is_cheap'] = (live_df['price'] < med).astype(int)
        return clean_nan(live_df.to_dict(orient='records'))
    except Exception as e:
        logger.error(f"stations error: {e}")
        return []


class LocationRequest(BaseModel):
    latitude: float
    longitude: float


@app.post("/api/find_cheapest_nearby")
async def find_cheapest_nearby(loc: LocationRequest, request: Request):
    """Find cheapest stations within 15km of GPS position."""
    _rate_limit(request, "find_cheapest_nearby", 60, 60)
    try:
        live_df = await run_in_threadpool(load_live_data_latest, state=None)
        if live_df.empty:
            return []

        live_df = _enrich_live_df(live_df)

        results = []
        for _, row in live_df.iterrows():
            try:
                dist = haversine(loc.longitude, loc.latitude, float(row['longitude']), float(row['latitude']))
                if dist <= 15.0:
                    results.append({
                        "name": str(row.get('name', 'Station')),
                        "price": float(row['price_cpl']),
                        "distance": round(dist, 1),
                        "brand": str(row.get('brand', '')),
                        "suburb": str(row.get('suburb', '')),
                        "lat": float(row.get('latitude', 0)),
                        "lng": float(row.get('longitude', 0)),
                    })
            except Exception:
                continue

        results.sort(key=lambda x: (x['price'], x['distance']))
        return clean_nan(results[:10])
    except Exception as e:
        logger.error(f"find_cheapest error: {e}")
        return []


class RouteRequest(BaseModel):
    start: str
    end: str


@app.post("/api/planner")
async def plan_route(req: RouteRequest, request: Request):
    """Route planner — find cheapest stops along a trip."""
    _rate_limit(request, "planner", 20, 60)
    try:
        res = await run_in_threadpool(route_optimizer.optimize_route, req.start, req.end)
        if res and 'stations' in res:
            res['stations'] = res['stations'].to_dict(orient='records')
        return clean_nan(res if res else {"error": "Route not found"})
    except Exception as e:
        logger.error(f"planner error: {e}")
        return {"error": str(e)}


@app.get("/api/analytics")
async def get_analytics(state: str = "QLD"):
    """Historical analytics, forecast, and suburb rankings."""
    try:
        state = state.upper()
        daily_df = await run_in_threadpool(_get_daily_data, state)

        if (daily_df is None or daily_df.empty):
            live_df = await run_in_threadpool(load_live_data_latest, state=state)
            if not live_df.empty:
                median = live_df['price_cpl'].median()
                if median > 0:
                    daily_df = pd.DataFrame({'day': [pd.Timestamp.now().normalize()], 'price_cpl': [median]})

        forecast = {"forecast_dates": [], "forecast_mean": [], "forecast_low": [], "forecast_high": []}

        if daily_df is not None and not daily_df.empty and ai_model:
            capital = config.STATES.get(state, config.STATES["QLD"])["capital"]
            trend = await run_in_threadpool(tgp_forecast.analyze_trend, city=capital)
            raw_tgp = _safe_float(trend.get('current_tgp'), 165.0)
            live_for_anchor = await run_in_threadpool(load_live_data_latest, state=state)
            current_tgp, _ = _forecast_tgp_anchor(raw_tgp, live_for_anchor, daily_df, trend)

            ai_input = daily_df.rename(columns={'day': 'date'})
            future_df = ai_model.predict_horizon(ai_input, days=14, tgp=current_tgp)
            if future_df is not None and not future_df.empty:
                forecast = {
                    "forecast_dates": future_df['date'].astype(str).tolist(),
                    "forecast_mean": future_df['predicted_price'].tolist(),
                    "forecast_low": future_df['predicted_low'].tolist() if 'predicted_low' in future_df.columns else [],
                    "forecast_high": future_df['predicted_high'].tolist() if 'predicted_high' in future_df.columns else [],
                }

        history = {}
        if daily_df is not None and not daily_df.empty:
            history = {
                "dates": daily_df['day'].dt.strftime('%Y-%m-%d').tolist(),
                "values": daily_df['price_cpl'].tolist()
            }

        # Suburb ranking
        suburbs = []
        try:
            live_df = await run_in_threadpool(load_live_data_latest, state=state)
            live_df = _enrich_live_df(live_df)
            if not live_df.empty and 'suburb' in live_df.columns:
                stats = live_df.groupby('suburb')['price_cpl'].agg(['mean', 'count']).reset_index()
                stats = stats[stats['count'] >= 2].sort_values('mean').head(10)
                suburbs = [{"suburb": str(r['suburb']), "price": round(float(r['mean']), 1)} for _, r in stats.iterrows()]
        except Exception:
            pass

        return clean_nan({"trend": {"history": history, "sarimax": forecast}, "suburb_ranking": suburbs})
    except Exception as e:
        logger.error(f"analytics error: {e}")
        return {"trend": {"history": {}, "sarimax": {}}, "suburb_ranking": []}


@app.get("/api/sentiment")
async def get_sentiment():
    """News sentiment analysis."""
    try:
        res = await run_in_threadpool(market_news.get_market_news)
        return clean_nan(res)
    except Exception as e:
        logger.error(f"sentiment error: {e}")
        return {"global": [], "domestic": []}


@app.get("/api/market-context")
async def get_market_context(state: str = "QLD"):
    """Market intelligence — why is fuel priced this way?"""
    try:
        if not market_context:
            return {"error": "Market context engine not available"}

        state = state.upper()
        live_df = await run_in_threadpool(load_live_data_latest, state=state)
        current_avg = float(live_df['price_cpl'].mean()) if not live_df.empty else 165.0

        capital = config.STATES.get(state, config.STATES["QLD"])["capital"]
        trend = await run_in_threadpool(tgp_forecast.analyze_trend, city=capital)

        raw_tgp = _safe_float(trend.get('current_tgp'), 165.0)
        daily_for_anchor = None
        try:
            daily_for_anchor = await run_in_threadpool(_get_daily_data, state)
        except Exception:
            daily_for_anchor = None
        tgp, _ = _forecast_tgp_anchor(raw_tgp, live_df, daily_for_anchor, trend)
        brent = _trend_value(trend, "current_oil", "oil_price_usd", fallback=75.0)
        fx = _trend_value(trend, "current_fx", "aud_usd", fallback=0.65)

        # Get cycle info
        ci = None
        if cycle_detector:
            try:
                daily_df = await run_in_threadpool(_get_daily_data, state)
                if daily_df is not None and len(daily_df) > 5:
                    ci = cycle_detector.detect_current_regime(daily_df['price_cpl'])
            except Exception:
                pass

        # Get news for context
        news = None
        try:
            news_data = await run_in_threadpool(market_news.get_market_news)
            news_items = []
            for feed_key in ['global', 'domestic']:
                items = news_data.get(feed_key, [])
                if isinstance(items, list):
                    news_items.extend(items[:3])
            news = news_items if news_items else None
        except Exception:
            pass

        result = market_context.generate_context(
            state=state,
            current_avg=current_avg,
            tgp=tgp,
            brent_usd=brent,
            aud_usd=fx,
            cycle_info=ci,
            news_items=news
        )
        return clean_nan(result)
    except Exception as e:
        logger.error(f"market-context error: {e}")
        return {"error": str(e)}


@app.get("/api/supply/summary")
async def get_supply_summary():
    """National petroleum supply summary."""
    try:
        if not supply_engine:
            return {"error": "Supply data engine not available"}
        result = supply_engine.get_supply_summary()
        return clean_nan(result)
    except Exception as e:
        logger.error(f"supply summary error: {e}")
        return {"error": str(e)}


@app.get("/api/supply/stocks")
async def get_supply_stocks():
    """Detailed national petroleum stock levels."""
    try:
        if not supply_engine:
            return {"error": "Supply data engine not available"}
        stocks = supply_engine.get_national_stocks()
        allocation = supply_engine.calculate_fuel_allocation()
        imports = supply_engine.get_import_statistics()
        return clean_nan({"stocks": stocks, "allocation": allocation, "imports": imports})
    except Exception as e:
        logger.error(f"supply stocks error: {e}")
        return {"error": str(e)}


@app.get("/api/supply/tankers")
async def get_supply_tankers():
    """Inbound oil tanker positions and ETAs."""
    try:
        if not tanker_tracker:
            return {"tankers": [], "ports": {}}
        tankers = tanker_tracker.get_inbound_tankers()
        ports = tanker_tracker.get_port_activity()
        return clean_nan({"tankers": tankers, "ports": ports})
    except Exception as e:
        logger.error(f"tankers error: {e}")
        return {"tankers": [], "ports": {}}


@app.get("/api/health")
async def get_health():
    """System health check."""
    try:
        snapshot_age = None
        if os.path.exists(SNAPSHOT_FILE):
            mtime = os.path.getmtime(SNAPSHOT_FILE)
            snapshot_age = round((time.time() - mtime) / 60, 1)

        return {
            "status": "healthy",
            "version": "4.0.0",
            "timestamp": datetime.now().isoformat(),
            "modules": {
                "data_store": db is not None,
                "ai_model": ai_model is not None,
                "model_loaded": MODEL_LOADED,
                "cycle_detector": cycle_detector is not None,
                "market_context": market_context is not None,
                "supply_engine": supply_engine is not None,
                "tanker_tracker": tanker_tracker is not None,
            },
            "data": {
                "active_states": config.ACTIVE_STATES,
                "snapshot_age_minutes": snapshot_age,
                "snapshot_file": os.path.exists(SNAPSHOT_FILE),
                "history_file": os.path.exists(HISTORY_FILE),
                "db_file": os.path.exists(config.DB_FILE) if hasattr(config, 'DB_FILE') else False,
            }
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ============================================================
# STATIC FILES & ROUTES
# ============================================================

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def read_root():
    return FileResponse('static/index.html')


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
