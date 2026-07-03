"""
data_store.py — SQLite persistence layer for the Australian Fuel-Price dashboard.

Tables
------
snapshots        Per-station scrape rows  (UNIQUE site_id + scraped_at)
daily_stats      Aggregated daily prices  (UNIQUE date + state)
tgp_history      Terminal Gate Prices      (UNIQUE date + city)
market_data      Brent / AUD / MOGAS-95   (UNIQUE date)
predictions      Model output archive
"""

import os
import sqlite3
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default database path — same directory as this module
# ---------------------------------------------------------------------------
_DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "fuel_data.db"
)

# Price sanity bounds (cents per litre)
_PRICE_MIN = 80.0
_PRICE_MAX = 350.0


def _valid_price(price: float) -> bool:
    """Return True if a price falls within the acceptable range."""
    try:
        p = float(price)
        return _PRICE_MIN <= p <= _PRICE_MAX
    except (TypeError, ValueError):
        return False


class FuelDataStore:
    """Lightweight SQLite wrapper for fuel-price data."""

    # ------------------------------------------------------------------
    # Construction / schema
    # ------------------------------------------------------------------
    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or _DEFAULT_DB_PATH
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    site_id     TEXT    NOT NULL,
                    price_cpl   REAL    NOT NULL,
                    reported_at TEXT,
                    lat         REAL,
                    lng         REAL,
                    name        TEXT,
                    brand       TEXT,
                    suburb      TEXT,
                    region      TEXT,
                    state       TEXT,
                    scraped_at  TEXT    NOT NULL,
                    UNIQUE(site_id, scraped_at)
                );

                CREATE TABLE IF NOT EXISTS daily_stats (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    date         TEXT    NOT NULL,
                    state        TEXT    NOT NULL,
                    median_price REAL,
                    mean_price   REAL,
                    min_price    REAL,
                    max_price    REAL,
                    count        INTEGER,
                    UNIQUE(date, state)
                );

                CREATE TABLE IF NOT EXISTS tgp_history (
                    id   INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    city TEXT NOT NULL,
                    tgp  REAL NOT NULL,
                    UNIQUE(date, city)
                );

                CREATE TABLE IF NOT EXISTS market_data (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    date      TEXT NOT NULL,
                    brent_usd REAL,
                    aud_usd   REAL,
                    mogas_95  REAL,
                    UNIQUE(date)
                );

                CREATE TABLE IF NOT EXISTS predictions (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    date            TEXT NOT NULL,
                    state           TEXT NOT NULL,
                    predicted_price REAL,
                    actual_price    REAL,
                    model_version   TEXT
                );
                """
            )
        logger.info("FuelDataStore schema ensured at %s", self.db_path)

    # ------------------------------------------------------------------
    # Snapshots
    # ------------------------------------------------------------------
    def save_snapshot(self, df: pd.DataFrame) -> int:
        """
        Persist a DataFrame of station-level scrape rows.

        Expected columns (at minimum): site_id, price_cpl, scraped_at.
        Extra columns are mapped when present.  Rows with invalid prices
        are silently dropped.

        Returns the number of rows inserted (duplicates ignored).
        """
        if df is None or df.empty:
            return 0

        df = df.copy()
        df.columns = [c.lower().strip() for c in df.columns]

        # Latitude / longitude normalisation
        for alias, canon in [("latitude", "lat"), ("longitude", "lng")]:
            if alias in df.columns and canon not in df.columns:
                df.rename(columns={alias: canon}, inplace=True)

        required = {"site_id", "price_cpl", "scraped_at"}
        if not required.issubset(set(df.columns)):
            logger.warning(
                "save_snapshot: missing required columns %s — have %s",
                required - set(df.columns),
                list(df.columns),
            )
            return 0

        # Price validation
        df["price_cpl"] = pd.to_numeric(df["price_cpl"], errors="coerce")
        df = df[df["price_cpl"].between(_PRICE_MIN, _PRICE_MAX)]

        if df.empty:
            return 0

        cols = [
            "site_id", "price_cpl", "reported_at", "lat", "lng",
            "name", "brand", "suburb", "region", "state", "scraped_at",
        ]
        for c in cols:
            if c not in df.columns:
                df[c] = None

        df["site_id"] = df["site_id"].astype(str)
        df["scraped_at"] = df["scraped_at"].astype(str)

        rows = df[cols].values.tolist()
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        sql = f"INSERT OR IGNORE INTO snapshots ({col_names}) VALUES ({placeholders})"

        inserted = 0
        with self._connect() as conn:
            cursor = conn.executemany(sql, rows)
            inserted = cursor.rowcount
            conn.commit()

        logger.info("save_snapshot: inserted %d / %d rows", inserted, len(rows))
        return inserted

    def get_latest_snapshot(self, state: str) -> pd.DataFrame:
        """
        Return the most recent scrape batch for *state*, identified by
        the latest ``scraped_at`` timestamp for that state.
        """
        sql = """
            SELECT site_id, price_cpl, reported_at, lat, lng,
                   name, brand, suburb, region, state, scraped_at
            FROM   snapshots
            WHERE  state = ?
              AND  scraped_at = (
                       SELECT MAX(scraped_at)
                       FROM   snapshots
                       WHERE  state = ?
                   )
            ORDER BY price_cpl ASC
        """
        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=(state, state))
        return df

    # ------------------------------------------------------------------
    # Daily statistics
    # ------------------------------------------------------------------
    def save_daily_stats(
        self, date, state: str, prices_series: pd.Series
    ) -> None:
        """Compute and store aggregate stats for *date* + *state*."""
        prices = pd.to_numeric(prices_series, errors="coerce").dropna()
        prices = prices[(prices >= _PRICE_MIN) & (prices <= _PRICE_MAX)]

        if prices.empty:
            logger.warning("save_daily_stats: no valid prices for %s / %s", date, state)
            return

        date_str = pd.Timestamp(date).strftime("%Y-%m-%d")
        row = (
            date_str,
            state,
            float(np.median(prices)),
            float(np.mean(prices)),
            float(np.min(prices)),
            float(np.max(prices)),
            int(len(prices)),
        )

        sql = """
            INSERT OR IGNORE INTO daily_stats
                (date, state, median_price, mean_price, min_price, max_price, count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        with self._connect() as conn:
            conn.execute(sql, row)
            conn.commit()

    def get_daily_stats(self, state: str, days: int = 90) -> pd.DataFrame:
        """
        Return a DataFrame with columns ``day`` (datetime) and
        ``price_cpl`` (median price) for the last *days* days.
        """
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        sql = """
            SELECT date AS day, median_price AS price_cpl
            FROM   daily_stats
            WHERE  state = ? AND date >= ?
            ORDER  BY date ASC
        """
        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=(state, cutoff))

        if not df.empty:
            df["day"] = pd.to_datetime(df["day"])
        else:
            df = pd.DataFrame(columns=["day", "price_cpl"])
            df["day"] = pd.to_datetime(df["day"])
        return df

    # ------------------------------------------------------------------
    # TGP history
    # ------------------------------------------------------------------
    def save_tgp(self, date, city: str, tgp: float) -> None:
        """Store a single Terminal Gate Price observation."""
        if not _valid_price(tgp):
            logger.warning("save_tgp: invalid tgp %.2f for %s/%s", tgp, date, city)
            return
        date_str = pd.Timestamp(date).strftime("%Y-%m-%d")
        sql = "INSERT OR IGNORE INTO tgp_history (date, city, tgp) VALUES (?, ?, ?)"
        with self._connect() as conn:
            conn.execute(sql, (date_str, city, float(tgp)))
            conn.commit()

    def get_tgp_history(self, city: str, days: int = 90) -> pd.DataFrame:
        """Return TGP history for *city* over the last *days* days."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        sql = """
            SELECT date, city, tgp
            FROM   tgp_history
            WHERE  city = ? AND date >= ?
            ORDER  BY date ASC
        """
        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=(city, cutoff))
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        return df

    # ------------------------------------------------------------------
    # Market data (Brent, AUD/USD, MOGAS-95)
    # ------------------------------------------------------------------
    def save_market_data(
        self,
        date,
        brent: float | None = None,
        aud_usd: float | None = None,
        mogas: float | None = None,
    ) -> None:
        date_str = pd.Timestamp(date).strftime("%Y-%m-%d")
        sql = """
            INSERT OR IGNORE INTO market_data (date, brent_usd, aud_usd, mogas_95)
            VALUES (?, ?, ?, ?)
        """
        with self._connect() as conn:
            conn.execute(sql, (date_str, brent, aud_usd, mogas))
            conn.commit()

    def get_market_history(self, days: int = 90) -> pd.DataFrame:
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        sql = """
            SELECT date, brent_usd, aud_usd, mogas_95
            FROM   market_data
            WHERE  date >= ?
            ORDER  BY date ASC
        """
        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=(cutoff,))
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        return df

    # ------------------------------------------------------------------
    # Predictions archive
    # ------------------------------------------------------------------
    def save_prediction(
        self,
        date,
        state: str,
        predicted_price: float,
        actual_price: float | None = None,
        model_version: str | None = None,
    ) -> None:
        date_str = pd.Timestamp(date).strftime("%Y-%m-%d")
        sql = """
            INSERT INTO predictions
                (date, state, predicted_price, actual_price, model_version)
            VALUES (?, ?, ?, ?, ?)
        """
        with self._connect() as conn:
            conn.execute(sql, (date_str, state, predicted_price, actual_price, model_version))
            conn.commit()

    # ------------------------------------------------------------------
    # Backfill from existing CSV files
    # ------------------------------------------------------------------
    def backfill_from_csv(self, csv_path: str) -> int:
        """
        Import a CSV of historical station-level rows into the snapshots
        table.  Handles two known formats:

        * **7-column** (history):
          site_id, price_cpl, reported_at, region, latitude, longitude
          *(header may or may not include a 7th column)*

        * **8-column** (live collection):
          site_id, price_cpl, reported_at, region, state, latitude, longitude, scraped_at

        Returns the number of rows inserted.
        """
        if not os.path.exists(csv_path):
            logger.error("backfill_from_csv: file not found — %s", csv_path)
            return 0

        try:
            df = pd.read_csv(csv_path, low_memory=False)
        except Exception as exc:
            logger.error("backfill_from_csv: read error — %s", exc)
            return 0

        df.columns = [c.lower().strip() for c in df.columns]

        # Detect format by column count / presence
        has_scraped = "scraped_at" in df.columns
        has_state = "state" in df.columns

        # Normalise latitude/longitude → lat/lng
        for alias, canon in [("latitude", "lat"), ("longitude", "lng")]:
            if alias in df.columns and canon not in df.columns:
                df.rename(columns={alias: canon}, inplace=True)

        # 7-column (no scraped_at): use reported_at as scraped_at
        if not has_scraped:
            if "reported_at" in df.columns:
                df["scraped_at"] = df["reported_at"]
            else:
                # last-resort: use file mtime
                mtime = datetime.fromtimestamp(os.path.getmtime(csv_path))
                df["scraped_at"] = mtime.strftime("%Y-%m-%d %H:%M:%S")

        # 7-column (no state): default to QLD (Brisbane-centric legacy file)
        if not has_state:
            df["state"] = "QLD"

        # Ensure site_id exists
        if "site_id" not in df.columns:
            logger.error("backfill_from_csv: no site_id column")
            return 0

        return self.save_snapshot(df)

    # ------------------------------------------------------------------
    # Aggregate snapshots → daily stats
    # ------------------------------------------------------------------
    def aggregate_daily_from_snapshots(self, state: str) -> int:
        """
        Walk all snapshot rows for *state*, compute daily medians, and
        persist them into daily_stats.  Returns number of days aggregated.
        """
        sql = """
            SELECT DATE(scraped_at) AS day, price_cpl
            FROM   snapshots
            WHERE  state = ?
              AND  price_cpl BETWEEN ? AND ?
            ORDER  BY day
        """
        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=(state, _PRICE_MIN, _PRICE_MAX))

        if df.empty:
            return 0

        days_count = 0
        for day, group in df.groupby("day"):
            self.save_daily_stats(day, state, group["price_cpl"])
            days_count += 1

        logger.info(
            "aggregate_daily_from_snapshots: %d days aggregated for %s",
            days_count,
            state,
        )
        return days_count

    # ------------------------------------------------------------------
    # Bulk daily-price retrieval (no date cutoff)
    # ------------------------------------------------------------------
    def get_all_daily_prices(self, state: str) -> pd.DataFrame:
        """
        Return *all* daily stats for *state* as a DataFrame with columns
        ``day`` (datetime) and ``price_cpl`` (median).
        """
        sql = """
            SELECT date AS day, median_price AS price_cpl
            FROM   daily_stats
            WHERE  state = ?
            ORDER  BY date ASC
        """
        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=(state,))

        if not df.empty:
            df["day"] = pd.to_datetime(df["day"])
        else:
            df = pd.DataFrame(columns=["day", "price_cpl"])
            df["day"] = pd.to_datetime(df["day"])
        return df


# Alias for backward compatibility
DataStore = FuelDataStore

