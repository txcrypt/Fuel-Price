"""
TGP Forecast — Terminal Gate Price Analysis & Forecasting
Scrapes live TGP from AIP/Viva, fetches market indicators (Brent, AUD/USD),
and builds a trend-anchored TGP history for dashboard consumption.

Changes from v1:
  - Frankfurter API as primary AUD/USD source (no API key needed)
  - yfinance as fallback for FX and oil data
  - `current_mogas` included in analyze_trend() return dict
  - get_import_parity_price() method added
  - Logging instead of print statements
  - 6-hour cache retained
"""

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
import warnings
import logging
from io import BytesIO
from datetime import datetime, timedelta
from urllib.parse import urljoin

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)

# --- Constants ---
AIP_URL = "https://www.aip.com.au/pricing/terminal-gate-prices"
VIVA_URL = "https://www.vivaenergy.com.au/quick-links/terminal-gate-pricing"
FRANKFURTER_URL = "https://api.frankfurter.app/latest?from=USD&to=AUD"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Import Parity Price constants
CRACK_SPREAD_USD_BBL = 15.0      # Typical crack spread (MOPS95 over Brent)
EXCISE_CPL = 51.1                # Federal excise (cents per litre)
GST_RATE = 0.10                  # 10% GST
QUALITY_PREMIUM_CPL = 2.0        # ULP95 quality premium
SHIPPING_CPL = 3.5               # Avg freight Singapore→Australia
WHARFAGE_CPL = 1.0               # Terminal handling

# --- Cache ---
_market_data_cache = {}
_market_data_cache_time = {}

_live_tgp_cache = {}
_live_tgp_cache_time = {}
_live_tgp_source_cache = {}

_fx_cache = {}
_fx_cache_time = {}


# ========================================================================== #
#  AUD/USD Exchange Rate (Frankfurter primary, yfinance fallback)
# ========================================================================== #

def _fetch_aud_usd_frankfurter() -> float | None:
    """
    Fetch AUD/USD rate from Frankfurter API (free, no key required).
    Frankfurter returns: {"base":"USD","date":"...","rates":{"AUD": x.xx}}
    where AUD value = how many AUD per 1 USD.
    We want AUD/USD (how many USD per 1 AUD) = 1 / rates.AUD.
    """
    cache_key = "frankfurter"
    now = datetime.now()

    if cache_key in _fx_cache and cache_key in _fx_cache_time:
        if (now - _fx_cache_time[cache_key]).total_seconds() < 21600:  # 6h cache
            return _fx_cache[cache_key]

    try:
        resp = requests.get(FRANKFURTER_URL, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            aud_per_usd = data.get("rates", {}).get("AUD")
            if aud_per_usd and aud_per_usd > 0:
                aud_usd = round(1.0 / aud_per_usd, 6)
                _fx_cache[cache_key] = aud_usd
                _fx_cache_time[cache_key] = now
                logger.info("Frankfurter AUD/USD: %.4f", aud_usd)
                return aud_usd
        logger.warning("Frankfurter returned unexpected response: %d", resp.status_code)
    except Exception as e:
        logger.warning("Frankfurter API error: %s", e)

    return None


def _fetch_aud_usd_yfinance() -> float | None:
    """Fallback: fetch AUD/USD from yfinance."""
    try:
        import yfinance as yf
        ticker = yf.Ticker("AUDUSD=X")
        hist = ticker.history(period="5d")
        if not hist.empty:
            val = float(hist["Close"].iloc[-1])
            if 0.3 < val < 1.2:  # sanity check
                logger.info("yfinance AUD/USD fallback: %.4f", val)
                return round(val, 6)
    except Exception as e:
        logger.warning("yfinance AUD/USD fallback error: %s", e)
    return None


def get_aud_usd() -> float:
    """
    Get the current AUD/USD exchange rate.
    Primary: Frankfurter API (free, no key)
    Fallback: yfinance
    Emergency: 0.65
    """
    rate = _fetch_aud_usd_frankfurter()
    if rate is not None:
        return rate

    rate = _fetch_aud_usd_yfinance()
    if rate is not None:
        return rate

    logger.error("All AUD/USD sources failed. Using emergency fallback 0.65")
    return 0.65


# ========================================================================== #
#  Market Data (Oil + FX)
# ========================================================================== #

def fetch_market_data(days=90):
    """
    Fetches market indicators: Brent Crude (Oil) and AUD/USD Exchange Rate.
    Returns a merged DataFrame with columns: oil_price, aud_fx
    """
    global _market_data_cache, _market_data_cache_time
    now = datetime.now()
    if days in _market_data_cache and days in _market_data_cache_time:
        if (now - _market_data_cache_time[days]).total_seconds() < 21600:  # 6 hour cache
            return _market_data_cache[days].copy()

    import yfinance as yf

    try:
        # Brent Crude Oil (BZ=F)
        oil = yf.Ticker("BZ=F").history(period=f"{days+10}d")['Close'].rename("oil_price")

        # AUD/USD: try Frankfurter for latest, yfinance for history
        fx = yf.Ticker("AUDUSD=X").history(period=f"{days+10}d")['Close'].rename("aud_fx")

        if oil.empty or fx.empty:
            raise ValueError("Empty data from yfinance")

        # Merge and clean
        df = pd.concat([oil, fx], axis=1).ffill().bfill()
        df.index = pd.to_datetime(df.index).tz_localize(None)

        # Override the latest FX value with Frankfurter if available
        live_fx = _fetch_aud_usd_frankfurter()
        if live_fx is not None and not df.empty:
            df.iloc[-1, df.columns.get_loc("aud_fx")] = live_fx

        # Trim to requested days
        res = df.tail(days)
        _market_data_cache[days] = res
        _market_data_cache_time[days] = now
        return res.copy()

    except Exception as e:
        logger.error("Error fetching market data: %s. Using synthetic fallback.", e)
        dates = pd.date_range(end=datetime.now(), periods=days)

        # Use Frankfurter for latest FX even in fallback
        live_fx = get_aud_usd()

        df = pd.DataFrame({
            'oil_price': [75.0] * days,
            'aud_fx': [live_fx] * days
        }, index=dates)
        _market_data_cache[days] = df
        _market_data_cache_time[days] = now
        return df.copy()


# ========================================================================== #
#  Live TGP Scraping
# ========================================================================== #

def _parse_tgp_value(raw) -> float | None:
    """Parse and sanity-check a cents-per-litre TGP value."""
    try:
        price = float(re.sub(r'[^\d.]', '', str(raw)))
    except (ValueError, TypeError):
        return None

    # Some feeds expose tenths of a cent (e.g. 1799 => 179.9 cpl).
    if 1000 < price < 2500:
        price = price / 10.0

    if 100 < price < 250:
        return price
    return None


def _cache_live_tgp(city_upper: str, value: float, timestamp: datetime, source: str) -> float:
    _live_tgp_cache[city_upper] = value
    _live_tgp_cache_time[city_upper] = timestamp
    _live_tgp_source_cache[city_upper] = source
    return value


def _mogas_cpl(brent_usd: float, aud_usd: float) -> float:
    """Approximate MOGAS/crude landed component in AUD cents per litre."""
    safe_aud = aud_usd if aud_usd > 0 else 0.65
    barrels_to_litres = 158.987
    return (brent_usd + CRACK_SPREAD_USD_BBL) / barrels_to_litres / safe_aud * 100


def _extract_aip_tgp_from_table(soup: BeautifulSoup, city_upper: str) -> float | None:
    """
    Extract city TGP from a rendered AIP HTML table when Drupal exposes rows.
    Handles both city-as-row and city-as-column table layouts.
    """
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue

        headers = [cell.get_text(" ", strip=True).upper() for cell in rows[0].find_all(["th", "td"])]
        city_index = next((idx for idx, header in enumerate(headers) if city_upper == header), None)

        if city_index is not None:
            for row in reversed(rows[1:]):
                cells = row.find_all(["th", "td"])
                if city_index < len(cells):
                    value = _parse_tgp_value(cells[city_index].get_text(" ", strip=True))
                    if value is not None:
                        return value

        for row in rows:
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            cell_text = [cell.get_text(" ", strip=True) for cell in cells]
            if city_upper not in " ".join(cell_text).upper():
                continue
            for text in cell_text:
                if city_upper in text.upper():
                    continue
                value = _parse_tgp_value(text)
                if value is not None:
                    return value

    return None


def _find_aip_daily_workbook_url(soup: BeautifulSoup, base_url: str) -> str | None:
    """Find the current non-annual AIP TGP workbook link on an AIP page."""
    candidates = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(" ", strip=True)
        target = f"{href} {text}".lower()
        if ".xlsx" not in target:
            continue
        if "annual" in target:
            continue
        if "aip_tgp_data" in target or "tgp" in target:
            candidates.append(urljoin(base_url, href))

    return candidates[0] if candidates else None


def _find_aip_historical_page_url(soup: BeautifulSoup, base_url: str) -> str | None:
    """Find AIP's historical TGP page, which currently hosts the daily workbook."""
    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(" ", strip=True)
        target = f"{href} {text}".lower()
        if "historical" in target and "tgp" in target:
            return urljoin(base_url, href)
    return None


def _fetch_aip_workbook_url(soup: BeautifulSoup, headers: dict[str, str]) -> str | None:
    workbook_url = _find_aip_daily_workbook_url(soup, AIP_URL)
    if workbook_url:
        return workbook_url

    historical_url = _find_aip_historical_page_url(soup, AIP_URL)
    if not historical_url:
        return None

    try:
        response = requests.get(historical_url, headers=headers, timeout=10)
        if response.status_code != 200:
            logger.warning("AIP historical TGP page returned HTTP %d", response.status_code)
            return None
        historical_soup = BeautifulSoup(response.text, "html.parser")
        return _find_aip_daily_workbook_url(historical_soup, historical_url)
    except Exception as e:
        logger.warning("AIP historical TGP page fetch failed: %s", e)
        return None


def _extract_aip_tgp_from_workbook(workbook_bytes: bytes, city_upper: str) -> float | None:
    """Read the latest city value from AIP's official daily TGP workbook."""
    try:
        df = pd.read_excel(BytesIO(workbook_bytes), sheet_name="Petrol TGP")
    except Exception as e:
        logger.warning("AIP TGP workbook parse failed: %s", e)
        return None

    city_column = next((col for col in df.columns if str(col).strip().upper() == city_upper), None)
    if city_column is None:
        logger.warning("AIP TGP workbook has no column for %s", city_upper)
        return None

    values = pd.to_numeric(df[city_column], errors="coerce").dropna()
    if values.empty:
        logger.warning("AIP TGP workbook has no numeric values for %s", city_upper)
        return None

    return _parse_tgp_value(values.iloc[-1])


def fetch_live_tgp(city="BRISBANE"):
    """
    Scrapes the current Terminal Gate Price from AIP (Primary) or Viva (Secondary).
    Returns float (cents per litre) or None.
    """
    global _live_tgp_cache, _live_tgp_cache_time
    city_upper = city.upper()
    now = datetime.now()
    if city_upper in _live_tgp_cache and city_upper in _live_tgp_cache_time:
        if (now - _live_tgp_cache_time[city_upper]).total_seconds() < 21600:  # 6 hour cache
            return _live_tgp_cache[city_upper]

    # 1. Try AIP
    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(AIP_URL, headers=headers, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            value = _extract_aip_tgp_from_table(soup, city_upper)
            if value is not None:
                return _cache_live_tgp(city_upper, value, now, "AIP table")

            workbook_url = _fetch_aip_workbook_url(soup, headers)
            if workbook_url:
                workbook_response = requests.get(workbook_url, headers=headers, timeout=15)
                if workbook_response.status_code == 200:
                    value = _extract_aip_tgp_from_workbook(workbook_response.content, city_upper)
                    if value is not None:
                        return _cache_live_tgp(city_upper, value, now, "AIP workbook")
                else:
                    logger.warning("AIP TGP workbook returned HTTP %d", workbook_response.status_code)
            else:
                logger.warning("AIP TGP workbook link not found")
        else:
            logger.warning("AIP TGP page returned HTTP %d", r.status_code)
    except Exception as e:
        logger.warning("AIP TGP fetch failed: %s", e)

    # 2. Try Viva Energy (Fallback)
    try:
        r = requests.get(VIVA_URL, headers=headers, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            for row in soup.find_all('tr'):
                text = row.get_text().upper()
                if city_upper in text:
                    cols = row.find_all('td')
                    for col in cols:
                        raw = col.get_text().strip()
                        if not raw or city_upper in raw.upper():
                            continue
                        price = _parse_tgp_value(raw)
                        if price is not None:
                            return _cache_live_tgp(city_upper, price, now, "Viva fallback")
    except Exception as e:
        logger.warning("Viva TGP fetch failed: %s", e)

    return None


# ========================================================================== #
#  TGP History Construction
# ========================================================================== #

def get_tgp_history(days=90, city="BRISBANE"):
    """
    Returns a pandas Series of TGP history.
    Strategy:
    1. Get current LIVE TGP.
    2. Get Market Data (Oil, FX).
    3. Calculate 'Theoretical TGP' history based on Import Parity.
    4. Bias/Shift the theoretical curve so the last point matches the LIVE TGP.
    """
    # 1. Live Anchor
    live_tgp = fetch_live_tgp(city)
    if live_tgp is None:
        live_tgp = 170.0  # Emergency Fallback
        _live_tgp_source_cache[city.upper()] = "Emergency fallback"
        logger.warning("Using emergency TGP fallback of 170.0 cpl for %s", city)

    # 2. Market Drivers
    market_df = fetch_market_data(days)

    # 3. Calculate MOGAS/crude component in cents per litre.
    market_df['mogas_cpl'] = market_df.apply(
        lambda row: _mogas_cpl(float(row['oil_price']), float(row['aud_fx'])),
        axis=1,
    )

    # 4. Anchor to Reality
    last_theoretical = market_df['mogas_cpl'].iloc[-1]

    if last_theoretical > 0:
        # TGP is not just commodity cost; it includes a relatively stable basis
        # of excise, freight, terminal and wholesale margin. Preserve commodity
        # cpl moves with an additive basis instead of scaling the whole series.
        basis_cpl = live_tgp - last_theoretical
        tgp_series = market_df['mogas_cpl'] + basis_cpl
    else:
        tgp_series = pd.Series([live_tgp] * len(market_df), index=market_df.index)

    tgp_series.name = 'tgp'
    return tgp_series


# ========================================================================== #
#  Import Parity Price Calculation
# ========================================================================== #

def get_import_parity_price(tgp: float, brent_usd: float, aud_usd: float) -> dict:
    """
    Calculate the theoretical Import Parity Price (IPP) and compare to actual TGP.

    The IPP model:
      1. MOPS95 = (Brent + Crack Spread) / barrels_per_litre  → USD cpl
      2. Convert to AUD: MOPS95_AUD = MOPS95 / AUD_USD * 100
      3. Add shipping, quality premium, wharfage
      4. Add excise
      5. Add GST (10% on total)

    Parameters
    ----------
    tgp       : Current Terminal Gate Price in cpl
    brent_usd : Brent crude price in USD per barrel
    aud_usd   : AUD/USD exchange rate

    Returns
    -------
    dict with ipp_cpl, components, tgp_vs_ipp_delta, assessment
    """
    safe_aud = aud_usd if aud_usd > 0 else 0.65
    barrels_to_litres = 158.987

    # Step 1: MOPS95 proxy in USD/litre
    mops_usd_per_litre = (brent_usd + CRACK_SPREAD_USD_BBL) / barrels_to_litres

    # Step 2: Convert to AUD cpl
    mops_aud_cpl = round(mops_usd_per_litre / safe_aud * 100, 2)

    # Step 3: Landed cost
    landed_cost = mops_aud_cpl + SHIPPING_CPL + QUALITY_PREMIUM_CPL + WHARFAGE_CPL

    # Step 4: Add excise
    pre_gst = landed_cost + EXCISE_CPL

    # Step 5: Add GST
    gst = round(pre_gst * GST_RATE, 2)
    ipp = round(pre_gst + gst, 2)

    # Delta
    delta = round(tgp - ipp, 2)

    if delta > 3.0:
        assessment = "TGP is ABOVE import parity — wholesale margins are elevated."
    elif delta < -3.0:
        assessment = "TGP is BELOW import parity — wholesale discounting or lagged pricing."
    else:
        assessment = "TGP is closely aligned with import parity — efficient market pricing."

    return {
        "ipp_cpl": ipp,
        "components": {
            "mops95_aud_cpl": mops_aud_cpl,
            "shipping_cpl": SHIPPING_CPL,
            "quality_premium_cpl": QUALITY_PREMIUM_CPL,
            "wharfage_cpl": WHARFAGE_CPL,
            "landed_cost_cpl": round(landed_cost, 2),
            "excise_cpl": EXCISE_CPL,
            "gst_cpl": gst,
        },
        "tgp_actual_cpl": round(tgp, 2),
        "tgp_vs_ipp_delta_cpl": delta,
        "assessment": assessment,
        "inputs": {
            "brent_usd": brent_usd,
            "aud_usd": aud_usd,
            "crack_spread_usd": CRACK_SPREAD_USD_BBL,
        },
    }


# ========================================================================== #
#  Trend Analysis
# ========================================================================== #

def analyze_trend(city="BRISBANE"):
    """
    Returns the trend analysis for the Dashboard.

    Backward compatible with the original return dict, plus enriched fields:
      - current_mogas: latest MOGAS/crude component in AUD cpl
      - aud_usd: current AUD/USD rate
      - oil_price_usd: current Brent crude USD/bbl
      - import_parity: IPP analysis dict
    """
    history = get_tgp_history(days=30, city=city)
    current_tgp = history.iloc[-1]

    # Trend (Last 7 days)
    delta_7d = current_tgp - history.iloc[-8] if len(history) > 7 else 0
    trend_direction = "RISING" if delta_7d > 0.5 else "FALLING" if delta_7d < -0.5 else "STABLE"

    # Market data for enrichment
    market = fetch_market_data(days=20)

    # Current MOGAS (AUD) — fixes the ticker showing '--.-'
    current_mogas = None
    current_oil = None
    current_fx = None

    if not market.empty:
        current_oil = round(float(market['oil_price'].iloc[-1]), 2)
        current_fx = round(float(market['aud_fx'].iloc[-1]), 6)

        if current_fx and current_fx > 0:
            current_mogas = round(_mogas_cpl(current_oil, current_fx), 2)

    # Singapore Lag (Proxy using Oil 10 days ago vs today)
    if len(market) > 10:
        oil_10_ago = market['oil_price'].iloc[-11]
        oil_now = market['oil_price'].iloc[-1]
        lag_delta = (oil_now - oil_10_ago)
        lag_msg = (
            "ROCKET (Rising Cost)" if lag_delta > 2
            else "FEATHER (Dropping Cost)" if lag_delta < -2
            else "NEUTRAL"
        )
    else:
        lag_msg = "UNKNOWN"

    # Build IPP analysis if we have the data
    ipp_data = None
    if current_oil is not None and current_fx is not None:
        ipp_data = get_import_parity_price(current_tgp, current_oil, current_fx)

    result = {
        # Original fields (backward compatible)
        'current_tgp': round(current_tgp, 2),
        'trend_direction': trend_direction,
        'delta_7d': round(delta_7d, 2),
        'import_parity_lag': lag_msg,
        'history': {
            'dates': history.index.strftime('%Y-%m-%d').tolist(),
            'values': history.round(2).tolist()
        },
        # New enrichment fields
        'current_mogas': current_mogas,
        'aud_usd': current_fx,
        'oil_price_usd': current_oil,
        'import_parity': ipp_data,
        'tgp_source': _live_tgp_source_cache.get(city.upper(), 'Unknown'),
        'tgp_fetched_at': (
            _live_tgp_cache_time.get(city.upper()).isoformat()
            if city.upper() in _live_tgp_cache_time else None
        ),
    }

    return result
