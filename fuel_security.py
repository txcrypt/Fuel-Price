"""Fuel security: Australia's fuel stocks, obligations, sales, imports and refining, plus
Queensland's sales and Port of Brisbane fuel imports. Display only; nothing here feeds a forecast.

Sources (all public; fetched at most twice a day, last good copy kept on failure):
  APS  Australian Petroleum Statistics (DCCEEW), monthly xlsx, ~2-month lag, CC BY 4.0
  MSO  Minimum Stockholding Obligation weekly snapshot (DCCEEW), Tuesday stock, published Saturday, CC BY 4.0
  PBPL Port of Brisbane historical monthly trade xlsx, tonnes, ~1-month lag

Queensland's own fuel STOCKS are not published by anyone; only national stocks exist. The
payload says so rather than apportioning national stock by sales share.

APS/MSO parsing and URL discovery verified against the July-2026 extract and the 22 Sep 2026
weekly snapshot (layout notes kept with each function).
"""
import re
from datetime import datetime
from urllib.parse import unquote, urljoin

import pandas as pd
import requests

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
APS_PAGE = "https://www.energy.gov.au/publications/australian-petroleum-statistics-{year}"
MSO_PAGE = ("https://www.dcceew.gov.au/energy/security/australias-fuel-security/"
            "minimum-stockholding-obligation/statistics")
MONTHS = {m: i for i, m in enumerate(
    "january february march april may june july august september october november december".split(), 1)}

# wide monthly sheets we export, keyed by output name -> sheet name
WIDE = {
    "stocks_ml": "Stock volume by product",
    "stocks_kt": "Stock mass by product",
    "consumption_cover_days": "Consumption cover",
    "iea_net_import_cover": "IEA days net import cover",
    "stocks_incl_on_the_way_ml": "Stock volume incl. on the way",
    "iea_days_incl_on_the_way": "Stock IEA days incl. on the way",
    "sales_ml": "Sales of products",
    "refinery_ml": "Refinery production",
    "imports_ml": "Imports volume",
    "exports_ml": "Exports volume",
    "production": "Petroleum production",
}
# friendly product keys -> (sheet key, column label) for the headline block
HEADLINE = {
    "days_cover": ("consumption_cover_days", {
        "crude": "Crude oil and refinery feedstocks (days)", "petrol": "Automotive gasoline (days)",
        "diesel": "Diesel oil (days)", "jet": "Aviation turbine fuel (days)", "lpg": "LPG (days)",
        "total_products_coe": "Total product stocks COE (days)"}),
    "stocks_ml": ("stocks_ml", {
        "crude": "Crude oil and refinery feedstocks (ML)", "petrol": "Automotive gasoline (ML)",
        "diesel": "Diesel oil (ML)", "jet": "Aviation turbine fuel (ML)", "lpg": "LPG (ML)",
        "total_coe": "Total stocks COE (ML)"}),
    "sales_ml": ("sales_ml", {
        "petrol": "Automotive gasoline total (ML)", "diesel": "Diesel oil: total (ML)",
        "jet": "Aviation turbine fuel total (ML)", "lpg": "LPG total (ML)", "total": "Total (ML)"}),
    "imports_ml": ("imports_ml", {
        "crude": "Crude oil & other refinery feedstocks (ML)", "petrol": "Automotive gasoline (ML)",
        "diesel": "Diesel oil (ML)", "jet": "Aviation turbine fuel (ML)",
        "refined_total": "Total refined petroleum products (ML)", "total": "Total oil imports (ML)"}),
    "refinery_ml": ("refinery_ml", {
        "crude_input": "Total input (ML)", "petrol": "Automotive gasoline (ML)", "diesel": "Diesel oil (ML)",
        "jet": "Aviation turbine fuel (ML)", "total_output": "Total (ML)",
        "domestic_crude_share_pct": "Percentage indigenous: Total input (%)"}),
    "iea": ("iea_net_import_cover", {
        "days": "IEA days of net import coverage", "daily_net_imports_kt": "Daily Net Imports (kT/day)"}),
    "iea_incl_on_the_way": ("iea_days_incl_on_the_way", {
        "days_total": "Held for the Australian market in Australia and overseas (days)"}),
}
IMPORT_PRODUCTS = {"crude": "Crude oil & other refinery feedstocks (ML)", "petrol": "Automotive gasoline (ML)",
                   "diesel": "Diesel oil (ML)", "jet": "Aviation turbine fuel (ML)",
                   "total": "Total oil imports (ML)"}


def _num(v):
    if v is None or isinstance(v, str) or pd.isna(v):
        return None  # "n.p." / "n.a." / "-" / blank
    return round(float(v), 3)


def _sheet(xl, name):
    want = re.sub(r"\s+", " ", name).strip().lower()
    for s in xl.sheet_names:
        if re.sub(r"\s+", " ", s).strip().lower() == want:
            return s
    raise KeyError(f"sheet {name!r} not found; have {xl.sheet_names}")


def _month_frame(xl, name, month_col="Month"):
    df = xl.parse(_sheet(xl, name))
    df.columns = [str(c).strip() for c in df.columns]
    if month_col not in df.columns:
        raise ValueError(f"{name}: no {month_col!r} column; header changed? {list(df.columns)[:5]}")
    df[month_col] = pd.to_datetime(df[month_col], errors="coerce")
    return df.dropna(subset=[month_col]).sort_values(month_col)


def _ym(ts):
    return ts.strftime("%Y-%m")


def parse_aps(path, months=13):
    """Parse an APS 'Data extract' xlsx into JSON-able dicts (last `months` months per series)."""
    xl = pd.ExcelFile(path)
    out = {"source": "Australian Petroleum Statistics (DCCEEW), CC BY 4.0", "file": str(path), "series": {}}

    for key, sheet in WIDE.items():
        df = _month_frame(xl, sheet).tail(months)
        cols = [c for c in df.columns if c != "Month"]
        out["series"][key] = {"months": [_ym(m) for m in df["Month"]],
                              "values": {c: [_num(v) for v in df[c]] for c in cols}}
    latest = out["series"]["stocks_ml"]["months"][-1]
    out["data_month"] = latest

    head = {}
    for hk, (sk, cols) in HEADLINE.items():
        s = out["series"][sk]
        if s["months"][-1] != latest:
            raise ValueError(f"{sk} ends {s['months'][-1]}, stocks end {latest}")
        head[hk] = {k: s["values"][c][-1] for k, c in cols.items()}
    out["headline"] = head

    # State sales (QLD etc.) - no state stocks exist in APS.
    st = _month_frame(xl, "Sales by state and territory")
    st["State"] = st["State"].astype(str).str.strip()
    keep = sorted(st["Month"].unique())[-months:]
    st = st[st["Month"].isin(keep)]
    out["state_sales_ml"] = {
        state: {"months": [_ym(m) for m in g["Month"]],
                "values": {c: [_num(v) for v in g[c]] for c in g.columns if c not in ("State", "Month")}}
        for state, g in st.groupby("State")}

    # Imports by source country: latest month + trailing 12 months, non-zero only, largest first.
    ic = _month_frame(xl, "Imports volume by country")
    ic["Source country"] = ic["Source country"].astype(str).str.strip()
    last12 = sorted(ic["Month"].unique())[-12:]

    def by_country(frame):
        g = frame.groupby("Source country")[list(IMPORT_PRODUCTS.values())].sum(numeric_only=True)
        return {k: {c: round(float(v), 1) for c, v in g[col].sort_values(ascending=False).items() if v > 0}
                for k, col in IMPORT_PRODUCTS.items()}

    out["imports_by_country_ml"] = {
        "latest_month": by_country(ic[ic["Month"] == last12[-1]]),
        "trailing_12m": by_country(ic[ic["Month"].isin(last12)]),
        "trailing_12m_range": [_ym(last12[0]), _ym(last12[-1])],
    }
    return out


def latest_aps_url(today=None, session=requests):
    """Newest APS data-extract xlsx URL, scraped from the yearly publication page(s)."""
    today = today or datetime.now()
    best = None
    for year in (today.year, today.year - 1):  # Jan/Feb: newest file is still on last year's page
        r = session.get(APS_PAGE.format(year=year), headers=UA, timeout=30)
        if r.status_code == 404:
            continue
        r.raise_for_status()
        for href in re.findall(r'href="([^"]+\.xlsx)"', r.text, re.I):
            name = unquote(href).lower()
            if "data" not in name or "extract" not in name:
                continue
            m = re.search(r"(" + "|".join(MONTHS) + r")[\s_\-]*(20\d\d)", name)
            if m:
                k = (int(m.group(2)), MONTHS[m.group(1)])
                if best is None or k > best[0]:
                    best = (k, urljoin(r.url, href))
        if best:
            break
    if not best:
        raise RuntimeError("no APS data extract link found")
    return best[1]


def latest_mso_weekly_url(session=requests):
    r = session.get(MSO_PAGE, headers=UA, timeout=30)
    r.raise_for_status()
    hrefs = [h for h in re.findall(r'href="([^"]+\.xlsx)"', r.text, re.I) if "weekly" in h.lower()]
    if not hrefs:
        raise RuntimeError("no MSO weekly xlsx link found")
    return urljoin(r.url, hrefs[0])


def parse_mso_weekly(path):
    """MSO weekly snapshot: sheets Gasoline/Diesel/Kerosene; row0 header, row1 product label, then weekly rows."""
    xl = pd.ExcelFile(path)
    out = {"source": "DCCEEW MSO Weekly Snapshot, CC BY 4.0", "products": {}}
    for prod in ("Gasoline", "Diesel", "Kerosene"):
        df = xl.parse(_sheet(xl, prod), header=0).iloc[1:]
        df.columns = [str(c).strip() for c in df.columns]

        def col(prefix):
            hit = [c for c in df.columns if c.lower().startswith(prefix)]
            return hit[0] if hit else None

        date, req, red = col("obligation date"), col("stocks required under mso (ml)"), col("stocks required under mso after")
        held, days = col("stock held under mso (ml)"), col("stock held under mso (days")
        df[date] = pd.to_datetime(df[date], errors="coerce")
        df = df.dropna(subset=[date]).sort_values(date)
        rows = []
        for _, r in df.iterrows():
            required = _num(r[red]) if red and _num(r[red]) is not None else _num(r[req])
            rows.append({"date": r[date].strftime("%Y-%m-%d"), "held_ml": _num(r[held]), "days": _num(r[days]),
                         "required_ml": _num(r[req]), "effective_required_ml": required,
                         "pct_above_effective": None if not required else round(100 * (_num(r[held]) / required - 1), 1)})
        out["products"][prod.lower()] = rows
    return out


# -- Port of Brisbane: crude imports feed Lytton; refined imports supply the south-east -----------
PBPL_PAGE = 'https://www.portbris.com.au/operations-and-trade/trade-development'
TONNES_PER_ML = 800   # rough: ~0.80 t per kL; petrol ~0.73, diesel ~0.84, crude ~0.80


def latest_pbpl_url(session=requests):
    r = session.get(PBPL_PAGE, headers=UA, timeout=30)
    r.raise_for_status()
    best = None
    for href in set(re.findall(r'href="([^"]*historical-monthly-trade[^"]*)"', r.text, re.I)):
        # Several versions stay linked; the slug ends with the last month covered, e.g. "...-august-2026-xlsx".
        found = re.findall(r"(" + "|".join(MONTHS) + r")-(20\d\d)", href.lower())
        if found:
            key = max((int(y), MONTHS[mon]) for mon, y in found)
            if best is None or key > best[0]:
                best = (key, urljoin(r.url, href.replace('&amp;', '&')))
    if not best:
        raise RuntimeError('no Port of Brisbane monthly trade link found')
    return best[1]


def parse_pbpl(path, months=24):
    """Rows 'Crude Oil' and 'Refined Oil' under the Import block, 'Refined Oil' under Export; tonnes.
    The same label appears in both blocks, so rows are found by label within each direction."""
    sheet = pd.read_excel(path, sheet_name=0, header=None).dropna(how='all').dropna(axis=1, how='all')
    sheet = sheet.reset_index(drop=True)
    header = next(i for i, row in sheet.iterrows() if any(str(v).startswith('Arr ') for v in row))
    columns = [(j, str(v)) for j, v in enumerate(sheet.iloc[header]) if str(v).startswith('Arr ')]
    found, block = {}, None
    for i in range(header + 1, len(sheet)):
        cells = [str(v).strip() for v in sheet.iloc[i, :3] if pd.notna(v)]
        if any(c.startswith('Import') for c in cells):
            block = 'import'
        elif any(c.startswith('Export') for c in cells):
            block = 'export'
        label = next((c for c in cells if c in ('Crude Oil', 'Refined Oil')), None)
        if label and block and (block, label) not in found:
            found[(block, label)] = i
    keep = columns[-months:]

    def month(text):
        return datetime.strptime(text.replace('Arr ', ''), '%Y/%b').strftime('%Y-%m')

    rows = {'crude_import_t': ('import', 'Crude Oil'), 'refined_import_t': ('import', 'Refined Oil'),
            'refined_export_t': ('export', 'Refined Oil')}
    series = {key: [_num(sheet.iloc[found[k], j]) if k in found else None for j, _ in keep] for key, k in rows.items()}
    return {'months': [month(t) for _, t in keep], **series, 'tonnes_per_ml': TONNES_PER_ML,
            'source': 'Port of Brisbane Pty Ltd, historical monthly trade'}


# -- fetching and caching ------------------------------------------------------------------------
class FuelSecurity:
    SOURCES = {'aps': (latest_aps_url, 'aps.xlsx'), 'mso': (latest_mso_weekly_url, 'mso-weekly.xlsx'),
               'pbpl': (latest_pbpl_url, 'pbpl-monthly.xlsx')}
    CHECK_HOURS = 12

    def __init__(self, directory):
        from pathlib import Path
        self.dir = Path(directory)
        self.state_path = self.dir / 'state.json'
        self._parsed = {}

    def refresh(self, now=None, session=requests):
        """Re-download a source when its newest file changed, at most every CHECK_HOURS."""
        from api_evidence import read_json, write_json
        now = now or datetime.now()
        state = read_json(self.state_path, {})
        for name, (find_url, filename) in self.SOURCES.items():
            entry = state.get(name, {})
            checked = entry.get('checked_at')
            if checked and (now - datetime.fromisoformat(checked)).total_seconds() < self.CHECK_HOURS * 3600:
                continue
            entry['checked_at'] = now.isoformat(timespec='seconds')
            try:
                url = find_url(session=session)
                path = self.dir / filename
                # The MSO weekly file keeps one URL and is overwritten each week, so it is always re-fetched.
                if url != entry.get('url') or name == 'mso' or not path.exists():
                    r = session.get(url, headers=UA, timeout=60)
                    r.raise_for_status()
                    self.dir.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(r.content)
                    entry.update(url=url, fetched_at=now.isoformat(timespec='seconds'))
                entry['error'] = None
            except Exception as exc:   # one source failing keeps its last file and does not stop the others
                entry['error'] = f'{type(exc).__name__}: could not refresh; showing the last saved file.'
            state[name] = entry
        write_json(self.state_path, state)

    def _parse(self, name, parser):
        if name not in self.SOURCES:
            return None
        path = self.dir / self.SOURCES[name][1]
        if not path.exists():
            return None
        key = path.stat().st_mtime_ns
        if self._parsed.get(name, (None,))[0] != key:
            try:
                self._parsed[name] = (key, parser(path))
            except Exception as exc:
                self._parsed[name] = (key, {'error': f'{type(exc).__name__}: the file layout may have changed.'})
        return self._parsed[name][1]

    def payload(self):
        from api_evidence import read_json
        state = read_json(self.state_path, {})
        aps, mso, pbpl = self._parse('aps', parse_aps), self._parse('mso', parse_mso_weekly), self._parse('pbpl', parse_pbpl)
        out = {'state': {k: {f: v.get(f) for f in ('fetched_at', 'checked_at', 'error')} for k, v in state.items()},
               'mso': mso, 'pbpl': pbpl, 'aps': None, 'qld': None,
               'notes': {
                   'measures': 'Consumption cover (APS), IEA net-import days and MSO days are different measures and '
                               'are not comparable with each other.',
                   'qld_stocks': 'No public source reports Queensland fuel stocks or days of cover; stocks are national.',
                   'attribution': 'Australian Petroleum Statistics and MSO statistics, Commonwealth of Australia (DCCEEW), '
                                  'CC BY 4.0; Port of Brisbane Pty Ltd trade statistics.'}}
        if aps and 'error' not in aps:
            # Only what the page draws: headline, 13-month series for cover/stocks/IEA, flows and country mix.
            series = aps['series']
            keep = {k: series[k] for k in ('consumption_cover_days', 'stocks_ml', 'iea_net_import_cover',
                                           'iea_days_incl_on_the_way', 'imports_ml', 'refinery_ml', 'sales_ml') if k in series}
            out['aps'] = {'data_month': aps['data_month'], 'headline': aps['headline'], 'series': keep,
                          'imports_by_country_ml': aps['imports_by_country_ml']}
            qld, national = aps['state_sales_ml'].get('QLD'), series.get('sales_ml')
            if qld and national:
                q, n = qld['values'], national['values']
                petrol, diesel = q.get('Automotive gasoline: total (ML)'), q.get('Diesel oil: total')
                all_petrol, all_diesel = n.get('Automotive gasoline total (ML)'), n.get('Diesel oil: total (ML)')

                def share(part, whole):
                    return round(100 * part[-1] / whole[-1], 1) if part and whole and part[-1] and whole[-1] else None

                out['qld'] = {'months': qld['months'], 'petrol_ml': petrol, 'diesel_ml': diesel,
                              'petrol_share_pct': share(petrol, all_petrol), 'diesel_share_pct': share(diesel, all_diesel)}
        elif aps:
            out['aps'] = aps
        return out
