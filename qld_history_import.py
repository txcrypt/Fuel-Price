"""Manual importer for the official Queensland monthly fuel-price history.

Run by hand only — nothing in the server imports this module's ``main``:

    .venv\\Scripts\\python.exe -m qld_history_import --years 2025 2026

Source: https://www.data.qld.gov.au/dataset/fuel-price-reporting-<year> (CC BY 4.0,
(c) State of Queensland, Department of Energy and Climate).  Each monthly CSV is a
log of *reported price change events*, not daily snapshots, and is published about
a month after the events it contains.  Everything reconstructed from it is
therefore labelled RETROSPECTIVE: this app did not collect it, and no claim may
imply it was available here at the time.

That label is about provenance, not about timing.  Each row carries the retailer's
genuine report timestamp, and those prices were publicly known then — the same
events flow through the live subscriber API.  So a backtest is legitimate provided
it uses only events reported on or before each origin, which is what
``forecasting.build_forecast`` does; see `docs/forecast-evaluation-2026-09-18.md`.
What is forbidden is relabelling these days as locally collected observations, or
letting a later monthly file alter an earlier prediction.

Reported events are stored in ``reported_price_events`` and never in
``snapshots``, which remains the record of what this app actually collected.
"""
import argparse
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

import config
from data_store import FuelDataStore

log = logging.getLogger(__name__)

CKAN_ACTION = 'https://www.data.qld.gov.au/api/3/action/package_show'
DATASET = 'fuel-price-reporting-{year}'
ATTRIBUTION = ('Queensland fuel price reporting, (c) State of Queensland, '
               'CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/)')
DOWNLOAD_DIR = config.BASE_DIR / '.local' / 'qld-history'

FUEL_TYPE = 'Unleaded'          # U91 as spelled in the published files.
RAW_UNAVAILABLE = 9999          # Publisher's "no price available" sentinel.
UNAVAILABLE_CPL = 0.0           # How that sentinel is stored (see data_store.py).
PRICE_MIN, PRICE_MAX = 80.0, 350.0
# A station's last report is carried forward at most this many days.  Beyond it the
# station is dropped from the day entirely rather than quoted at a stale price.
AS_OF_EXPIRY_DAYS = 14
# The publisher writes UTC in DD/MM/YYYY.  Never parse this with format='mixed',
# which silently reads 04/08 as 4 August or 8 April depending on the file.
TIME_FORMATS = ('%d/%m/%Y %H:%M', '%d/%m/%Y %H:%M:%S')
COLUMNS = {'siteid', 'site_state', 'site_latitude', 'site_longitude',
           'fuel_type', 'price', 'transactiondateutc'}


def normalise_column(name):
    """The publisher alternates between 'Site_State' and 'Site State' between months."""
    return str(name).strip().lower().replace(' ', '_')


# ---------------------------------------------------------------- acquisition
def resources(year, timeout=60):
    """CSV resources published for one year, newest publication last."""
    response = requests.get(CKAN_ACTION, params={'id': DATASET.format(year=year)}, timeout=timeout)
    response.raise_for_status()
    found = response.json()['result']['resources']
    return [r for r in found if (r.get('format') or '').upper() == 'CSV']


def download(url, destination, timeout=600):
    """Stream to disk, hashing as we go.  Returns (sha256, bytes)."""
    digest, size = hashlib.sha256(), 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        with destination.open('wb') as handle:
            for chunk in response.iter_content(1 << 20):
                digest.update(chunk)
                size += len(chunk)
                handle.write(chunk)
    return digest.hexdigest(), size


# ------------------------------------------------------------------- parsing
def parse_reported_at(values):
    """DD/MM/YYYY HH:MM[:SS] in UTC, explicitly — no mixed-format guessing."""
    parsed = pd.to_datetime(values, format=TIME_FORMATS[0], utc=True, errors='coerce')
    missing = parsed.isna() & values.notna()
    if missing.any():
        parsed.loc[missing] = pd.to_datetime(
            values[missing], format=TIME_FORMATS[1], utc=True, errors='coerce')
    return parsed


def read_events(path, fuel_type=FUEL_TYPE, chunksize=200_000):
    """Read one monthly CSV into (events DataFrame, counts dict).

    Kept rows are Brisbane-bounds QLD reports of *fuel_type* with either a sane
    price or the publisher's explicit unavailability sentinel.
    """
    bounds, frames = config.BOUNDS, []
    counts = {'rows_read': 0, 'rows_kept': 0, 'rows_unavailable': 0}
    # dtype=str keeps SiteId exactly as published; numbers are converted below.
    # The context manager matters on Windows: an open handle blocks the caller's unlink.
    with pd.read_csv(path, chunksize=chunksize, dtype=str,
                     usecols=lambda c: normalise_column(c) in COLUMNS) as reader:
        for chunk in reader:
            counts['rows_read'] += len(chunk)
            chunk.columns = [normalise_column(c) for c in chunk.columns]
            missing = COLUMNS - set(chunk.columns)
            if missing:
                raise ValueError(f'{path.name} is missing expected columns: {sorted(missing)}')
            chunk = chunk.loc[chunk.site_state.astype(str).str.strip().eq('QLD')
                              & chunk.fuel_type.astype(str).str.strip().eq(fuel_type)]
            if chunk.empty:
                continue
            chunk = chunk.assign(
                latitude=pd.to_numeric(chunk.site_latitude, errors='coerce'),
                longitude=pd.to_numeric(chunk.site_longitude, errors='coerce'),
                raw_price=pd.to_numeric(chunk.price, errors='coerce'),
                reported=parse_reported_at(chunk.transactiondateutc))
            chunk = chunk.loc[chunk.latitude.between(bounds['lat_min'], bounds['lat_max'])
                              & chunk.longitude.between(bounds['lng_min'], bounds['lng_max'])
                              & chunk.reported.notna()]
            unavailable = chunk.raw_price.eq(RAW_UNAVAILABLE)
            cents = chunk.raw_price / 10.0          # Published prices are tenths of a cent.
            keep = unavailable | cents.between(PRICE_MIN, PRICE_MAX)
            counts['rows_unavailable'] += int(unavailable.sum())
            chunk = chunk.loc[keep]
            if chunk.empty:
                continue
            frames.append(pd.DataFrame({
                'site_id': chunk.siteid.astype(str).str.strip(),
                'fuel_type': fuel_type,
                'reported_at_utc': chunk.reported.dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'price_cpl': cents.loc[chunk.index].where(~unavailable.loc[chunk.index], UNAVAILABLE_CPL)}))
    events = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=['site_id', 'fuel_type', 'reported_at_utc', 'price_cpl'])
    counts['rows_kept'] = len(events)
    return events, counts


def import_file(store, path, source_id, url='', dataset='', resource_id='',
                sha256='', published_at=None, size=None, fuel_type=FUEL_TYPE):
    """Import one already-downloaded CSV and record its provenance."""
    events, counts = read_events(path, fuel_type)
    inserted = store.save_reported_events(events, source_id)
    store.save_import_source(
        source_id, dataset=dataset, resource_id=resource_id, url=url, sha256=sha256,
        published_at=published_at, imported_at=datetime.now(timezone.utc).isoformat(timespec='seconds'),
        bytes=size if size is not None else path.stat().st_size, **counts)
    return {'source_id': source_id, 'inserted': inserted, **counts}


def import_resource(store, resource, dataset, download_dir=DOWNLOAD_DIR, fuel_type=FUEL_TYPE):
    """Download, hash, import, then delete the raw file — only evidence is kept."""
    source_id = resource.get('id') or resource['url']
    path = download_dir / f'{source_id}.csv'
    sha256, size = download(resource['url'], path)
    try:
        result = import_file(
            store, path, source_id, url=resource['url'], dataset=dataset,
            resource_id=resource.get('id', ''), sha256=sha256, size=size,
            published_at=resource.get('last_modified') or resource.get('created'),
            fuel_type=fuel_type)
    finally:
        path.unlink(missing_ok=True)   # 13 MB each; the checksum is the evidence.
    return {'name': resource.get('name', ''), 'sha256': sha256, 'bytes': size, **result}


# ----------------------------------------------------------- reconstruction
def _daily_last_reports(events):
    """Last report per station per Brisbane day, wide (days x stations)."""
    local = pd.to_datetime(events.reported_at_utc, format='%Y-%m-%dT%H:%M:%SZ', utc=True)
    frame = events.assign(day=local.dt.tz_convert('Australia/Brisbane').dt.strftime('%Y-%m-%d'))
    last = frame.sort_values('reported_at_utc').drop_duplicates(['day', 'site_id'], keep='last')
    wide = last.pivot(index='day', columns='site_id', values='price_cpl')
    wide.index = pd.to_datetime(wide.index)
    return frame, wide.reindex(pd.date_range(wide.index.min(), wide.index.max(), freq='D'))


def reported_series(store, fuel_type=FUEL_TYPE, expiry_days=AS_OF_EXPIRY_DAYS):
    """Daily median of the reconstructed reported series.

    As-of policy: a station's last report holds until its next one and expires
    ``expiry_days`` days after it (day+N is still carried, day+N+1 is not); an
    expired station leaves the day's median rather than being quoted stale.  An
    explicit unavailability report replaces the held price and expires the same way.

    ``retrospective`` is always True: the monthly files publish about a month
    after the events they contain, so no day here was knowable on that day.
    """
    events = store.get_reported_events(fuel_type)
    meta = {'origin': 'reported', 'retrospective': True, 'fuel_type': fuel_type,
            'expiry_days': expiry_days, 'attribution': ATTRIBUTION}
    if events.empty:
        return {**meta, 'days': []}
    _, wide = _daily_last_reports(events)
    # Unavailability rides through the fill as the 0 sentinel, then is masked out.
    held = wide.ffill(limit=expiry_days).mask(lambda f: f.eq(UNAVAILABLE_CPL))
    days = [{'date': day.strftime('%Y-%m-%d'), 'price': round(float(row.median()), 2),
             'stations': int(row.notna().sum())}
            for day, row in held.iterrows() if row.notna().any()]
    return {**meta, 'days': days}


def coverage(store, fuel_type=FUEL_TYPE, expiry_days=AS_OF_EXPIRY_DAYS):
    """Per-day coverage reconciled against the raw imported event rows."""
    events = store.get_reported_events(fuel_type)
    sources = store.get_import_sources()
    summary = {'origin': 'reported', 'retrospective': True, 'fuel_type': fuel_type,
               'expiry_days': expiry_days, 'attribution': ATTRIBUTION,
               'sources': [] if sources.empty else sources.to_dict('records'),
               'raw_events': len(events), 'days': [], 'reconciled': True}
    if events.empty:
        return summary
    frame, wide = _daily_last_reports(events)
    held = wide.ffill(limit=expiry_days).mask(lambda f: f.eq(UNAVAILABLE_CPL))
    per_day = frame.groupby('day').agg(raw_events=('site_id', 'size'),
                                       stations_reporting=('site_id', 'nunique'))
    unavailable = frame.loc[frame.price_cpl.eq(UNAVAILABLE_CPL)].groupby('day').size()
    priced = wide.notna() & wide.ne(UNAVAILABLE_CPL)
    days = []
    for day, row in held.iterrows():
        key = day.strftime('%Y-%m-%d')
        counted = int(row.notna().sum())
        days.append({'date': key,
                     'raw_events': int(per_day.raw_events.get(key, 0)),
                     'stations_reporting': int(per_day.stations_reporting.get(key, 0)),
                     'stations_priced': counted,
                     'stations_carried_forward': counted - int(priced.loc[day].sum()),
                     'unavailable': int(unavailable.get(key, 0))})
    summary['days'] = days
    summary['counted_events'] = sum(d['raw_events'] for d in days)
    summary['reconciled'] = summary['counted_events'] == len(events)
    return summary


# ------------------------------------------------------------------- manual
def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--years', nargs='+', required=True)
    parser.add_argument('--db', default=str(config.DB_FILE), help='SQLite file to import into.')
    parser.add_argument('--match', default='', help='Only resources whose name contains this.')
    parser.add_argument('--limit', type=int, default=0, help='Stop after N resources per year.')
    parser.add_argument('--list', action='store_true', help='List resources; import nothing.')
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    store = FuelDataStore(args.db)
    for year in args.years:
        found = [r for r in resources(year) if args.match.lower() in (r.get('name') or '').lower()]
        if args.limit:
            found = found[:args.limit]
        for resource in found:
            if args.list:
                log.info('%s  %s', resource.get('name'), resource['url'])
                continue
            result = import_resource(store, resource, DATASET.format(year=year))
            log.info('%(name)s read=%(rows_read)d kept=%(rows_kept)d '
                     'unavailable=%(rows_unavailable)d new=%(inserted)d sha256=%(sha256)s', result)
    log.info('Reported series is retrospective. %s', ATTRIBUTION)


if __name__ == '__main__':
    main()
