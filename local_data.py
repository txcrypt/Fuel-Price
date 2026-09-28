"""Collection, durable storage and health reporting for the local dashboard."""
import csv
import json
import logging
import os
import threading
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import config
from data_store import FuelDataStore
from fuel_engine import FetchError, FuelEngine
from forecasting import build_forecast, MODELS
from qld_history_import import reported_series
from api_evidence import ApiEvidence
from context_sources import ContextSources

log = logging.getLogger(__name__)
CSV_COLUMNS = ['site_id', 'price_cpl', 'reported_at', 'region', 'state',
               'latitude', 'longitude', 'scraped_at']


def atomic_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2), encoding='utf-8')
    os.replace(temporary, path)


def within(frame, bounds=None):
    """Valid QLD rows inside *bounds*, defaulting to the whole collection area."""
    if frame.empty:
        return frame
    frame = frame.rename(columns={'lat': 'latitude', 'lng': 'longitude'}).copy()
    if 'state' in frame:
        frame = frame.loc[frame.state.eq('QLD')]
    b = bounds or config.BOUNDS
    for column in ['latitude', 'longitude', 'price_cpl']:
        frame[column] = pd.to_numeric(frame[column], errors='coerce')
    return frame.loc[frame.latitude.between(b['lat_min'], b['lat_max']) &
                     frame.longitude.between(b['lng_min'], b['lng_max']) &
                     frame.price_cpl.between(80, 350)].copy()


# A price this far under the statewide median sits below wholesale cost: an entry error, not a bargain
# (2026-09-28: one roadhouse at 99.9 c/L against a 239 median). Remote prices far ABOVE it are real.
SUSPECT_BELOW_MEDIAN_CPL = 60
STALE_REPORT_DAYS = 30


def price_flag(price, reported, median, now):
    """'stale' (last report over a month old), 'suspect' (implausibly low), or None.

    Age is checked first: an old low price was a real price when reported, so 'stale' is the truer label.
    """
    if pd.notna(reported) and now - reported > pd.Timedelta(days=STALE_REPORT_DAYS):
        return 'stale'
    if price is not None and median is not None and price < median - SUSPECT_BELOW_MEDIAN_CPL:
        return 'suspect'
    return None


# Early warning of a citywide price-cycle restoration, observed not forecast. Backtest over the
# 10 restorations in the official 2025-01..2026-08 reports (docs/cycle-signal-evaluation-2026-09-28.md):
# the restored group spreads over 5-14 days before the median jumps. A share >= 10% gave a median
# 7 days' warning; >= 37% at end of day flagged 8 of 9 starts the evening before. Known false alarm:
# cost-driven climbs (July 2026 reached 18.5% with no restoration).
RESTORED_ABOVE_MEDIAN_CPL = 20
RESTORED_EXPIRY_DAYS = 14      # Same as the backtest's as-of expiry, so thresholds carry over.


def restoration_signal(focus, now=None):
    """Share of recently reporting stations priced >= 20 c/L above the median, or None."""
    if focus.empty:
        return None
    now = now or pd.Timestamp.now(tz='UTC')
    reported = pd.to_datetime(focus.reported_at, format='mixed', utc=True, errors='coerce')
    prices = focus.loc[reported >= now - pd.Timedelta(days=RESTORED_EXPIRY_DAYS), 'price_cpl']
    if len(prices) < 50:
        return None
    share = float(prices.ge(prices.median() + RESTORED_ABOVE_MEDIAN_CPL).mean())
    level = 'imminent' if share >= .37 else 'building' if share >= .10 else 'quiet'
    return {'restored_share': round(share, 3), 'stations': int(len(prices)), 'level': level,
            'message': {'imminent': 'A citywide price jump is likely within about a day.',
                        'building': 'A citywide price jump is building; historically it followed within about a week.',
                        'quiet': 'No sign of a citywide price jump building.'}[level],
            'basis': 'Share of stations already priced 20 c/L or more above the median. In the 2025-2026 backtest it '
                     'warned of every cycle restoration; cost-driven climbs can raise it without a jump.'}


# Collection widened to all of Queensland on 2026-09-28; the old name said Brisbane
# while returning Cairns. Kept as an alias so nothing importing it breaks.
brisbane = within


class LocalData:
    def __init__(self, root=None, engine=None):
        self.root = Path(root or config.BASE_DIR)
        self.collection = self.root / 'brisbane_fuel_live_collection.csv'
        self.snapshot = self.root / 'live_snapshot.csv'
        self.status_path = self.root / 'collector_status.json'
        self.db = FuelDataStore(str(self.root / 'fuel_data.db'))
        self.engine = engine or FuelEngine()
        self.api_evidence = ApiEvidence(self.root, self.db)
        self.context = ContextSources(self.root, self.db)
        self.lock = threading.Lock()
        self.data_lock = threading.RLock()
        self.status = {'api': 'not_checked', 'csv': 'not_checked', 'database': 'not_checked',
                       'snapshot': 'not_checked', 'last_attempt': None, 'last_success': None,
                       'last_csv_write': None, 'last_csv_rows': 0, 'next_refresh': None, 'error': None,
                       'last_database_write': None, 'last_snapshot_write': None, 'last_api_evidence_write': None,
                       'consecutive_failures': 0, 'failure_category': None, 'outage_started': None, 'outages': []}
        if self.status_path.exists():
            try:
                self.status.update(json.loads(self.status_path.read_text(encoding='utf-8')))
            except (ValueError, OSError):
                log.warning('Previous collector status could not be read.')
        self._record_startup_gap()
        # Persisted success is history, not proof that this process has checked the API.
        self.status.update(api='not_checked', csv='not_checked', database='not_checked',
                           snapshot='not_checked', next_refresh=None, error=None)
        self.cached = None
        self.source_key = None
        with self.db._connect() as connection:
            connection.execute('CREATE INDEX IF NOT EXISTS snapshots_state_time ON snapshots(state, scraped_at)')
            connection.execute('''CREATE TABLE IF NOT EXISTS local_forecasts (
                issued_date TEXT, target_date TEXT, model TEXT, price REAL,
                PRIMARY KEY(issued_date, target_date))''')
            # Forward test: every candidate's path, every day, scored later on collected days only.
            connection.execute('''CREATE TABLE IF NOT EXISTS forward_predictions (
                issued_date TEXT, target_date TEXT, model TEXT, price REAL,
                PRIMARY KEY(issued_date, target_date, model))''')

    def _record_startup_gap(self):
        """A collection window missed while asleep/stopped is recorded, never back-filled."""
        due = self.status.get('next_refresh') or self.status.get('last_success')
        started = self.status.get('outage_started') or self.status.get('last_success') or due
        if not due:
            return
        now = datetime.now()
        try:
            gap = (now - datetime.fromisoformat(due)).total_seconds()
            began = datetime.fromisoformat(started)
        except (TypeError, ValueError):
            return
        if gap <= 1.5 * config.REFRESH_SECONDS:
            return
        outages = list(self.status.get('outages') or [])
        outages.append({'started': began.isoformat(timespec='seconds'),
                        'ended': now.isoformat(timespec='seconds'),
                        'duration_minutes': round((now - began).total_seconds() / 60, 1),
                        'category': self.status.get('failure_category') or 'not_running'})
        self.status['outages'] = outages[-20:]

    def _schedule_next(self, succeeded):
        now = datetime.now()
        if succeeded:
            self.status.update(consecutive_failures=0, failure_category=None, outage_started=None)
            delay = config.REFRESH_SECONDS
        else:
            count = (self.status.get('consecutive_failures') or 0) + 1
            self.status['consecutive_failures'] = count
            if not self.status.get('outage_started'):
                self.status['outage_started'] = (self.status.get('last_success')
                                                 or now.isoformat(timespec='seconds'))
            # Floor of 60s: the upstream price cache has a 60s TTL, so a faster retry only re-serves it.
            delay = max(60, min(config.REFRESH_SECONDS, 60 * 2 ** (count - 1)))
        self.status['next_refresh'] = (now + timedelta(seconds=delay)).isoformat(timespec='seconds')

    def _csv_info(self):
        if not self.collection.exists():
            return {'rows': 0, 'bytes': 0, 'last_record': None}
        with self.collection.open(newline='', encoding='utf-8-sig') as source:
            reader = csv.DictReader(source)
            count, last = 0, None
            for row in reader:
                count += 1
                if row.get('state') == 'QLD':
                    last = row.get('scraped_at')
        return {'rows': count, 'bytes': self.collection.stat().st_size, 'last_record': last}

    def _append(self, frame):
        info = self._csv_info()
        last = pd.to_datetime(info['last_record'], errors='coerce')
        if pd.notna(last) and (datetime.now() - last.to_pydatetime()).total_seconds() < config.CSV_INTERVAL_SECONDS:
            self.status.update(csv='up_to_date', last_csv_write=info['last_record'])
            return
        exists = self.collection.exists() and self.collection.stat().st_size > 0
        if exists:
            with self.collection.open(newline='', encoding='utf-8-sig') as source:
                if next(csv.reader(source), []) != CSV_COLUMNS:
                    raise ValueError('History CSV header differs from the expected schema; append stopped to protect existing data.')
        # Flush the append before reporting success. A process restart respects the on-disk timestamp.
        with self.collection.open('a', newline='', encoding='utf-8') as target:
            frame[CSV_COLUMNS].to_csv(target, index=False, header=not exists)
            target.flush()
            os.fsync(target.fileno())
        self.status.update(csv='recording', last_csv_write=str(frame.scraped_at.max()), last_csv_rows=len(frame))

    def refresh(self):
        if not self.lock.acquire(blocking=False):
            return False
        succeeded = False
        try:
            self.status.update(last_attempt=datetime.now().isoformat(timespec='seconds'),
                               api='checking', error=None, csv='not_checked', database='not_checked', snapshot='not_checked', api_evidence='not_checked')
            try:
                frame = self.engine.get_market_snapshot()
                if frame is None or frame.empty:
                    raise FetchError('API returned no usable prices.', 'empty')
                self.status.update(api='connected', last_api_success=str(frame.scraped_at.max()), api_rows=len(frame))
            except Exception as exc:
                self.status.update(api='error', error=str(exc),
                                   failure_category=getattr(exc, 'category', 'unknown'))
                log.warning('Collection failed: %s', exc)
                return False
            with self.data_lock:
                failures = []
                written = datetime.now().isoformat(timespec='seconds')
                for stage in ['database', 'snapshot', 'csv', 'api_evidence']:
                    try:
                        if stage == 'database':
                            inserted = self.db.save_snapshot(frame)
                            self.db.save_daily_stats(datetime.now().date(), 'QLD', frame.price_cpl)
                            self.status.update(database='recording', database_rows_inserted=inserted,
                                               last_database_write=written)
                        elif stage == 'snapshot':
                            temporary = self.snapshot.with_suffix('.tmp')
                            frame.to_csv(temporary, index=False)
                            os.replace(temporary, self.snapshot)
                            self.status.update(snapshot='recording', last_snapshot_write=written)
                        elif stage == 'csv':
                            self._append(frame)
                        elif getattr(self.engine, 'last_evidence', None):
                            self.api_evidence.save(self.engine.last_evidence)
                            self.status.update(api_evidence='recording', last_api_evidence_write=written)
                    except Exception as exc:
                        self.status[stage] = 'error'
                        failures.append(f'{stage}: {exc}')
                        log.exception('Write failed: %s', stage)
                self.status['error'] = '; '.join(failures) or None
                if failures:
                    self.status['failure_category'] = 'write'
                else:
                    self.status['last_success'] = datetime.now().isoformat(timespec='seconds')
                succeeded = not failures
                return succeeded
        finally:
            self._schedule_next(succeeded)
            try:
                atomic_json(self.status_path, self.status)
            except OSError:
                log.exception('Unable to persist collector status')
            with self.data_lock:
                self.cached = None
            self.lock.release()

    def _history(self):
        frames = []
        if self.collection.exists():
            frames.append(pd.read_csv(self.collection, low_memory=False))
        with self.db._connect() as connection:
            frames.append(pd.read_sql_query('''SELECT site_id, price_cpl, scraped_at, lat AS latitude,
                lng AS longitude, state FROM snapshots WHERE state = 'QLD' ''', connection))
        frame = within(pd.concat(frames, ignore_index=True), config.ANALYSIS_BOUNDS)
        frame['day'] = pd.to_datetime(frame.scraped_at, format='mixed', errors='coerce').dt.normalize()
        frame = frame.dropna(subset=['day'])
        frame['site_id'] = frame.site_id.astype(str)
        # Each station has equal weight, regardless of how many times it was scraped.
        frame = frame.sort_values('scraped_at').drop_duplicates(['day', 'site_id'], keep='last')
        daily = frame.groupby('day').agg(price_cpl=('price_cpl', 'median'), stations=('site_id', 'nunique')).reset_index()
        return daily

    def _reported(self):
        """The official backfill, if any was imported. Empty until it is, and never
        an observation this app collected — see forecasting.merge_history."""
        try:
            return reported_series(self.db)
        except Exception:
            log.exception('Reported backfill could not be read; forecasting on collected days only.')
            return None

    def _latest(self):
        candidates = [within(self.db.get_latest_snapshot('QLD'))]
        if self.snapshot.exists():
            candidates.append(within(pd.read_csv(self.snapshot, low_memory=False)))
        available = [df for df in candidates if not df.empty]
        if not available:
            return pd.DataFrame()
        return max(available, key=lambda df: str(df.scraped_at.max())).sort_values('price_cpl').drop_duplicates('site_id')

    def _prediction_log(self, forecast, daily):
        today = datetime.now().strftime('%Y-%m-%d')
        with self.db._connect() as connection:
            if forecast.get('as_of') == today:
                for point in forecast['points']:
                    connection.execute('INSERT OR IGNORE INTO local_forecasts VALUES (?, ?, ?, ?)',
                                       (today, point['date'], forecast['model_id'], point['price']))
                for model, path in (forecast.get('paths') or {}).items():
                    for point, price in zip(forecast['points'], path):
                        connection.execute('INSERT OR IGNORE INTO forward_predictions VALUES (?, ?, ?, ?)',
                                           (today, point['date'], model, price))
            rows = connection.execute('SELECT target_date, price FROM local_forecasts WHERE target_date < ?', (today,)).fetchall()
            total = connection.execute('SELECT COUNT(*) FROM local_forecasts').fetchone()[0]
            forward_rows = connection.execute('SELECT model, target_date, price FROM forward_predictions WHERE target_date < ?',
                                              (today,)).fetchall()
            first = connection.execute('SELECT MIN(issued_date) FROM forward_predictions').fetchone()[0]
        # Scored only against days this app collected: the retrospective backfill never grades a forward prediction.
        actuals = {row.day.strftime('%Y-%m-%d'): row.price_cpl for row in daily.itertuples()}
        errors = [abs(price - actuals[date]) for date, price in rows if date in actuals]
        by_model = {}
        for model, date, price in forward_rows:
            if date in actuals:
                by_model.setdefault(model, []).append(abs(price - actuals[date]))
        forward = [{'model': model, 'name': MODELS.get(model, model), 'scored': len(e), 'mae': round(sum(e) / len(e), 2)}
                   for model, e in sorted(by_model.items(), key=lambda item: sum(item[1]) / len(item[1]))]
        return {'saved': total, 'scored': len(errors), 'mae': round(sum(errors) / len(errors), 2) if errors else None,
                'forward': forward, 'forward_since': first}

    def dashboard(self):
        with self.data_lock:
            source_key = tuple((p.stat().st_mtime_ns, p.stat().st_size) if p.exists() else None
                               for p in [self.collection, self.snapshot])
            if source_key != self.source_key:
                self.cached = None
                self.source_key = source_key
            if self.cached is None:
                daily = self._history()
                live = self._latest()
                forecast = build_forecast(daily, reported=self._reported())
                prediction_log = self._prediction_log(forecast, daily)
                with self.db._connect() as connection:
                    db_rows = connection.execute("SELECT COUNT(*) FROM snapshots WHERE state='QLD'").fetchone()[0]
                    db_last = connection.execute("SELECT MAX(scraped_at) FROM snapshots WHERE state='QLD'").fetchone()[0]
                latest = str(live.scraped_at.max()) if not live.empty else None
                # The headline figures track the analysis area so they agree with the
                # daily history and the forecast; the statewide block is reported beside
                # them rather than silently replacing them.
                focus = within(live, config.ANALYSIS_BOUNDS)
                stations = []
                for row in live.to_dict('records'):
                    stations.append({key: row.get(key) for key in
                                     ['site_id', 'name', 'price_cpl', 'region', 'reported_at', 'latitude', 'longitude']})
                # Replace pandas NaN with JSON null.
                stations = json.loads(pd.DataFrame(stations).to_json(orient='records'))
                statewide_median = float(live.price_cpl.median()) if not live.empty else None
                now = pd.Timestamp.now(tz='UTC')
                for station in stations:
                    # TransactionDateUtc is UTC even when the API omits a Z suffix.
                    reported = pd.to_datetime(station.get('reported_at'), format='mixed', utc=True, errors='coerce')
                    station['reported_at'] = reported.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if pd.notna(reported) else None
                    station['flag'] = price_flag(station.get('price_cpl'), reported, statewide_median, now)
                historical = [{'date': row.day.strftime('%Y-%m-%d'), 'price': round(row.price_cpl, 1),
                               'stations': row.stations} for row in daily.itertuples()]
                gap_days = int((daily.day.max() - daily.day.min()).days + 1 - len(daily)) if len(daily) else 0
                self.cached = {'latest': latest, 'stations': stations, 'forecast': forecast,
                               'history': historical, 'prediction_log': prediction_log,
                               'summary': {'median': round(float(focus.price_cpl.median()), 1) if not focus.empty else None,
                                           'minimum': round(float(focus.price_cpl.min()), 1) if not focus.empty else None,
                                           'maximum': round(float(focus.price_cpl.max()), 1) if not focus.empty else None,
                                           'station_count': len(focus), 'area': config.ANALYSIS_AREA_NAME,
                                           # The station holding the minimum above, so the name always matches the price.
                                           'cheapest_name': (str(focus.loc[focus.price_cpl.idxmin()].get('name') or '')
                                                             if not focus.empty else None)},
                               'restoration': restoration_signal(focus),
                               'grades': self.api_evidence.grades(config.ANALYSIS_BOUNDS),
                               'statewide': {'median': round(float(live.price_cpl.median()), 1) if not live.empty else None,
                                             'minimum': round(float(live.price_cpl.min()), 1) if not live.empty else None,
                                             'maximum': round(float(live.price_cpl.max()), 1) if not live.empty else None,
                                             'station_count': len(live), 'area': 'Queensland',
                                             'regions': {str(k): int(v) for k, v in live.region.value_counts().items()} if 'region' in live else {},
                                             'note': 'Collected statewide. The daily history, headline median and forecast cover '
                                                     f'{config.ANALYSIS_AREA_NAME} only, so the two medians are not the same series.'},
                               'files': {'csv': self._csv_info(), 'database_rows': db_rows, 'database_last': db_last},
                               'coverage': {'days': len(daily), 'missing_days': gap_days,
                                            'start': historical[0]['date'] if historical else None,
                                            'end': historical[-1]['date'] if historical else None}}
            payload = dict(self.cached)
        latest = payload['latest']
        age = (datetime.now() - pd.Timestamp(latest).to_pydatetime()).total_seconds() / 60 if latest else None
        payload['health'] = {**dict(self.status), 'collecting': self.lock.locked(),
                             'snapshot_age_minutes': round(age, 1) if age is not None else None,
                             'fresh': age is not None and 0 <= age < 90,
                             'refresh_seconds': config.REFRESH_SECONDS, 'csv_interval_seconds': config.CSV_INTERVAL_SECONDS,
                             'token_configured': bool(config.FUEL_API_TOKEN)}
        payload['generated_at'] = datetime.now().isoformat(timespec='seconds')
        return payload
