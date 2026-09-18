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
from fuel_engine import FuelEngine
from forecasting import build_forecast
from api_evidence import ApiEvidence
from context_sources import ContextSources

log = logging.getLogger(__name__)
CSV_COLUMNS = ['site_id', 'price_cpl', 'reported_at', 'region', 'state',
               'latitude', 'longitude', 'scraped_at']


def atomic_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2), encoding='utf-8')
    os.replace(temporary, path)


def brisbane(frame):
    if frame.empty:
        return frame
    frame = frame.rename(columns={'lat': 'latitude', 'lng': 'longitude'}).copy()
    if 'state' in frame:
        frame = frame.loc[frame.state.eq('QLD')]
    b = config.BOUNDS
    for column in ['latitude', 'longitude', 'price_cpl']:
        frame[column] = pd.to_numeric(frame[column], errors='coerce')
    return frame.loc[frame.latitude.between(b['lat_min'], b['lat_max']) &
                     frame.longitude.between(b['lng_min'], b['lng_max']) &
                     frame.price_cpl.between(80, 350)].copy()


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
                       'last_csv_write': None, 'last_csv_rows': 0, 'next_refresh': None, 'error': None}
        if self.status_path.exists():
            try:
                self.status.update(json.loads(self.status_path.read_text(encoding='utf-8')))
            except (ValueError, OSError):
                log.warning('Previous collector status could not be read.')
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
        try:
            self.status.update(last_attempt=datetime.now().isoformat(timespec='seconds'),
                               api='checking', error=None, csv='not_checked', database='not_checked', snapshot='not_checked', api_evidence='not_checked')
            try:
                frame = self.engine.get_market_snapshot()
                if frame is None or frame.empty:
                    raise ValueError('API returned no usable prices.')
                self.status.update(api='connected', last_api_success=str(frame.scraped_at.max()), api_rows=len(frame))
            except Exception as exc:
                self.status.update(api='error', error=str(exc))
                log.warning('Collection failed: %s', exc)
                return False
            with self.data_lock:
                failures = []
                for stage in ['database', 'snapshot', 'csv', 'api_evidence']:
                    try:
                        if stage == 'database':
                            inserted = self.db.save_snapshot(frame)
                            self.db.save_daily_stats(datetime.now().date(), 'QLD', frame.price_cpl)
                            self.status.update(database='recording', database_rows_inserted=inserted)
                        elif stage == 'snapshot':
                            temporary = self.snapshot.with_suffix('.tmp')
                            frame.to_csv(temporary, index=False)
                            os.replace(temporary, self.snapshot)
                            self.status['snapshot'] = 'recording'
                        elif stage == 'csv':
                            self._append(frame)
                        elif getattr(self.engine, 'last_evidence', None):
                            self.api_evidence.save(self.engine.last_evidence)
                            self.status['api_evidence'] = 'recording'
                    except Exception as exc:
                        self.status[stage] = 'error'
                        failures.append(f'{stage}: {exc}')
                        log.exception('Write failed: %s', stage)
                self.status['error'] = '; '.join(failures) or None
                if not failures:
                    self.status['last_success'] = datetime.now().isoformat(timespec='seconds')
                return not failures
        finally:
            self.status['next_refresh'] = (datetime.now() + timedelta(seconds=config.REFRESH_SECONDS)).isoformat(timespec='seconds')
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
        frame = brisbane(pd.concat(frames, ignore_index=True))
        frame['day'] = pd.to_datetime(frame.scraped_at, format='mixed', errors='coerce').dt.normalize()
        frame = frame.dropna(subset=['day'])
        frame['site_id'] = frame.site_id.astype(str)
        # Each station has equal weight, regardless of how many times it was scraped.
        frame = frame.sort_values('scraped_at').drop_duplicates(['day', 'site_id'], keep='last')
        daily = frame.groupby('day').agg(price_cpl=('price_cpl', 'median'), stations=('site_id', 'nunique')).reset_index()
        return daily

    def _latest(self):
        candidates = [brisbane(self.db.get_latest_snapshot('QLD'))]
        if self.snapshot.exists():
            candidates.append(brisbane(pd.read_csv(self.snapshot, low_memory=False)))
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
            rows = connection.execute('SELECT target_date, price FROM local_forecasts WHERE target_date < ?', (today,)).fetchall()
            total = connection.execute('SELECT COUNT(*) FROM local_forecasts').fetchone()[0]
        actuals = {row.day.strftime('%Y-%m-%d'): row.price_cpl for row in daily.itertuples()}
        errors = [abs(price - actuals[date]) for date, price in rows if date in actuals]
        return {'saved': total, 'scored': len(errors), 'mae': round(sum(errors) / len(errors), 2) if errors else None}

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
                forecast = build_forecast(daily)
                prediction_log = self._prediction_log(forecast, daily)
                with self.db._connect() as connection:
                    db_rows = connection.execute("SELECT COUNT(*) FROM snapshots WHERE state='QLD'").fetchone()[0]
                    db_last = connection.execute("SELECT MAX(scraped_at) FROM snapshots WHERE state='QLD'").fetchone()[0]
                latest = str(live.scraped_at.max()) if not live.empty else None
                stations = []
                for row in live.to_dict('records'):
                    stations.append({key: row.get(key) for key in
                                     ['site_id', 'name', 'price_cpl', 'region', 'reported_at', 'latitude', 'longitude']})
                # Replace pandas NaN with JSON null.
                stations = json.loads(pd.DataFrame(stations).to_json(orient='records'))
                for station in stations:
                    # TransactionDateUtc is UTC even when the API omits a Z suffix.
                    reported = pd.to_datetime(station.get('reported_at'), format='mixed', utc=True, errors='coerce')
                    station['reported_at'] = reported.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if pd.notna(reported) else None
                historical = [{'date': row.day.strftime('%Y-%m-%d'), 'price': round(row.price_cpl, 1),
                               'stations': row.stations} for row in daily.itertuples()]
                gap_days = int((daily.day.max() - daily.day.min()).days + 1 - len(daily)) if len(daily) else 0
                self.cached = {'latest': latest, 'stations': stations, 'forecast': forecast,
                               'history': historical, 'prediction_log': prediction_log,
                               'summary': {'median': round(float(live.price_cpl.median()), 1) if not live.empty else None,
                                           'minimum': round(float(live.price_cpl.min()), 1) if not live.empty else None,
                                           'maximum': round(float(live.price_cpl.max()), 1) if not live.empty else None,
                                           'station_count': len(live)},
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
