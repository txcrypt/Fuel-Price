"""Read-only evidence views. Report dates are retrospective, never training data."""
import json
import threading
import pandas as pd
from local_data import brisbane


def records(frame):
    return json.loads(frame.to_json(orient='records', date_format='iso'))


def fingerprint(path):
    return (path.stat().st_mtime_ns, path.stat().st_size) if path.exists() else None


class Observatory:
    def __init__(self, data):
        self.data = data
        self.key = None
        self.lock = threading.RLock()

    def _load(self):
        archive = self.data.root / 'brisbane_fuel_history_clean.csv'
        key = (fingerprint(self.data.collection), fingerprint(archive))
        if key == self.key:
            return
        raw = pd.read_csv(self.data.collection, dtype={'site_id': str}, low_memory=False) if self.data.collection.exists() else pd.DataFrame()
        self.raw = raw
        self.frame = brisbane(raw)
        self.archive_raw = pd.read_csv(archive, dtype={'site_id': str}, low_memory=False) if archive.exists() else pd.DataFrame()
        self.archive = brisbane(self.archive_raw)
        for frame in [self.frame, self.archive]:
            if frame.empty:
                continue
            # API timestamps mix fractional seconds, Z, and UTC offsets.
            frame['report_time'] = pd.to_datetime(frame.reported_at, format='mixed', utc=True, errors='coerce').dt.tz_convert('Australia/Brisbane').dt.tz_localize(None)
            frame['capture_time'] = pd.to_datetime(frame.get('scraped_at'), format='mixed', errors='coerce') if 'scraped_at' in frame else pd.NaT
        self.key = key

    @staticmethod
    def event_series(frame):
        if frame.empty:
            return []
        events = frame.dropna(subset=['report_time']).drop_duplicates(['site_id', 'report_time', 'price_cpl']).copy()
        events['date'] = events.report_time.dt.strftime('%Y-%m-%d')
        # Equal weight per station per report day, not per repeated capture.
        daily = events.sort_values('report_time').drop_duplicates(['date', 'site_id'], keep='last')
        return records(daily.groupby('date').agg(price=('price_cpl', 'median'), stations=('site_id', 'nunique')).reset_index())

    def overview(self):
        with self.data.data_lock, self.lock:
            self._load()
            frame = self.frame
            if frame.empty:
                return {'audit': {'rows': len(self.raw), 'brisbane_rows': 0}, 'captures': [], 'events': [], 'archive_events': [], 'calendar': [], 'changes': [], 'previous_capture': None}
            captures = frame.dropna(subset=['capture_time']).sort_values('capture_time').drop_duplicates(['capture_time', 'site_id'], keep='last')
            series = captures.groupby('capture_time').agg(price=('price_cpl', 'median'), minimum=('price_cpl', 'min'), maximum=('price_cpl', 'max'), stations=('site_id', 'nunique')).reset_index().rename(columns={'capture_time': 'date'})
            calendar = captures.assign(date=captures.capture_time.dt.strftime('%Y-%m-%d')).groupby('date').agg(rows=('site_id', 'size'), stations=('site_id', 'nunique'), captures=('capture_time', 'nunique')).reset_index()
            latest = self.data.dashboard()
            # Compare live data with the last distinct earlier CSV capture.
            previous = captures.loc[captures.capture_time < pd.Timestamp(latest['latest'])] if latest['latest'] else captures.iloc[:0]
            previous_time = previous.capture_time.max() if len(previous) else None
            previous = previous.loc[previous.capture_time.eq(previous_time)].set_index('site_id')
            changes = []
            for station in latest['stations']:
                sid = str(station['site_id'])
                if sid in previous.index:
                    before = float(previous.loc[sid, 'price_cpl'])
                    changes.append({'site_id': sid, 'before': before, 'change': round(station['price_cpl'] - before, 1)})
            return {'audit': {'source': self.data.collection.name, 'rows': len(self.raw),
                    'states': {str(k): int(v) for k, v in self.raw.state.value_counts(dropna=False).items()},
                    'brisbane_rows': len(frame), 'excluded_rows': len(self.raw) - len(frame),
                    'stations': int(frame.site_id.nunique()), 'capture_days': len(calendar), 'captures': len(series),
                    'unique_reports': len(frame.dropna(subset=['report_time']).drop_duplicates(['site_id', 'report_time', 'price_cpl'])),
                    'invalid_capture_dates': int(frame.capture_time.isna().sum()), 'invalid_report_dates': int(frame.report_time.isna().sum()),
                    'archive_rows': len(self.archive_raw), 'archive_brisbane_rows': len(self.archive), 'bytes': self.data.collection.stat().st_size},
                    'captures': records(series), 'events': self.event_series(frame),
                    'archive_events': self.event_series(self.archive), 'calendar': records(calendar),
                    'previous_capture': str(previous_time) if previous_time is not None else None, 'changes': changes}

    def station(self, site_id):
        with self.data.data_lock, self.lock:
            self._load()
            frame = self.frame
            if frame.empty:
                return {'site_id': site_id, 'observations': [], 'reports': []}
            station = frame.loc[frame.site_id.eq(site_id)]
            observations = station.dropna(subset=['capture_time']).sort_values('capture_time').drop_duplicates('capture_time', keep='last')
            reports = station.dropna(subset=['report_time']).sort_values('report_time').drop_duplicates(['report_time', 'price_cpl'])
            return {'site_id': site_id, 'rows': len(station),
                    'observations': records(observations[['capture_time', 'price_cpl']].rename(columns={'capture_time': 'date', 'price_cpl': 'price'})),
                    'reports': records(reports[['report_time', 'price_cpl']].rename(columns={'report_time': 'date', 'price_cpl': 'price'}))}

    def rows(self, source='live', site_id='', state='', offset=0, limit=50):
        with self.data.data_lock, self.lock:
            self._load()
            frame = self.raw if source == 'live' else self.archive_raw
            if site_id and not frame.empty:
                frame = frame.loc[frame.site_id.eq(site_id)]
            if state:
                frame = frame.loc[frame.state.eq(state)] if 'state' in frame else frame.iloc[:0] if state != 'QLD' else frame
            return {'total': len(frame), 'offset': offset, 'columns': list(frame.columns),
                    'rows': records(frame.iloc[::-1].iloc[offset:offset + limit])}
