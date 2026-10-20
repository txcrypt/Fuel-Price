"""Patterns read from the price history: station histories, a favourites watchlist, city
comparisons, weekday/hour timing and day-by-day replays of past price jumps.

One daily table (days x stations) underlies them: the official Queensland reports (Brisbane
area, 2025 onwards, retrospective) overlaid with this app's own captures (all of Queensland,
from collection). Each station's last report of a day is held for up to 14 days, as in
``qld_history_import.reported_series``. Observed data only; nothing here is a forecast.
"""
import threading
import numpy as np
import pandas as pd
from qld_history_import import _daily_last_reports, UNAVAILABLE_CPL, AS_OF_EXPIRY_DAYS

JUMP_CPL = 8.0          # A one-day rise in the area median this large starts a jump (restoration).
RESTORED_CPL = 20.0     # A station this far above the day's median has already restored.
MIN_REGION = 5          # Fewest stations for a regional median or percentile to mean anything.


def _steps(events):
    """Each report's local time and change from that station's previous report (once per history load)."""
    if events is None or events.empty:
        return events
    frame = events.loc[events.price_cpl.ne(UNAVAILABLE_CPL)].copy()
    frame['site_id'] = frame.site_id.astype(str)
    frame['time'] = pd.to_datetime(frame.reported_at_utc, utc=True).dt.tz_convert('Australia/Brisbane')
    frame = frame.sort_values(['site_id', 'time'])
    frame['step'] = frame.groupby('site_id').price_cpl.diff()
    return frame


class Insights:
    def __init__(self, data, observatory):
        self.data, self.observatory = data, observatory
        self.lock = threading.Lock()
        self._key, self._wide, self._events = None, None, None

    # -- the daily table ------------------------------------------------------------------------
    def wide(self):
        """Days x site_id held prices, rebuilt only when the history or the captures change."""
        obs = self.observatory
        with self.data.data_lock, obs.lock:
            obs._load()
            frame = obs.frame
            with self.data.db._connect() as connection:
                reported = connection.execute('SELECT COUNT(*), MAX(reported_at_utc) FROM reported_price_events').fetchone()
            key = (obs.key, reported)
            if key == self._key:
                return self._wide
            events = self.data.db.get_reported_events('Unleaded')
        parts = []
        if not events.empty:
            _, backfill = _daily_last_reports(events)
            parts.append(backfill)
        if not frame.empty and 'report_time' in frame:
            live = frame.dropna(subset=['report_time']).assign(day=lambda f: f.report_time.dt.normalize())
            live = live.sort_values('report_time').drop_duplicates(['day', 'site_id'], keep='last')
            parts.append(live.pivot(index='day', columns='site_id', values='price_cpl'))
        if not parts:
            wide = pd.DataFrame()
        else:
            # Live captures win on days both cover: they are this app's own observations.
            wide = parts[-1] if len(parts) == 1 else parts[1].combine_first(parts[0])
            wide.columns = wide.columns.astype(str)
            wide = wide.reindex(pd.date_range(wide.index.min(), wide.index.max(), freq='D'))
            wide = wide.ffill(limit=AS_OF_EXPIRY_DAYS).mask(lambda f: f.eq(UNAVAILABLE_CPL))
        steps = _steps(events)
        with self.lock:
            self._key, self._wide, self._events, self._timing = key, wide, steps, {}
        return wide

    # -- one station, and a watchlist ------------------------------------------------------------
    def station(self, site_id, region_sites, days=180):
        """A station's daily price beside its region's median, and how often it was among the cheapest."""
        wide = self.wide()
        site_id = str(site_id)
        if wide.empty or site_id not in wide:
            return {'site_id': site_id, 'days': [], 'cheap_share': None}
        peers = [s for s in region_sites if s in wide] or list(wide.columns)
        window = wide.iloc[-days:]
        own, region = window[site_id], window[peers]
        counted = region.notna().sum(axis=1) >= MIN_REGION
        median, p25 = region.median(axis=1), region.quantile(.25, axis=1)
        rows = [{'date': d.strftime('%Y-%m-%d'), 'price': round(float(p), 1),
                 'region_median': round(float(median[d]), 1) if counted[d] else None}
                for d, p in own.dropna().items()]
        valid = own.notna() & counted
        cheap = float((own[valid] <= p25[valid]).mean()) if valid.any() else None
        return {'site_id': site_id, 'days': rows, 'cheap_share': round(cheap, 3) if cheap is not None else None,
                'cheap_days': int(valid.sum()),
                'note': 'Cheap share: days this station was at or below the cheapest quarter of stations in its city.'}

    def watch(self, sites, region_of):
        """Latest price, change since the previous day and gap to the city median, per watched station."""
        wide = self.wide()
        out = []
        for site in sites:
            site = str(site)
            series = wide[site].dropna() if not wide.empty and site in wide else pd.Series(dtype=float)
            if series.empty:
                out.append({'site_id': site, 'price': None})
                continue
            day = series.index[-1]
            before = series.loc[:day - pd.Timedelta(days=1)]
            peers = [s for s, r in region_of.items() if r == region_of.get(site) and s in wide]
            row = wide.loc[day, peers] if peers else pd.Series(dtype=float)
            median = float(row.median()) if row.notna().sum() >= MIN_REGION else None
            out.append({'site_id': site, 'price': round(float(series.iloc[-1]), 1), 'date': day.strftime('%Y-%m-%d'),
                        'change': round(float(series.iloc[-1] - before.iloc[-1]), 1) if len(before) else None,
                        'vs_median': round(float(series.iloc[-1]) - median, 1) if median is not None else None,
                        'region': region_of.get(site)})
        return out

    # -- when prices move ------------------------------------------------------------------------
    def timing(self, sites):
        """Weekday price pattern (median detrended against its own week) and the hours stations raise prices.
        Cached per set of stations until the history changes."""
        wide = self.wide()
        cols = [s for s in sites if s in wide] if sites else list(wide.columns)
        key = tuple(sorted(cols))
        cache = getattr(self, '_timing', None)
        if cache is None:
            cache = self._timing = {}
        if key not in cache:
            cache[key] = self._timing_build(wide, cols)
        return cache[key]

    def _timing_build(self, wide, cols):
        if wide.empty or not cols:
            return {'weekdays': [], 'hours': []}
        median = wide[cols].median(axis=1).dropna()
        # Price relative to a centred 7-day median, so the cycle and cost trends cancel out.
        rel = (median - median.rolling(7, center=True, min_periods=7).median()).dropna()
        weekdays = []
        for i, name in enumerate(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']):
            values = rel[rel.index.dayofweek == i]
            weekdays.append({'day': name, 'relative': round(float(values.mean()), 2) if len(values) else None,
                             'cheapest_share': None, 'n': int(len(values))})
        # Which weekday was strictly the cheapest of its week. Flat weeks tie, and a tie names no day:
        # idxmin would hand every tie to Monday and invent a pattern.
        winners, weeks = [], 0
        for _, values in rel.groupby(rel.index.to_period('W')):
            if len(values) < 7:
                continue
            weeks += 1
            lowest = values[values <= values.min() + 0.05]
            if len(lowest) == 1:
                winners.append(lowest.index[0].dayofweek)
        counts = pd.Series(winners, dtype=int).value_counts()
        for i, w in enumerate(weekdays):
            w['cheapest_share'] = round(float(counts.get(i, 0) / weeks), 3) if weeks else None
        spread = [w['relative'] for w in weekdays if w['relative'] is not None]
        hours = self._hike_hours(set(cols))
        return {'weekdays': weekdays, 'weeks': weeks, 'clear_weeks': len(winners), 'hours': hours,
                # A pattern needs both history (8 full weeks) and size (half a cent between weekdays).
                'pattern': weeks >= 8 and bool(spread) and max(spread) - min(spread) >= 0.5,
                'basis': f'{len(rel)} days of the area median, each compared with its own centred week; '
                         'hour pattern from station price reports. A pattern, not a promise.'}

    def _hike_hours(self, sites):
        steps = self._events
        if steps is None or steps.empty:
            return []
        frame = steps.loc[steps.site_id.isin(sites)]
        if frame.empty:
            return []
        hikes = frame.loc[frame.step >= JUMP_CPL]
        cuts = frame.loc[frame.step <= -1]
        by = lambda f: f.time.dt.hour.value_counts().reindex(range(24), fill_value=0)  # noqa: E731
        h, c = by(hikes), by(cuts)
        return [{'hour': i, 'hikes': int(h[i]), 'cuts': int(c[i])} for i in range(24)]

    # -- city comparison -------------------------------------------------------------------------
    @staticmethod
    def leaderboard(sites):
        """Current U91 and diesel by city, from the latest full capture."""
        by_city = {}
        for s in sites:
            city = s.get('city')
            if not city:
                continue
            u91 = next((p['price_cpl'] for p in s['fuels'] if p['fuel_id'] == 2 and p['status'] == 'available'), None)
            diesel = next((p['price_cpl'] for p in s['fuels'] if p['fuel_id'] == 3 and p['status'] == 'available'), None)
            entry = by_city.setdefault(city, {'u91': [], 'diesel': []})
            if u91 is not None:
                entry['u91'].append((u91, s['name']))
            if diesel is not None:
                entry['diesel'].append(diesel)
        rows = []
        for city, e in by_city.items():
            if len(e['u91']) < MIN_REGION:
                continue
            prices = sorted(e['u91'])
            values = [p for p, _ in prices]
            rows.append({'city': city, 'stations': len(values), 'median': round(float(np.median(values)), 1),
                         'lowest': {'price': prices[0][0], 'name': prices[0][1]},
                         'highest': {'price': prices[-1][0], 'name': prices[-1][1]},
                         'spread': round(prices[-1][0] - prices[0][0], 1),
                         'diesel_median': round(float(np.median(e['diesel'])), 1) if len(e['diesel']) >= MIN_REGION else None})
        return sorted(rows, key=lambda r: r['median'])

    # -- replaying a jump ------------------------------------------------------------------------
    def jumps(self, sites):
        """Days the area median rose by JUMP_CPL or more after not doing so the day before."""
        wide = self.wide()
        cols = [s for s in sites if s in wide]
        if wide.empty or not cols:
            return []
        median = wide[cols].median(axis=1)
        diff = median.diff()
        start = diff.ge(JUMP_CPL) & ~diff.shift(1).ge(JUMP_CPL)
        return [{'date': d.strftime('%Y-%m-%d'), 'from': round(float(median.shift(1)[d]), 1),
                 'rise': round(float(diff.loc[d:d + pd.Timedelta(days=2)].clip(lower=0).sum()), 1)}
                for d in median.index[start]]

    def replay(self, sites, start, before=10, after=4):
        """Day-by-day held prices around a date, and who had restored (>= median + 20 c/L) each day."""
        wide = self.wide()
        cols = [s for s in sites if s in wide]
        day = pd.Timestamp(start)
        window = wide.loc[day - pd.Timedelta(days=before):day + pd.Timedelta(days=after), cols]
        frames = []
        for d, row in window.iterrows():
            row = row.dropna()
            if len(row) < MIN_REGION:
                continue
            median = float(row.median())
            frames.append({'date': d.strftime('%Y-%m-%d'), 'median': round(median, 1),
                           'restored_share': round(float(row.ge(median + RESTORED_CPL).mean()), 3),
                           'prices': {site: round(float(p), 1) for site, p in row.items()}})
        return {'start': day.strftime('%Y-%m-%d'), 'frames': frames, 'threshold_cpl': RESTORED_CPL}

    def month(self, sites, month):
        """A calendar month (to date, for the current one) of daily prices, plus the day before it so the
        first day has a change, the jumps inside it and the range of months that have data."""
        wide = self.wide()
        first = pd.Timestamp(month + '-01')
        out = self.replay(sites, first, before=1, after=first.days_in_month - 1)
        out.update(month=month, jumps=[j for j in self.jumps(sites) if j['date'].startswith(month)],
                   months=[wide.index[0].strftime('%Y-%m'), wide.index[-1].strftime('%Y-%m')] if not wide.empty else [])
        return out
