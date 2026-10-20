from unittest import TestCase
import pandas as pd
from insights import Insights


def table(columns, days=28, start='2026-01-05'):
    """A days x stations price table; ``columns`` maps site -> function(day index) -> price."""
    index = pd.date_range(start, periods=days, freq='D')
    return pd.DataFrame({site: [f(i) for i in range(days)] for site, f in columns.items()}, index=index)


def insights(frame, events=None):
    ins = Insights(data=None, observatory=None)
    ins.wide = lambda: frame
    ins._events = events
    return ins


class InsightTests(TestCase):
    def test_station_cheap_share_and_watch_change(self):
        # Five peers at 200-204; 'cheap' sits at 199 for the last 10 days, 210 before.
        cols = {f'p{i}': (lambda i: (lambda d: 200.0 + i))(i) for i in range(5)}
        cols['cheap'] = lambda d: 199.0 if d >= 18 else 210.0
        ins = insights(table(cols))
        result = ins.station('cheap', list(cols))
        self.assertEqual((result['cheap_share'], result['cheap_days']), (round(10 / 28, 3), 28))
        watch = ins.watch(['cheap', 'missing'], {s: 'Town' for s in cols})[0]
        self.assertEqual((watch['price'], watch['change'], watch['vs_median']), (199.0, 0.0, -2.5))

    def test_flat_weeks_name_no_cheapest_day(self):
        ins = insights(table({f's{i}': (lambda d: 200.0) for i in range(5)}, days=70), events=pd.DataFrame())
        timing = ins.timing([f's{i}' for i in range(5)])
        self.assertEqual(timing['clear_weeks'], 0)
        self.assertFalse(timing['pattern'])

    def test_jumps_and_replay_restored_share(self):
        # Ten stations flat at 180; from day 10 one more station a day is at 210. On day 14 (19 Jan) five of
        # ten have restored, so the median moves 180 -> 195: that is the jump day.
        cols = {f's{i}': (lambda i: (lambda d: 210.0 if d >= 10 + i or d >= 15 else 180.0))(i) for i in range(10)}
        ins = insights(table(cols))
        jumps = ins.jumps(list(cols))
        self.assertEqual(jumps[0]['date'], '2026-01-19')
        frames = {f['date']: f for f in ins.replay(list(cols), '2026-01-20', before=6, after=1)['frames']}
        self.assertEqual(frames['2026-01-14']['restored_share'], 0.0)
        self.assertEqual(frames['2026-01-16']['restored_share'], 0.2)   # two of ten at +30 over a 180 median
        month = ins.month(list(cols), '2026-01')
        self.assertEqual(month['frames'][0]['date'], '2026-01-05')   # the table starts on the 5th
        self.assertEqual([j['date'] for j in month['jumps']], ['2026-01-19'])
        self.assertEqual(month['months'][0], '2026-01')

    def test_leaderboard_needs_five_stations_and_sorts_by_median(self):
        def site(city, price, diesel=None):
            fuels = [{'fuel_id': 2, 'status': 'available', 'price_cpl': price}]
            if diesel:
                fuels.append({'fuel_id': 3, 'status': 'available', 'price_cpl': diesel})
            return {'city': city, 'name': f'{city} {price}', 'fuels': fuels}
        sites = [site('A', 200 + i, 280) for i in range(5)] + [site('B', 190 + i) for i in range(6)] + [site('C', 100)]
        rows = Insights.leaderboard(sites)
        self.assertEqual([(r['city'], r['median'], r['diesel_median']) for r in rows], [('B', 192.5, None), ('A', 202.0, 280.0)])
