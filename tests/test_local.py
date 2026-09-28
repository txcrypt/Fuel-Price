import json
import math
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
from fastapi.testclient import TestClient
import config
from forecasting import build_forecast, merge_history, predict
from fuel_engine import FetchError
from local_data import LocalData, CSV_COLUMNS
import fuel_dashboard


# One collection time for the whole run: snapshots are unique per (site_id, scraped_at), so a
# per-call datetime.now() made restart deduplication depend on which second the test landed on.
COLLECTED_AT = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def snapshot(price=180):
    return pd.DataFrame([{'site_id': '123', 'price_cpl': price, 'reported_at': '2026-09-16T01:00:00Z',
        'name': 'Test station', 'region': 'North', 'state': 'QLD', 'latitude': -27.4,
        'longitude': 153.0, 'scraped_at': COLLECTED_AT}])


class FakeEngine:
    def get_market_snapshot(self):
        return snapshot()


class CollectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data = LocalData(self.temp.name, FakeEngine())

    def test_real_writes_and_hourly_dedup_survive_restart(self):
        self.assertTrue(self.data.refresh())
        initial = self.data.collection.read_bytes()
        restarted = LocalData(self.temp.name, FakeEngine())
        self.assertTrue(restarted.refresh())
        self.assertEqual(initial, restarted.collection.read_bytes())
        payload = restarted.dashboard()
        self.assertEqual(payload['files']['csv']['rows'], 1)
        self.assertEqual(payload['files']['database_rows'], 1)
        self.assertEqual(payload['health']['csv'], 'up_to_date')
        self.assertEqual(payload['summary']['median'], 180)
        self.assertEqual(payload['prediction_log']['saved'], 7)

    def test_api_failure_keeps_last_saved_observations(self):
        self.data.refresh()
        original = self.data.collection.read_bytes()
        with patch.object(self.data.engine, 'get_market_snapshot', side_effect=RuntimeError('HTTP 401')):
            self.assertFalse(self.data.refresh())
        payload = self.data.dashboard()
        self.assertEqual(payload['health']['api'], 'error')
        self.assertEqual(payload['health']['api_evidence'], 'not_checked')
        self.assertEqual(payload['summary']['median'], 180)
        self.assertEqual(original, self.data.collection.read_bytes())

    def test_bad_csv_schema_is_not_overwritten(self):
        self.data.collection.write_text('wrong,header\n1,2\n')
        self.assertFalse(self.data.refresh())
        self.assertEqual(self.data.status['csv'], 'error')
        self.assertEqual(self.data.status['database'], 'recording')
        self.assertEqual(self.data.collection.read_text(), 'wrong,header\n1,2\n')

    def test_daily_stats_update_and_history_weights_stations_equally(self):
        self.data.db.save_daily_stats(datetime.now().date(), 'QLD', pd.Series([180, 200]))
        self.data.db.save_daily_stats(datetime.now().date(), 'QLD', pd.Series([200, 220]))
        self.assertEqual(self.data.db.get_all_daily_prices('QLD').iloc[-1].price_cpl, 210)
        rows = pd.concat([snapshot(100)]*3 + [snapshot(200)], ignore_index=True)
        rows.loc[3, 'site_id'] = '456'
        rows.loc[:2, 'scraped_at'] = ['2026-09-15 01:00:00', '2026-09-15 02:00:00', '2026-09-15 03:00:00']
        rows.loc[3, 'scraped_at'] = '2026-09-15 03:00:00'
        rows[CSV_COLUMNS].to_csv(self.data.collection, index=False)
        self.assertEqual(self.data._history().iloc[-1].price_cpl, 150)

    def test_endpoints_export_and_cross_origin_refresh(self):
        with patch.object(fuel_dashboard, 'LocalData', return_value=self.data):
            with TestClient(fuel_dashboard.app) as client:
                self.assertEqual(client.get('/').status_code, 200)
                self.assertEqual(client.get('/api/dashboard').status_code, 200)
                self.assertEqual(client.get('/api/health').json()['ok'], True)
                self.assertEqual(client.post('/api/refresh', headers={'origin': 'https://unrelated.example'}).status_code, 403)
                self.assertEqual(client.get('/api/export/unknown').status_code, 404)
                self.data.refresh()
                self.assertIn('price_cpl', client.get('/api/export/history').text)
                self.assertEqual(client.get('/api/advanced/briefing').status_code, 404)
                self.assertEqual(client.post('/api/buying-plan', json={'tank_l': 50}).status_code, 200)
                self.assertEqual(client.post('/api/buying-plan', json={'remaining_l': 100}).status_code, 422)


class StaleEngine:
    """Returns a real-shaped snapshot that was collected hours ago."""

    def get_market_snapshot(self):
        frame = snapshot()
        frame['scraped_at'] = (datetime.now() - timedelta(hours=5)).strftime('%Y-%m-%d %H:%M:%S')
        return frame


class CountingEngine:
    def __init__(self):
        self.calls = 0

    def get_market_snapshot(self):
        self.calls += 1
        return snapshot()


class ContinuityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data = LocalData(self.temp.name, FakeEngine())

    def wait_seconds(self, data=None):
        data = data or self.data
        # next_refresh is stored to whole seconds, so the remaining wait reads a fraction short.
        return math.ceil((datetime.fromisoformat(data.status['next_refresh']) - datetime.now()).total_seconds())

    def fail_once(self, category='network'):
        with patch.object(self.data.engine, 'get_market_snapshot',
                          side_effect=FetchError('upstream unreachable', category)):
            return self.data.refresh()

    def test_transient_failure_retries_before_the_normal_cadence(self):
        self.assertFalse(self.fail_once())
        self.assertEqual(self.data.status['consecutive_failures'], 1)
        self.assertEqual(self.data.status['failure_category'], 'network')
        self.assertIsNotNone(self.data.status['outage_started'])
        # Backoff floor is the upstream 60s price cache TTL, well under the 1800s success cadence.
        self.assertGreater(self.wait_seconds(), 55)
        self.assertLessEqual(self.wait_seconds(), 60)

    def test_repeated_failures_escalate_then_cap_at_the_normal_interval(self):
        delays = []
        for _ in range(8):
            self.fail_once()
            delays.append(self.wait_seconds())
        self.assertEqual(delays[:3], [60, 120, 240])
        self.assertTrue(all(b >= a for a, b in zip(delays, delays[1:])))
        self.assertTrue(all(d <= config.REFRESH_SECONDS for d in delays))
        self.assertEqual(delays[-1], config.REFRESH_SECONDS)
        self.assertEqual(self.data.status['consecutive_failures'], 8)

    def test_success_resets_the_failure_counter_and_cadence(self):
        self.fail_once()
        self.fail_once()
        self.assertTrue(self.data.refresh())
        self.assertEqual(self.data.status['consecutive_failures'], 0)
        self.assertIsNone(self.data.status['failure_category'])
        self.assertIsNone(self.data.status['outage_started'])
        self.assertGreater(self.wait_seconds(), config.REFRESH_SECONDS - 5)
        for field in ['last_database_write', 'last_snapshot_write', 'last_csv_write']:
            self.assertIsNotNone(self.data.status[field])

    def test_retry_after_failure_within_the_hour_adds_no_duplicate_csv_row(self):
        self.assertTrue(self.data.refresh())
        original = self.data.collection.read_bytes()
        self.assertFalse(self.fail_once())
        self.assertTrue(self.data.refresh())
        self.assertEqual(original, self.data.collection.read_bytes())
        self.assertEqual(self.data.status['csv'], 'up_to_date')
        self.assertEqual(self.data.dashboard()['files']['csv']['rows'], 1)

    def test_concurrent_refresh_makes_no_second_upstream_call(self):
        self.data.engine = CountingEngine()
        self.assertTrue(self.data.lock.acquire(blocking=False))
        try:
            self.assertFalse(self.data.refresh())
        finally:
            self.data.lock.release()
        self.assertEqual(self.data.engine.calls, 0)
        self.assertTrue(self.data.refresh())
        self.assertEqual(self.data.engine.calls, 1)

    def test_missed_window_is_recorded_as_an_outage_and_stays_unhealthy(self):
        self.data.engine = StaleEngine()
        self.assertTrue(self.data.refresh())
        stale = (datetime.now() - timedelta(hours=6)).isoformat(timespec='seconds')
        persisted = json.loads(self.data.status_path.read_text(encoding='utf-8'))
        persisted.update(next_refresh=stale, last_success=stale)
        self.data.status_path.write_text(json.dumps(persisted), encoding='utf-8')

        restarted = LocalData(self.temp.name, StaleEngine())
        self.assertEqual(len(restarted.status['outages']), 1)
        outage = restarted.status['outages'][0]
        self.assertGreater(outage['duration_minutes'], 90)
        self.assertEqual(outage['category'], 'not_running')
        health = restarted.dashboard()['health']
        self.assertFalse(health['fresh'])
        self.assertGreater(health['snapshot_age_minutes'], 90)
        self.assertEqual(health['outages'], restarted.status['outages'])
        self.assertEqual(health['consecutive_failures'], 0)

    def test_short_restart_records_no_outage(self):
        self.assertTrue(self.data.refresh())
        self.assertEqual(LocalData(self.temp.name, FakeEngine()).status['outages'], [])


class ForecastTests(unittest.TestCase):
    def test_stale_and_missing_days_do_not_create_future_certainty(self):
        daily = pd.DataFrame({'day': pd.to_datetime(['2026-01-01','2026-07-01']), 'price_cpl': [150,200]})
        result = build_forecast(daily, today='2026-09-16')
        self.assertEqual(result['status'], 'stale')
        self.assertEqual(result['points'], [])
        result = build_forecast(daily, today='2026-07-01')
        self.assertEqual(result['status'], 'limited')
        self.assertTrue(all(p['price'] == 200 and p['low'] is None for p in result['points']))

    def test_future_prices_cannot_change_a_past_prediction(self):
        index = pd.date_range('2026-01-01', periods=100)
        series = pd.Series(180 + np.sin(np.arange(100)/4)*15, index=index)
        for model in ['persistence','damped_trend','weekly','analog']:
            initial = predict(series, index[70], 7, model)
            changed = series.copy()
            changed.iloc[71:] = 340
            self.assertEqual(initial, predict(changed, index[70], 7, model))

    def test_backtest_has_disjoint_selection_and_validation_targets(self):
        daily = pd.DataFrame({'day':pd.date_range('2026-01-01', periods=85), 'price_cpl':np.full(85,180.)})
        result = build_forecast(daily, today='2026-03-26')
        self.assertEqual(result['status'], 'ready')
        self.assertGreaterEqual(result['validation_origins'], 8)
        self.assertEqual(result['model_id'], 'persistence')
        self.assertTrue(all(s['mae'] == 0 for s in result['scores']))
        self.assertLess(result['selection_origins'] + result['validation_origins'], 85-13)

    @staticmethod
    def reported(days, start='2026-01-01'):
        """A reported series shaped like qld_history_import.reported_series output."""
        index = pd.date_range(start, periods=days)
        return {'origin': 'reported', 'retrospective': True, 'expiry_days': 14,
                'days': [{'date': day.strftime('%Y-%m-%d'), 'price': round(180 + float(np.sin(i / 5)) * 12, 2),
                          'stations': 400} for i, day in enumerate(index)]}

    def test_backfilled_days_stay_reported_and_never_count_as_collection(self):
        reported = self.reported(120)                       # 2026-01-01 .. 2026-04-30
        collected = pd.DataFrame({'day': pd.to_datetime(['2026-04-29', '2026-04-30']), 'price_cpl': [111., 112.]})
        result = build_forecast(collected, today='2026-04-30', reported=reported)
        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['provenance']['collected_days'], 2)
        self.assertEqual(result['provenance']['reported_days'], 118)
        self.assertTrue(result['provenance']['retrospective_backfill'])
        self.assertGreater(result['provenance']['validation_origins_reported'], 0)
        self.assertIn('did not collect those days', result['reason'])
        # Backfilled days are not collection: the consecutive run counts only the two collected days.
        self.assertEqual(result['recent_consecutive_days'], 2)
        merged = merge_history(collected, reported)
        overlap = merged.loc[merged.day.eq('2026-04-30')]
        self.assertEqual(float(overlap.price_cpl.iloc[0]), 112.)
        self.assertEqual(overlap.provenance.iloc[0], 'collected')

    def test_backfill_cannot_make_stopped_collection_look_current(self):
        reported = self.reported(30)                        # runs to 2026-01-30
        collected = pd.DataFrame({'day': pd.to_datetime(['2026-01-05']), 'price_cpl': [150.]})
        result = build_forecast(collected, today='2026-01-30', reported=reported)
        self.assertEqual(result['status'], 'stale')
        self.assertEqual(result['points'], [])
        self.assertEqual(result['recent_consecutive_days'], 0)

    def test_later_reported_records_cannot_change_an_earlier_forecast(self):
        full = self.reported(160)
        cut = '2026-04-30'
        truncated = {**full, 'days': [day for day in full['days'] if day['date'] <= cut]}
        collected = pd.DataFrame({'day': pd.to_datetime([cut]), 'price_cpl': [180.]})
        as_known_then = build_forecast(collected, today=cut, reported=truncated)
        with_later_months = build_forecast(collected, today=cut, reported=full)
        self.assertEqual(as_known_then['status'], 'ready')
        self.assertTrue(as_known_then['points'])
        self.assertEqual(as_known_then, with_later_months)


if __name__ == '__main__':
    unittest.main()
