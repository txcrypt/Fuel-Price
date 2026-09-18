import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
from fastapi.testclient import TestClient
from forecasting import build_forecast, predict
from local_data import LocalData, CSV_COLUMNS
import fuel_dashboard


def snapshot(price=180):
    return pd.DataFrame([{'site_id': '123', 'price_cpl': price, 'reported_at': '2026-09-16T01:00:00Z',
        'name': 'Test station', 'region': 'North', 'state': 'QLD', 'latitude': -27.4,
        'longitude': 153.0, 'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}])


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


if __name__ == '__main__':
    unittest.main()
