import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from fastapi.testclient import TestClient
import fuel_dashboard
from local_data import LocalData, CSV_COLUMNS
from observatory import Observatory
from test_local import FakeEngine, snapshot


class ObservatoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data = LocalData(self.temp.name, FakeEngine())
        self.observatory = Observatory(self.data)

    def write_rows(self, rows):
        frame = pd.DataFrame(rows)
        frame[CSV_COLUMNS].to_csv(self.data.collection, index=False)

    def test_report_formats_timezone_dedup_and_source_reconciliation(self):
        base = snapshot().iloc[0].to_dict()
        rows = [dict(base, site_id='1', price_cpl=160, reported_at='2026-09-15T15:00:00Z', scraped_at='2026-09-17 12:00:00'),
                dict(base, site_id='1', price_cpl=160, reported_at='2026-09-15T15:00:00.000+00:00', scraped_at='2026-09-17 13:00:00'),
                dict(base, site_id='2', price_cpl=200, reported_at='2026-09-16T01:00:00.12Z', scraped_at='2026-09-17 13:00:00'),
                dict(base, site_id='3', price_cpl=250, state='WA'),
                dict(base, site_id='4', latitude=-28.5)]
        self.write_rows(rows)
        self.data.snapshot.write_text(pd.DataFrame(rows[:3]).to_csv(index=False))
        result = self.observatory.overview()
        self.assertEqual(result['audit']['rows'], 5)
        self.assertEqual(result['audit']['brisbane_rows'], 3)
        self.assertEqual(result['audit']['excluded_rows'], 2)
        self.assertEqual(result['audit']['unique_reports'], 2)
        self.assertEqual(result['audit']['invalid_report_dates'], 0)
        self.assertEqual(result['events'], [{'date':'2026-09-16', 'price':180, 'stations':2}])
        self.assertEqual(result['audit']['capture_days'], 1)
        self.assertEqual(len(self.observatory.station('1')['reports']), 1)
        self.assertEqual(len(self.observatory.station('1')['observations']), 2)
        self.assertEqual(self.observatory.rows(state='WA')['total'], 1)
        self.assertEqual(self.observatory.rows(offset=1, limit=1)['rows'][0]['site_id'], '3')

    def test_movement_is_matched_against_earlier_capture(self):
        base = snapshot().iloc[0].to_dict()
        self.write_rows([dict(base, site_id='123', price_cpl=190, scraped_at='2026-09-16 12:00:00'),
                         dict(base, site_id='123', price_cpl=180, scraped_at='2026-09-17 12:00:00'),
                         dict(base, site_id='456', price_cpl=150, scraped_at='2026-09-17 12:00:00')])
        current = pd.DataFrame([dict(base, price_cpl=175, scraped_at='2026-09-17 13:00:00'),
                                dict(base, site_id='789', price_cpl=210, scraped_at='2026-09-17 13:00:00')])
        current.to_csv(self.data.snapshot, index=False)
        result = self.observatory.overview()
        self.assertEqual(result['previous_capture'], '2026-09-17 12:00:00')
        self.assertEqual(result['changes'], [{'site_id':'123', 'before':180, 'change':-5}])

    def test_external_append_invalidates_caches_without_refresh(self):
        base = snapshot().iloc[0].to_dict()
        self.write_rows([base])
        self.assertEqual(self.data.dashboard()['files']['csv']['rows'], 1)
        self.assertEqual(self.observatory.rows()['total'], 1)
        self.write_rows([base, dict(base, site_id='456', price_cpl=220)])
        self.assertEqual(self.data.dashboard()['files']['csv']['rows'], 2)
        self.assertEqual(self.data.dashboard()['history'][-1]['price'], 200)
        self.assertEqual(self.observatory.rows()['total'], 2)

    def test_raw_archive_preserved_and_events_excluded_from_daily_history(self):
        base = snapshot().iloc[0].to_dict()
        self.write_rows([base])
        archive = pd.DataFrame([dict(base, reported_at='2025-01-01T20:00:00Z'), dict(base, latitude=-40)])
        archive.drop(columns=['scraped_at', 'state']).to_csv(Path(self.temp.name)/'brisbane_fuel_history_clean.csv', index=False)
        self.assertEqual(self.observatory.rows(source='archive')['total'], 2)
        self.assertEqual(len(self.observatory.overview()['archive_events']), 1)
        self.assertEqual(len(self.data.dashboard()['history']), 1)
        self.assertEqual(self.observatory.rows(source='archive', state='WA')['total'], 0)

    def test_api_evidence_filters_and_bounds(self):
        with patch.object(fuel_dashboard, 'LocalData', return_value=self.data):
            with TestClient(fuel_dashboard.app) as client:
                self.data.refresh()
                self.assertIn('Fuel Observatory', client.get('/').text)
                self.assertEqual(client.get('/planner').status_code, 200)
                self.assertEqual(client.get('/api/observatory').json()['audit']['brisbane_rows'], 1)
                self.assertEqual(client.get('/api/observatory/station/123').json()['rows'], 1)
                self.assertEqual(client.get('/api/observatory/rows?site_id=123').json()['total'], 1)
                self.assertEqual(client.get('/api/observatory/rows?limit=1000').status_code, 422)
                self.assertEqual(client.get('/api/observatory/rows?source=unknown').status_code, 422)


if __name__ == '__main__':
    unittest.main()
