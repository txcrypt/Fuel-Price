"""Provenance tests for the official monthly backfill. Fixtures only — no network."""
import tempfile
import unittest
from pathlib import Path

import pandas as pd

import qld_history_import as history
from data_store import FuelDataStore
from observatory import Observatory
from local_data import LocalData
from test_local import FakeEngine, snapshot

HEADER = ['SiteId', 'Site_Name', 'Site_Brand', 'Sites_Address_Line_1', 'Site_Suburb',
          'Site_State', 'Site_Post_Code', 'Site_Latitude', 'Site_Longitude',
          'Fuel_Type', 'Price', 'TransactionDateutc']


def row(site='1', state='QLD', lat=-27.47, lng=153.02, fuel='Unleaded',
        price=2099, when='04/08/2026 10:00'):
    return dict(zip(HEADER, [site, f'Site {site}', 'Brand', '1 Test St', 'Brisbane',
                             state, '4000', lat, lng, fuel, price, when]))


class HistoryImportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = FuelDataStore(str(self.root / 'test.db'))

    def fixture(self, rows, name='month.csv'):
        path = self.root / name
        pd.DataFrame(rows, columns=HEADER).to_csv(path, index=False)
        return path

    def events(self):
        return self.store.get_reported_events()

    def test_spaced_headers_import_the_same_as_underscored_ones(self):
        # March 2026 ships 'Site State'/'Fuel Type'; January 2026 ships 'Site_State'.
        # A real 26-month import died on this, so both spellings must read identically.
        underscored = self.fixture([row()], 'underscored.csv')
        spaced = self.root / 'spaced.csv'
        frame = pd.DataFrame([row()], columns=HEADER)
        frame.columns = [c.replace('_', ' ') for c in HEADER]
        frame.to_csv(spaced, index=False)
        first, counts = history.read_events(underscored)
        second, spaced_counts = history.read_events(spaced)
        self.assertEqual(len(second), 1)
        pd.testing.assert_frame_equal(first, second)
        self.assertEqual(counts, spaced_counts)

    def test_a_genuinely_different_schema_names_the_missing_columns(self):
        path = self.root / 'renamed.csv'
        frame = pd.DataFrame([row()], columns=HEADER).rename(columns={'Site_State': 'Region_Code'})
        frame.to_csv(path, index=False)
        with self.assertRaisesRegex(ValueError, 'site_state'):
            history.read_events(path)

    def test_reader_releases_the_file_so_the_raw_download_can_be_deleted(self):
        # Windows refuses to unlink a file the chunk reader still holds open.
        path = self.fixture([row()], 'closed.csv')
        history.read_events(path)
        path.unlink()
        self.assertFalse(path.exists())

    # ------------------------------------------------------------------
    def test_second_import_of_the_same_file_adds_no_duplicate_rows(self):
        path = self.fixture([row(), row(site='2', price=1999, when='05/08/2026 09:30')])
        first = history.import_file(self.store, path, 'src-1', url='http://example/x.csv', sha256='abc')
        second = history.import_file(self.store, path, 'src-1', url='http://example/x.csv', sha256='abc')
        self.assertEqual(first['inserted'], 2)
        self.assertEqual(second['inserted'], 0)
        self.assertEqual(len(self.events()), 2)

    def test_day_first_timestamps_are_read_as_utc_and_shown_in_brisbane(self):
        # 04/08/2026 is 4 August; read as MM/DD it would be 8 April.
        # 16:00 UTC is 02:00 next day in Brisbane (UTC+10, no DST).
        path = self.fixture([row(when='04/08/2026 16:00')])
        history.import_file(self.store, path, 'src-1')
        self.assertEqual(self.events().reported_at_utc.tolist(), ['2026-08-04T16:00:00Z'])
        self.assertEqual(history.reported_series(self.store)['days'][0]['date'], '2026-08-05')

    def test_prices_convert_from_tenths_of_a_cent(self):
        history.import_file(self.store, self.fixture([row(price=2099)]), 'src-1')
        self.assertEqual(self.events().price_cpl.tolist(), [209.9])

    def test_unavailable_is_counted_not_stored_as_a_price(self):
        path = self.fixture([row(price=9999), row(site='2', price=2099)])
        result = history.import_file(self.store, path, 'src-1')
        self.assertEqual(result['rows_unavailable'], 1)
        self.assertEqual(result['rows_kept'], 2)
        stored = self.events().set_index('site_id').price_cpl
        self.assertEqual(stored['1'], history.UNAVAILABLE_CPL)
        self.assertNotIn(999.9, stored.tolist())
        # The unavailable station is excluded from the day, not priced at 999.9.
        day = history.reported_series(self.store)['days'][0]
        self.assertEqual((day['price'], day['stations']), (209.9, 1))

    def test_out_of_bounds_and_non_qld_and_other_fuels_are_excluded(self):
        path = self.fixture([row(site='keep'),
                             row(site='interstate', state='NSW', lat=-33.8, lng=151.2),
                             row(site='cairns', lat=-16.9, lng=145.7),
                             row(site='west', lng=140.0),
                             row(site='diesel', fuel='Diesel'),
                             row(site='silly', price=10)])
        result = history.import_file(self.store, path, 'src-1')
        self.assertEqual(result['rows_read'], 6)
        self.assertEqual(result['rows_kept'], 1)
        self.assertEqual(self.events().site_id.tolist(), ['keep'])

    def test_as_of_expiry_holds_to_the_boundary_and_drops_past_it(self):
        # One station reports once; a second keeps reporting so the day grid continues.
        rows = [row(site='quiet', price=2000, when='01/08/2026 00:00')]
        rows += [row(site='chatty', price=1800, when=f'{day:02d}/08/2026 00:00')
                 for day in range(1, 20)]
        history.import_file(self.store, self.fixture(rows), 'src-1')
        days = {d['date']: d for d in history.reported_series(self.store)['days']}
        self.assertEqual(days['2026-08-01']['stations'], 2)
        self.assertEqual(days['2026-08-15']['stations'], 2)   # day + 14, still carried
        self.assertEqual(days['2026-08-16']['stations'], 1)   # day + 15, expired
        self.assertEqual(days['2026-08-15']['price'], 190.0)
        self.assertEqual(days['2026-08-16']['price'], 180.0)

    def test_series_and_coverage_are_labelled_retrospective(self):
        history.import_file(self.store, self.fixture([row()]), 'src-1')
        series = history.reported_series(self.store)
        self.assertEqual(series['origin'], 'reported')
        self.assertTrue(series['retrospective'])
        self.assertEqual(series['expiry_days'], 14)
        self.assertTrue(history.coverage(self.store)['retrospective'])

    def test_coverage_reconciles_carried_days_against_raw_rows(self):
        rows = [row(site='1', price=2000, when='01/08/2026 00:00'),
                row(site='1', price=2010, when='01/08/2026 06:00'),
                row(site='2', price=1900, when='01/08/2026 00:00'),
                row(site='2', price=9999, when='02/08/2026 00:00'),
                row(site='3', price=1800, when='02/08/2026 00:00')]
        history.import_file(self.store, self.fixture(rows), 'src-1')
        report = history.coverage(self.store)
        self.assertEqual(report['raw_events'], 5)
        self.assertEqual(report['counted_events'], 5)
        self.assertTrue(report['reconciled'])
        first, second = report['days']
        self.assertEqual((first['raw_events'], first['stations_reporting']), (3, 2))
        self.assertEqual((first['stations_priced'], first['stations_carried_forward']), (2, 0))
        # Day two: site 1 carried, site 2 explicitly unavailable, site 3 fresh.
        self.assertEqual((second['stations_reporting'], second['unavailable']), (2, 1))
        self.assertEqual((second['stations_priced'], second['stations_carried_forward']), (2, 1))

    def test_checksum_and_counts_are_recorded_in_import_sources(self):
        path = self.fixture([row(), row(site='2', price=9999)])
        history.import_file(self.store, path, 'src-1', url='http://example/x.csv',
                            dataset='fuel-price-reporting-2026', resource_id='res-9',
                            sha256='deadbeef', published_at='2026-09-01T00:00:00')
        sources = self.store.get_import_sources()
        self.assertEqual(len(sources), 1)
        recorded = sources.iloc[0]
        self.assertEqual(recorded.sha256, 'deadbeef')
        self.assertEqual(recorded.resource_id, 'res-9')
        self.assertEqual((recorded.rows_read, recorded.rows_kept, recorded.rows_unavailable), (2, 2, 1))
        self.assertEqual(recorded.bytes, path.stat().st_size)
        self.assertTrue(recorded.imported_at)

    def test_import_leaves_locally_collected_snapshots_untouched(self):
        data = LocalData(self.temp.name, FakeEngine())
        data.db.save_snapshot(snapshot())
        with data.db._connect() as connection:
            before = connection.execute('SELECT * FROM snapshots').fetchall()
        history.import_file(data.db, self.fixture([row()]), 'src-1')
        with data.db._connect() as connection:
            after = connection.execute('SELECT * FROM snapshots').fetchall()
        self.assertEqual(before, after)
        self.assertTrue(before)
        self.assertEqual(len(data.db.get_reported_events()), 1)
        # And the backfill is not visible as a local capture.
        self.assertEqual(Observatory(data).coverage()['raw_events'], 1)

    def test_coverage_endpoint_serves_the_reconciliation(self):
        from types import SimpleNamespace
        import fuel_dashboard
        registered = {r.path: r.endpoint.__name__
                      for r in fuel_dashboard.app.routes if hasattr(r, 'endpoint')}
        self.assertEqual(registered['/api/observatory/coverage'], 'observatory_coverage')
        data = LocalData(self.temp.name, FakeEngine())
        history.import_file(data.db, self.fixture([row()]), 'src-1')
        state = SimpleNamespace(state=SimpleNamespace(observatory=Observatory(data)))
        payload = fuel_dashboard.observatory_coverage(SimpleNamespace(app=state))
        self.assertTrue(payload['retrospective'])
        self.assertEqual(payload['days'][0]['stations_reporting'], 1)

    def test_coverage_is_empty_and_honest_before_any_import(self):
        report = history.coverage(self.store)
        self.assertEqual((report['raw_events'], report['days'], report['sources']), (0, [], []))
        self.assertEqual(history.reported_series(self.store)['days'], [])


if __name__ == '__main__':
    unittest.main()
