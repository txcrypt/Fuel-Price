from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock
from datetime import datetime, timedelta
import pandas as pd
import fuel_security as fs


class PortOfBrisbaneTests(TestCase):
    def test_refined_oil_is_read_from_each_block(self):
        rows = [['Total Trade in Tonnes', None, None, None, None],
                [None, ' ', None, 'Arr 2026/Jul', 'Arr 2026/Aug'],
                [None, 'Import ', 'Crude Oil', 459097.5, 268228.6],
                [None, None, 'Refined Oil', 374816.7, 442195.3],
                [None, 'Export ', 'Coal', 1, 2],
                [None, None, 'Refined Oil', 104017.2, 112470.8]]
        with TemporaryDirectory() as d:
            path = Path(d) / 'p.xlsx'
            pd.DataFrame(rows).to_excel(path, header=False, index=False)
            out = fs.parse_pbpl(path)
        self.assertEqual(out['months'], ['2026-07', '2026-08'])
        self.assertEqual((out['crude_import_t'][-1], out['refined_import_t'][-1], out['refined_export_t'][-1]),
                         (268228.6, 442195.3, 112470.8))


class RefreshTests(TestCase):
    def test_checks_twice_a_day_and_keeps_last_file_on_failure(self):
        with TemporaryDirectory() as d:
            security = fs.FuelSecurity(d)
            security.SOURCES = {'pbpl': (lambda session: 'https://example/x-august-2026.xlsx', 'pbpl-monthly.xlsx')}
            session = Mock(get=Mock(return_value=Mock(content=b'xlsx-bytes', raise_for_status=Mock())))
            now = datetime(2026, 10, 1, 9)
            security.refresh(now, session)
            self.assertEqual((Path(d) / 'pbpl-monthly.xlsx').read_bytes(), b'xlsx-bytes')
            security.refresh(now + timedelta(hours=1), session)          # inside 12 h: no request at all
            self.assertEqual(session.get.call_count, 1)
            session.get.side_effect = ConnectionError('down')
            security.SOURCES = {'pbpl': (lambda session: 'https://example/x-september-2026.xlsx', 'pbpl-monthly.xlsx')}
            security.refresh(now + timedelta(hours=13), session)
            self.assertEqual((Path(d) / 'pbpl-monthly.xlsx').read_bytes(), b'xlsx-bytes')   # last file kept
            self.assertIn('could not refresh', security.payload()['state']['pbpl']['error'])
