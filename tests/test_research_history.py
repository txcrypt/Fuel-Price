"""Guard the historical timestamp difference that otherwise drops 2023 reports."""
from pathlib import Path
import sys
import unittest

import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'research'))
from extend_history import parse_historical_at


class HistoricalTimestampTests(unittest.TestCase):
    def test_iso_and_day_first_publisher_formats_preserve_instants(self):
        raw=pd.Series(['2023-01-06T23:45:00','06/01/2023 23:45',
                       '2023-01-06T23:45:00.123','06/01/2023 23:45:00'])
        actual=parse_historical_at(raw)
        expected=pd.to_datetime(['2023-01-06T23:45:00Z','2023-01-06T23:45:00Z',
                                 '2023-01-06T23:45:00.123Z','2023-01-06T23:45:00Z'],format='ISO8601',utc=True)
        self.assertEqual(actual.tolist(),expected.tolist())
        self.assertEqual(actual.dt.tz_convert('Australia/Brisbane').dt.date.astype(str).tolist(),['2023-01-07']*4)

    def test_unrecognised_nonempty_timestamp_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'Unparsed historical report timestamps'):
            parse_historical_at(pd.Series(['2023-01-06T23:45:00','01-06-2023 23:45']))


if __name__=='__main__':unittest.main()
