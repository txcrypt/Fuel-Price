"""Avoid treating a weak instrument or a weekend date as precise identification."""
import unittest

import numpy as np
import pandas as pd

from research.oil_shock_analysis import build_events, quadratic_set, weak_iv_set


class OilShockIdentificationTests(unittest.TestCase):
    def test_confidence_sets_can_be_disjoint_or_unbounded(self):
        self.assertEqual(quadratic_set(1, 0, -1), {'kind': 'bounded', 'low': -1, 'high': 1})
        self.assertEqual(quadratic_set(-1, 0, 1), {'kind': 'outside_interval', 'low': -1, 'high': 1})
        self.assertEqual(quadratic_set(-1, 0, -1), {'kind': 'all_real'})

    def test_an_irrelevant_instrument_cannot_produce_a_bounded_confidence_set(self):
        n = 100
        x = np.linspace(-2, 2, n)
        controls = np.ones((n, 1))
        z = np.sin(np.arange(n))
        projection = np.column_stack([np.ones(n), x])
        z -= projection @ np.linalg.lstsq(projection, z, rcond=None)[0]
        y = 2*x + z
        result = weak_iv_set(y, x, controls, z, pd.date_range('2020-01-01', periods=n, freq='7D'), 14)
        self.assertLess(result['first_stage_robust_f'], 1e-12)
        self.assertNotEqual(result['approximate_ar95']['kind'], 'bounded')

    def test_sunday_announcement_uses_first_trading_quote_and_next_brisbane_day(self):
        dates = pd.date_range('2023-12-01', '2024-01-31')
        daily = pd.DataFrame({'price_cpl': np.arange(len(dates)) + 180.0}, index=dates)
        raw = pd.DataFrame({'observed_date': pd.to_datetime(['2023-12-29', '2024-01-05', '2024-01-08', '2024-01-09']),
                            'value': [75.0, 80.0, 84.0, 90.0], 'source': 'brent'})
        shocks = pd.DataFrame({'Date/Contract': [pd.Timestamp('2024-01-07')], 'PC': [1.0], 'Front': [1.0]})
        result = build_events(daily, raw, shocks)
        self.assertEqual(result.anchor.iloc[0], pd.Timestamp('2024-01-09'))
        self.assertEqual(result.baseline.iloc[0], pd.Timestamp('2024-01-06'))
        self.assertAlmostEqual(result.brent_response_pct.iloc[0], 100*np.log(84/80))
        raw.loc[raw.observed_date.eq('2024-01-09'), 'value'] = 10000
        changed = build_events(daily, raw, shocks)
        self.assertEqual(result.brent_response_pct.iloc[0], changed.brent_response_pct.iloc[0])


if __name__ == '__main__':
    unittest.main()
