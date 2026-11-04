"""Announcement timing matters even where an effective date is retrospective."""
import unittest
from unittest.mock import patch

import pandas as pd

from research import excise_experiments as excise


class ResearchExciseTimingTests(unittest.TestCase):
    def test_april_extra_relief_cannot_enter_the_march_forecast(self):
        target = pd.Timestamp('2026-04-07')
        self.assertAlmostEqual(excise.tax_path(target, pd.Timestamp('2026-03-30')), 52.6)
        self.assertAlmostEqual(excise.tax_path(target, pd.Timestamp('2026-03-31')), 26.3)
        self.assertAlmostEqual(excise.tax_path(target, pd.Timestamp('2026-04-03')), 20.6)

    def test_july_extension_only_supersedes_reversion_after_announcement(self):
        target = pd.Timestamp('2026-07-07')
        self.assertAlmostEqual(excise.tax_path(target, pd.Timestamp('2026-06-21')), 52.6)
        self.assertAlmostEqual(excise.tax_path(target, pd.Timestamp('2026-06-22')), 36.6)

    def test_future_schedule_revisions_cannot_change_a_past_forecast(self):
        origin, target = pd.Timestamp('2026-03-31'), pd.Timestamp('2026-04-07')
        before = excise.tax_path(target, origin)
        with patch.object(excise, 'SCHEDULE', excise.SCHEDULE + [('2026-04-01', '2026-04-10', 999)]):
            self.assertEqual(excise.tax_path(target, origin), before)


if __name__ == '__main__':
    unittest.main()
