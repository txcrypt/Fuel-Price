"""Safety properties for the isolated research harness, not production model tests."""
import unittest
from io import StringIO
from contextlib import nullcontext

import numpy as np
import pandas as pd

from research.forecast_experiments import add_prequential_intervals, features, matured_training_mask, run, summarize, upstream_features


class ResearchLeakageTests(unittest.TestCase):
    def test_training_labels_must_have_arrived_at_origin(self):
        dates = pd.date_range("2026-01-01", periods=100)
        origin = dates[75]
        mask = matured_training_mask(dates, origin, 28)
        self.assertEqual(dates[mask].max(), origin - pd.Timedelta(days=28))

    def test_future_prices_cannot_change_earlier_features_or_forecasts(self):
        dates = pd.date_range("2025-01-01", periods=135)
        frame = pd.DataFrame({"median": 180 + np.sin(np.arange(135) / 8) * 10}, index=dates)
        changed = frame.copy()
        origin = dates[100]
        changed.loc[changed.index > origin, "median"] += 100
        x, _, _ = features(frame)
        other, _, _ = features(changed)
        pd.testing.assert_frame_equal(x.loc[:origin], other.loc[:origin])
        try:
            from threadpoolctl import threadpool_limits
            thread_context = threadpool_limits(limits=1)
        except ImportError:
            thread_context = nullcontext()
        with thread_context:
            first = run(frame, min_training=30)
            second = run(changed, min_training=30)
        first = first.loc[first.origin.eq(origin), ["model", "horizon", "prediction"]].reset_index(drop=True)
        second = second.loc[second.origin.eq(origin), ["model", "horizon", "prediction"]].reset_index(drop=True)
        self.assertEqual(len(first), 20)
        pd.testing.assert_frame_equal(first, second)

    def test_intervals_do_not_use_unmatured_errors(self):
        dates = pd.date_range("2026-01-01", periods=100)
        data = pd.DataFrame({"origin": dates, "target_date": dates + pd.Timedelta(days=28),
                             "horizon": 28, "model": "test", "absolute_error": 1.0})
        data.loc[data.index >= 70, "absolute_error"] = 1000
        result = add_prequential_intervals(data)
        self.assertTrue(result.radius80.iloc[:87].isna().all())
        self.assertEqual(result.radius80.iloc[95], 1.0)

    def test_upstream_values_wait_until_assumed_publication_and_expire(self):
        dates = pd.date_range("2026-01-01", periods=35)
        raw = pd.DataFrame({"observed_date": ["2026-01-01", "2026-01-08"],
                            "assumed_available_date": ["2026-01-03", "2026-01-10"],
                            "source": ["tgp", "tgp"], "value": [170.0, 180.0]})
        x = upstream_features(StringIO(raw.to_csv(index=False)), dates, pd.Series(200.0, index=dates))
        self.assertTrue(pd.isna(x.loc["2026-01-02", "retail_tgp_gap"]))
        self.assertEqual(x.loc["2026-01-09", "retail_tgp_gap"], 30)
        self.assertEqual(x.loc["2026-01-10", "retail_tgp_gap"], 20)
        self.assertTrue(pd.isna(x.loc["2026-01-23", "retail_tgp_gap"]))

    def test_perfect_baseline_has_undefined_relative_skill(self):
        dates = pd.date_range("2026-01-01", periods=3)
        rows = pd.DataFrame({"origin": dates, "target_date": dates + pd.Timedelta(days=1),
            "horizon": 1, "model": "persistence", "current": 200., "prediction": 200.,
            "actual": 200., "error": 0., "absolute_error": 0., "period": "test"})
        result = summarize(add_prequential_intervals(rows))
        self.assertTrue(pd.isna(result.skill_vs_persistence.iloc[0]))


if __name__ == "__main__":
    unittest.main()
