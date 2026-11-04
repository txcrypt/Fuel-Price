import unittest
import json

import numpy as np
import pandas as pd

from research.mechanism_models import (available_upstream, nonnegative_change_ridge,
    predict_candidates, select_from_matured_losses)


def synthetic_inputs():
    dates = pd.date_range("2023-01-01", periods=230)
    t = np.arange(len(dates))
    wholesale = 170 + np.sin(t / 20)
    retail = wholesale + 15 + 8 * np.sin(t / 7)
    frame = pd.DataFrame({"median": retail, "hi20": np.maximum(0, .2 * np.sin(t / 7)),
                          "share_rising": np.maximum(0, .3 * np.cos(t / 7)),
                          "q90": retail + 20 + 10 * np.sin(t / 7)}, index=dates)
    upstream = pd.concat([pd.DataFrame({"source": source, "observed_date": dates,
        "known_at": dates.tz_localize("Australia/Brisbane").tz_convert("UTC"), "value": value})
        for source, value in [("tgp", wholesale), ("gasoline_gulf", 2 + .1 * np.sin(t / 10)),
                              ("audusd", np.repeat(.7, len(t)))]], ignore_index=True)
    return frame, upstream


class MechanismTimingTests(unittest.TestCase):
    def test_vintage_revisions_and_future_observations_wait(self):
        raw = pd.DataFrame({"source": ["aip"] * 4,
            "observed_date": ["2026-01-01", "2026-01-01", "2026-01-03", "2026-01-02"],
            "value": [170, 190, 200, 180],
            "known_at": ["2026-01-01T00:00:00Z", "2026-01-03T00:00:00Z",
                         "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"]})
        joined = available_upstream(raw, pd.date_range("2026-01-01", periods=4))
        self.assertEqual(joined.tgp.tolist(), [170, 180, 200, 200])

    def test_unknown_publication_timing_is_not_silently_invented(self):
        raw = pd.DataFrame({"source": ["tgp"], "observed_date": ["2026-01-01"], "value": [170]})
        with self.assertRaises(ValueError):
            available_upstream(raw, pd.date_range("2026-01-01", periods=2))

    def test_legacy_naive_known_at_is_brisbane_local(self):
        raw = pd.DataFrame({"source": ["aip"], "observed_date": ["2026-01-02"],
                            "value": [180], "known_at": ["2026-01-02T20:00:00"]})
        joined = available_upstream(raw, pd.date_range("2026-01-01", periods=3))
        self.assertTrue(pd.isna(joined.tgp.iloc[0]))
        self.assertEqual(joined.tgp.iloc[1], 180)

    def test_zero_cost_changes_anchor_exactly_to_persistence(self):
        rng = np.random.default_rng(8)
        x = rng.normal(size=(150, 3))
        change, coefficient = nonnegative_change_ridge(x, x[:, 0] - x[:, 1], np.zeros(3), np.arange(150))
        self.assertEqual(change, 0.)
        self.assertTrue(np.all(np.array(coefficient) >= 0))

    def test_forecasts_ignore_future_prices_and_late_revisions(self):
        frame, upstream = synthetic_inputs()
        origin = frame.index[200]
        changed = frame.copy()
        changed.loc[changed.index > origin, "median"] += 100
        revision = upstream.iloc[[0]].copy()
        revision["value"] = 999.
        revision["known_at"] = pd.Timestamp(origin + pd.Timedelta(days=1), tz="UTC")
        revised = pd.concat([upstream, revision], ignore_index=True)
        for horizon in (7, 28):
            first = predict_candidates(frame, upstream, origin, horizon)
            second = predict_candidates(changed, revised, origin, horizon)
            self.assertEqual(set(first), set(second))
            self.assertIn("mechanism_combined", first)
            for name in first:
                self.assertEqual(first[name]["prediction"], second[name]["prediction"])
            combined = first["mechanism_combined"]
            self.assertAlmostEqual(combined["prediction"] - frame.loc[origin, "median"],
                                   combined["state"]["cost_delta"] + combined["state"]["margin_delta"])

    def test_online_selector_uses_only_matured_and_already_scored_losses(self):
        origin, horizon = pd.Timestamp("2026-07-01"), 7
        candidates = {"persistence": {"prediction": 200.}, "cost_only": {"prediction": 205.}}
        history = [{"origin_date": day, "horizon": horizon, "model": model, "absolute_error_cpl": loss}
            for day in pd.date_range("2026-01-01", "2026-06-30")
            for model, loss in [("persistence", 10.), ("cost_only", 1.)]]
        selected, _ = select_from_matured_losses(candidates, history, origin, horizon)
        self.assertEqual(selected, "cost_only")
        for row in history:
            if row["origin_date"] + pd.Timedelta(days=horizon) > origin:
                row["absolute_error_cpl"] = 10000. if row["model"] == "cost_only" else 0.
        self.assertEqual(select_from_matured_losses(candidates, history, origin, horizon)[0], "cost_only")
        late = [{**row, "scored_at_utc": "2026-07-02T00:00:00Z"} for row in history]
        self.assertEqual(select_from_matured_losses(candidates, late, origin, horizon)[0], "persistence")

    def test_retail_ablation_can_run_without_wholesale_vintages(self):
        frame, _ = synthetic_inputs()
        results = predict_candidates(frame, pd.DataFrame(), frame.index[200], 7)
        self.assertIn("retail_diffusion_hurdle", results)
        self.assertNotIn("cost_only", results)
        self.assertNotIn("mechanism_combined", results)
        self.assertEqual(results["online_selector"]["state"]["selected_model"], "persistence")
        self.assertIn("cost_only", results["online_selector"]["state"]["excluded_candidates"])
        json.dumps(results, allow_nan=False)

    def test_retail_ablation_does_not_enter_frozen_original_selector(self):
        origin = pd.Timestamp("2026-07-01")
        candidates = {"persistence": {"prediction": 200.}, "retail_diffusion_hurdle": {"prediction": 205.}}
        history = [{"origin_date": day, "horizon": 7, "model": model, "absolute_error_cpl": loss}
            for day in pd.date_range("2026-01-01", "2026-06-30")
            for model, loss in [("persistence", 10.), ("retail_diffusion_hurdle", 0.)]]
        self.assertEqual(select_from_matured_losses(candidates, history, origin, 7)[0], "persistence")


if __name__ == "__main__":
    unittest.main()
