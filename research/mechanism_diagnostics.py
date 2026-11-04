"""Derived diagnostics for the fixed mechanism run; does not refit models."""
from pathlib import Path
import argparse
import json

import numpy as np
import pandas as pd


def paired_blocks(values, dates, block, draws=2000):
    values = np.asarray(values)
    if len(values) < 4 * block or not np.all(np.diff(dates.values).astype("timedelta64[D]").astype(int) == 1):
        return None, None
    rng = np.random.default_rng(20261104 + block)
    starts = rng.integers(0, len(values) - block + 1, size=(draws, int(np.ceil(len(values) / block))))
    indices = (starts[..., None] + np.arange(block)).reshape(draws, -1)[:, :len(values)]
    return tuple(float(x) for x in np.quantile(values[indices].mean(axis=1), [.025, .975]))


def diagnostics(output):
    predictions = pd.read_csv(output / "predictions.csv", parse_dates=["origin", "target_date"])
    state = pd.read_csv(output / "state_features.csv", parse_dates=["date"]).set_index("date")
    slices = {"all_diagnostic": predictions}
    for year, group in predictions.groupby("year"):
        slices[f"year_{year}"] = group.loc[group.target_date.dt.year.eq(year)]
    intervals = []
    for period, section in slices.items():
        for horizon, group in section.groupby("horizon"):
            baseline = group.loc[group.model.eq("persistence")].set_index("origin").absolute_error
            for model, forecasts in group.groupby("model"):
                if model == "persistence":
                    continue
                paired = forecasts.sort_values("origin").set_index("origin")
                difference = paired.absolute_error - baseline
                for block in (56, 84):
                    low, high = paired_blocks(difference.to_numpy(), paired.index, block)
                    intervals.append({"period": period, "horizon": horizon, "model": model,
                        "n": len(paired), "block_days": block, "mae_difference": difference.mean(),
                        "ci95_low": low, "ci95_high": high})
    pd.DataFrame(intervals).to_csv(output / "block_uncertainty.csv", index=False)

    event_rows, component_rows = [], []
    for _, row in predictions.loc[predictions.model.eq("diffusion_hurdle")].iterrows():
        params = json.loads(row.parameters)
        margin_change = float(state.loc[row.target_date, "margin"] - state.loc[row.origin, "margin"])
        actual = int(margin_change >= 10)
        probability = params["margin_expansion_probability"]
        base_probability = params["positive_training_labels"] / params["training_n"]
        event_rows.append({"origin": row.origin, "target_date": row.target_date, "year": row.year,
            "horizon": row.horizon, "event": actual, "probability": probability,
            "called": params["jump_called"], "brier": (probability - actual) ** 2,
            "frequency_brier": (base_probability - actual) ** 2,
            "true_positive": int(params["jump_called"] and actual),
            "false_positive": int(params["jump_called"] and not actual),
            "false_negative": int(not params["jump_called"] and actual)})
    events = pd.DataFrame(event_rows)
    events.to_csv(output / "event_predictions.csv", index=False)
    tables = []
    for period, chosen in [("all_diagnostic", events)] + [(f"year_{year}", group.loc[group.target_date.dt.year.eq(year)])
                                                         for year, group in events.groupby("year")]:
        for horizon, group in chosen.groupby("horizon"):
            tp, fp, fn = group.true_positive.sum(), group.false_positive.sum(), group.false_negative.sum()
            tables.append({"period": period, "horizon": horizon, "n": len(group),
                "events": int(group.event.sum()), "calls": int(group.called.sum()),
                "precision": float(tp / (tp + fp)) if tp + fp else None,
                "recall": float(tp / (tp + fn)) if tp + fn else None,
                "brier": group.brier.mean(), "historical_frequency_brier": group.frequency_brier.mean()})
    pd.DataFrame(tables).to_csv(output / "event_metrics.csv", index=False)

    for _, row in predictions.loc[predictions.model.eq("mechanism_combined")].iterrows():
        params = json.loads(row.parameters)
        actual_cost = float(state.loc[row.target_date, "tgp"] - state.loc[row.origin, "tgp"])
        actual_margin = float(state.loc[row.target_date, "margin"] - state.loc[row.origin, "margin"])
        component_rows.append({"origin": row.origin, "horizon": row.horizon,
            "cost_delta": params["cost_delta"], "actual_cost_delta": actual_cost,
            "margin_delta": params["margin_delta"], "actual_margin_delta": actual_margin,
            "cost_absolute_error": abs(params["cost_delta"] - actual_cost),
            "cost_zero_change_error": abs(actual_cost),
            "margin_absolute_error": abs(params["margin_delta"] - actual_margin),
            "margin_zero_change_error": abs(actual_margin)})
    components = pd.DataFrame(component_rows)
    components.to_csv(output / "component_predictions.csv", index=False)
    components.groupby("horizon").agg(n=("origin", "size"),
        cost_mae=("cost_absolute_error", "mean"), cost_zero_change_mae=("cost_zero_change_error", "mean"),
        margin_mae=("margin_absolute_error", "mean"), margin_zero_change_mae=("margin_zero_change_error", "mean")
        ).reset_index().to_csv(output / "component_metrics.csv", index=False)
    tails = predictions.groupby(["horizon", "model"]).absolute_error.agg(
        median_absolute_error="median", p90_absolute_error=lambda x: x.quantile(.9),
        p95_absolute_error=lambda x: x.quantile(.95), max_absolute_error="max",
        severe_error_ge10_rate=lambda x: x.ge(10).mean()).reset_index()
    tails.to_csv(output / "tail_metrics.csv", index=False)
    print(pd.DataFrame(intervals).query("period == 'all_diagnostic'").to_string(index=False))
    print(pd.DataFrame(tables).query("period == 'all_diagnostic'").to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(".local/research-2026-11-04/mechanisms"))
    diagnostics(parser.parse_args().output)
