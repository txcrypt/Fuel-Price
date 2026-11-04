"""Isolated, reproducible daily Brisbane price experiments; production is never changed.

Run from the repository root with the existing Python environment. Features are
backward-looking; a training label may enter a fit only once its target date has
arrived. The post-2026-09-28 slice is a retrospective temporal test, not a claim
that these algorithms were specified or forecasts saved in September.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from contextlib import nullcontext
from pathlib import Path

import numpy as np
import pandas as pd

HORIZONS = (1, 7, 14, 28)
TEST_START = pd.Timestamp("2026-09-29")
MODELS = ("persistence", "damped_trend", "ridge_history", "ridge_panel", "panel_analog")
PANEL_COLUMNS = ("mean", "q10", "q25", "q75", "q90", "hi20", "lo20",
                 "share_rising", "share_falling", "station_count")


def features(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str], list[str]]:
    """No interpolation; all shifts refer to calendar days after reindexing."""
    frame = frame.reindex(pd.date_range(frame.index.min(), frame.index.max(), freq="D"))
    y = frame["median"]
    x = pd.DataFrame(index=frame.index)
    x["level"] = y
    for k in (1, 2, 3, 7, 14, 28):
        x[f"change_{k}"] = y - y.shift(k)
    for k in (7, 14, 28):
        x[f"mean_gap_{k}"] = y - y.rolling(k, min_periods=k).mean()
        x[f"volatility_{k}"] = y.diff().rolling(k, min_periods=k).std()
    # Mean and quantiles are expressed relative to the contemporaneous median.
    history = list(x)
    for col in PANEL_COLUMNS:
        if col not in frame:
            continue
        x[col] = frame[col] - y if col in ("mean", "q10", "q25", "q75", "q90") else frame[col]
        if col != "station_count":
            x[f"{col}_change7"] = x[col] - x[col].shift(7)
    return x, history, list(x)


def upstream_features(path: Path, index: pd.DatetimeIndex, retail: pd.Series) -> pd.DataFrame:
    """Join publisher-date histories under explicit assumed publication lags, not vintages."""
    raw = pd.read_csv(path, parse_dates=["observed_date", "assumed_available_date"])
    out = pd.DataFrame(index=index)
    for source, group in raw.groupby("source"):
        group = group.sort_values(["assumed_available_date", "observed_date"])
        joined = pd.merge_asof(pd.DataFrame({"date": index}),
                              group[["assumed_available_date", "observed_date", "value"]],
                              left_on="date", right_on="assumed_available_date",
                              direction="backward")
        values = joined["value"].where((joined.date - joined.observed_date).dt.days <= 14)
        out[source] = values.to_numpy()
    result = pd.DataFrame(index=index)
    if "tgp" in out:
        result["retail_tgp_gap"] = retail - out.tgp
        for k in (1, 7, 14):
            result[f"tgp_change{k}"] = out.tgp - out.tgp.shift(k)
    if "audusd" in out:
        for source, conversion in (("brent", 100 / 158.987), ("gasoline_gulf", 100 / 3.785411784)):
            if source in out:
                converted = out[source] / out.audusd * conversion
                for k in (7, 14):
                    result[f"{source}_aud_change{k}"] = converted - converted.shift(k)
        result["fx_change7"] = out.audusd - out.audusd.shift(7)
    return result


def ridge_predict(train_x: np.ndarray, train_y: np.ndarray,
                  test_x: np.ndarray, alpha: float = 30.0) -> float:
    """Fixed ridge strength. Every mean/scale comes from this origin's training data."""
    centre = train_x.mean(axis=0)
    scale = train_x.std(axis=0)
    scale[scale < 1e-8] = 1.0
    a = (train_x - centre) / scale
    target_mean = train_y.mean()
    beta = np.linalg.solve(a.T @ a + alpha * np.eye(a.shape[1]), a.T @ (train_y - target_mean))
    return float(target_mean + ((test_x - centre) / scale) @ beta)


def analog_predict(train_x: np.ndarray, train_y: np.ndarray, test_x: np.ndarray) -> float:
    centre, scale = train_x.mean(axis=0), train_x.std(axis=0)
    scale[scale < 1e-8] = 1.0
    distance = np.mean(((train_x - test_x) / scale) ** 2, axis=1)
    nearest = np.argsort(distance, kind="stable")[:15]
    # Inverse-distance weighting, a fixed rule declared before examining the new test.
    weights = 1.0 / np.sqrt(distance[nearest] + 0.05)
    return float(np.average(train_y[nearest], weights=weights))


def matured_training_mask(index: pd.DatetimeIndex, origin: pd.Timestamp, horizon: int,
                          lookback_days: int = 365) -> np.ndarray:
    return ((index + pd.Timedelta(days=horizon) <= origin)
            & (index >= origin - pd.Timedelta(days=lookback_days)))


def run(frame: pd.DataFrame, min_training: int = 90, upstream: Path | None = None,
        nonlinear: bool = False, horizons: tuple[int, ...] = HORIZONS) -> pd.DataFrame:
    x, history_cols, panel_cols = features(frame)
    y = frame["median"].reindex(x.index)
    if upstream:
        context = upstream_features(upstream, x.index, y)
        x = x.join(context)
    rows = []
    if nonlinear:
        from sklearn.ensemble import HistGradientBoostingRegressor
    for horizon in horizons:
        delta = y.shift(-horizon) - y
        # Shared complete cases ensure all candidate comparisons use the same origins.
        valid = np.isfinite(x.to_numpy()).all(axis=1) & np.isfinite(delta)
        for origin in x.index[valid]:
            train = valid & matured_training_mask(x.index, origin, horizon)
            if train.sum() < min_training:
                continue
            current = float(y.loc[origin])
            past = y.loc[origin - pd.Timedelta(days=6):origin]
            slope = float(past.diff().median())
            predictions = {
                "persistence": current,
                "damped_trend": current + np.clip(slope, -4, 4) * sum(.8 ** k for k in range(horizon)),
                "ridge_history": current + ridge_predict(x.loc[train, history_cols].to_numpy(),
                    delta.loc[train].to_numpy(), x.loc[origin, history_cols].to_numpy()),
                "ridge_panel": current + ridge_predict(x.loc[train, panel_cols].to_numpy(),
                    delta.loc[train].to_numpy(), x.loc[origin, panel_cols].to_numpy()),
                "panel_analog": current + analog_predict(x.loc[train, panel_cols].to_numpy(),
                    delta.loc[train].to_numpy(), x.loc[origin, panel_cols].to_numpy()),
            }
            if upstream:
                predictions["ridge_panel_context"] = current + ridge_predict(x.loc[train].to_numpy(),
                    delta.loc[train].to_numpy(), x.loc[origin].to_numpy())
            if nonlinear:
                estimator = HistGradientBoostingRegressor(max_iter=100, max_leaf_nodes=7,
                    learning_rate=.05, l2_regularization=10, min_samples_leaf=20, random_state=20261104)
                estimator.fit(x.loc[train].to_numpy(), delta.loc[train].to_numpy())
                predictions["boosted_panel_context"] = current + float(estimator.predict(
                    x.loc[[origin]].to_numpy())[0])
            for model, prediction in predictions.items():
                rows.append({"origin": origin, "target_date": origin + pd.Timedelta(days=horizon),
                             "horizon": horizon, "model": model, "current": current,
                             "actual": float(y.loc[origin + pd.Timedelta(days=horizon)]),
                             "prediction": float(prediction), "training_rows": int(train.sum())})
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["error"] = out.prediction - out.actual
    out["absolute_error"] = out.error.abs()
    out["period"] = np.select([out.origin >= TEST_START, out.target_date < TEST_START],
                               ["fresh_temporal", "development"], default="embargo")
    out["year"] = out.origin.dt.year
    return add_prequential_intervals(out)


def risk_predictions(frame: pd.DataFrame, horizons: tuple[int, ...] = HORIZONS) -> pd.DataFrame:
    """Forecast probability of endpoint median being >=10 c/L above origin.

    Ridge linear probability is intentionally modest; calibration/Brier performance
    is measured against a trailing historical-frequency baseline, not asserted.
    This event does not mean the first station restoration or any intrahorizon peak.
    """
    x, _, panel_cols = features(frame)
    y = frame["median"].reindex(x.index)
    rows = []
    for horizon in horizons:
        future = y.shift(-horizon)
        label = (future - y >= 10).astype(float).where(future.notna())
        valid = np.isfinite(x.to_numpy()).all(axis=1) & label.notna()
        for origin in x.index[valid]:
            train = valid & matured_training_mask(x.index, origin, horizon)
            if train.sum() < 90:
                continue
            probabilities = {"historical_frequency": float(label.loc[train].mean()),
                "ridge_panel_risk": float(np.clip(ridge_predict(x.loc[train, panel_cols].to_numpy(),
                    label.loc[train].to_numpy(), x.loc[origin, panel_cols].to_numpy()), 0, 1))}
            for model, probability in probabilities.items():
                rows.append({"origin": origin, "horizon": horizon, "model": model,
                    "probability": probability, "event": int(label.loc[origin]),
                    "period": ("fresh_temporal" if origin >= TEST_START else "development"
                               if origin + pd.Timedelta(days=horizon) < TEST_START else "embargo")})
    return pd.DataFrame(rows)


def add_prequential_intervals(predictions: pd.DataFrame) -> pd.DataFrame:
    """Symmetric residual quantiles learned only from targets known by each origin.

    The name is deliberately descriptive: serial dependence and regime shifts mean
    these empirical intervals have no distribution-free coverage guarantee.
    """
    out = predictions.copy()
    for col in ("radius80", "radius95", "interval_calibration_n"):
        out[col] = np.nan
    for (_, horizon), group in out.groupby(["model", "horizon"]):
        for row_id, row in group.iterrows():
            prior = group.loc[(group.target_date <= row.origin)
                              & (group.origin >= row.origin - pd.Timedelta(days=180))]
            if len(prior) >= 60:
                out.loc[row_id, "radius80"] = prior.absolute_error.quantile(.8, interpolation="higher")
                out.loc[row_id, "radius95"] = prior.absolute_error.quantile(.95, interpolation="higher")
                out.loc[row_id, "interval_calibration_n"] = len(prior)
    return out


def block_ci(values: np.ndarray, horizon: int, dates: pd.DatetimeIndex,
             draws: int = 2000) -> tuple[float | None, float | None]:
    """Calendar-contiguous moving blocks; refuse tiny or gappy test sets."""
    block = max(14, horizon)
    n = len(values)
    if n < 4 * block or not np.all(np.diff(dates.values).astype("timedelta64[D]").astype(int) == 1):
        return None, None
    rng = np.random.default_rng(20261104 + horizon)
    starts = rng.integers(0, n - block + 1, size=(draws, int(np.ceil(n / block))))
    indices = (starts[..., None] + np.arange(block)).reshape(draws, -1)[:, :n]
    low, high = np.quantile(values[indices].mean(axis=1), [.025, .975])
    return float(low), float(high)


def summarize(predictions: pd.DataFrame) -> pd.DataFrame:
    summaries = []
    for (period, horizon), section in predictions.groupby(["period", "horizon"]):
        baseline = section.loc[section.model.eq("persistence")].set_index("origin")
        baseline_mae = baseline.absolute_error.mean()
        for model, group in section.groupby("model"):
            ordered = group.sort_values("origin").set_index("origin")
            diff = ordered.absolute_error - baseline.absolute_error
            ci = block_ci(diff.to_numpy(), int(horizon), ordered.index)
            intervals = ordered.dropna(subset=["radius80"])
            direction_actual = np.sign(ordered.actual - ordered.current)
            direction_forecast = np.sign(ordered.prediction - ordered.current)
            summaries.append({"period": period, "horizon": int(horizon), "model": model,
                "n": len(group), "first_origin": str(ordered.index.min().date()),
                "last_origin": str(ordered.index.max().date()),
                "mae": float(ordered.absolute_error.mean()),
                "rmse": float(np.sqrt((ordered.error ** 2).mean())),
                "bias": float(ordered.error.mean()),
                "skill_vs_persistence": (float(1 - ordered.absolute_error.mean() / baseline_mae)
                                         if baseline_mae > 0 else None),
                "mae_difference_ci95_low": ci[0], "mae_difference_ci95_high": ci[1],
                "direction_accuracy_including_flat": float((direction_actual == direction_forecast).mean()),
                "large_rise_events_ge10": int((ordered.actual - ordered.current >= 10).sum()),
                "interval_n": len(intervals),
                "coverage80": float((intervals.absolute_error <= intervals.radius80).mean()) if len(intervals) else None,
                "coverage95": float((intervals.absolute_error <= intervals.radius95).mean()) if len(intervals) else None,
                "mean_width80": float(2 * intervals.radius80.mean()) if len(intervals) else None,
                "mean_width95": float(2 * intervals.radius95.mean()) if len(intervals) else None})
    return pd.DataFrame(summaries)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path(".local/research-2026-11-04/models"))
    parser.add_argument("--upstream", type=Path)
    parser.add_argument("--nonlinear", action="store_true", help="Requires scikit-learn; fixed HGB settings")
    parser.add_argument("--target", choices=["median", "mean"], default="median")
    parser.add_argument("--horizons", type=int, nargs="+", default=HORIZONS)
    args = parser.parse_args()
    frame = pd.read_csv(args.daily, parse_dates=["date"]).set_index("date").sort_index()
    frame = frame.rename(columns={"price_cpl": "median", "mean_cpl": "mean", "stations": "station_count",
                                  "hiked5_share": "share_rising", "cut5_share": "share_falling"})
    if args.target == "mean":
        frame["median"] = frame["mean"]
    if frame.index.has_duplicates:
        raise ValueError("Daily input has duplicate dates")
    # Load OpenMP before constructing threadpool_limits; otherwise its DLL would
    # appear only after the controller took its library inventory.
    if args.nonlinear:
        from sklearn.ensemble import HistGradientBoostingRegressor  # noqa: F401
    try:
        from threadpoolctl import threadpool_limits
        thread_context = threadpool_limits(limits=1)
    except ImportError:
        thread_context = nullcontext()
    input_hash = hashlib.sha256(args.daily.read_bytes()).hexdigest()
    with thread_context:
        predictions = run(frame, upstream=args.upstream, nonlinear=args.nonlinear, horizons=tuple(args.horizons))
        risks = risk_predictions(frame, horizons=tuple(args.horizons))
    args.output.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(args.output / "predictions.csv", index=False)
    summary = summarize(predictions)
    summary.to_csv(args.output / "metrics.csv", index=False)
    regime = predictions.loc[predictions.period.eq("development")].copy()
    regime["period"] = "development_" + regime.origin.dt.year.astype(str)
    summarize(regime).to_csv(args.output / "regime_metrics.csv", index=False)
    selected = summary.loc[summary.period.eq("development")].sort_values(["horizon", "mae", "model"]).groupby(
        "horizon", as_index=False).first()[["horizon", "model", "mae"]].rename(columns={"mae": "development_mae"})
    selected.merge(summary.loc[summary.period.eq("fresh_temporal"), ["horizon", "model", "mae", "n"]],
        on=["horizon", "model"]).to_csv(args.output / "development_selection.csv", index=False)
    risks.to_csv(args.output / "risk_predictions.csv", index=False)
    risks["brier"] = (risks.probability - risks.event) ** 2
    risks.groupby(["period", "horizon", "model"]).agg(n=("event", "size"), events=("event", "sum"),
        brier=("brier", "mean"), average_probability=("probability", "mean")).reset_index().to_csv(
            args.output / "risk_metrics.csv", index=False)
    manifest = {"input": str(args.daily.resolve()), "input_sha256": input_hash,
                "target": f"Brisbane U91 equal-weight station {args.target}, c/L",
                "horizons": args.horizons, "test_start": str(TEST_START.date()),
                "models": list(predictions.model.unique()), "ridge_alpha": 30, "training_lookback_days": 365,
                "minimum_training_rows": 90, "analog_neighbors": 15,
                "known_limitations": ["New test is retrospective, not a saved forward forecast",
                    "Monthly archive release times and later corrections are not reconstructed vintages",
                    "Forecast origin is after Brisbane day end; today's partial day is not equivalent",
                    "Fixed settings are not optimised on the final temporal slice",
                    "Fresh horizon sample sizes vary; do not average unlike test dates"]}
    packages = ["numpy", "pandas"] + (["scikit-learn", "scipy", "threadpoolctl"] if args.nonlinear else [])
    manifest["versions"] = {name: importlib.metadata.version(name) for name in packages}
    manifest["upstream_input_sha256"] = (hashlib.sha256(args.upstream.read_bytes()).hexdigest()
                                        if args.upstream else None)
    manifest["boosting_parameters"] = (dict(max_iter=100, max_leaf_nodes=7, learning_rate=.05,
        l2_regularization=10, min_samples_leaf=20, random_state=20261104) if args.nonlinear else None)
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
