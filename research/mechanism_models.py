"""Mechanism-separated diagnostic forecasts and a point-in-time shadow-trial callback.

All previously inspected dates, including September/October 2026, are diagnostic.
Models separate future published-wholesale changes from retail-minus-wholesale
margin changes. This accounting decomposition does not identify causal effects.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path

import numpy as np
import pandas as pd

HORIZONS = (1, 7, 14, 28)
MODELS = ("persistence", "damped_trend", "cost_only", "diffusion_hurdle", "mechanism_combined", "online_selector")
MIN_TRAINING = 120
COST_COLUMNS = ["tgp_change3", "tgp_change7", "gasoline_aud_change7"]
EVENT_COLUMNS = ["hi20", "hi20_change3", "hike_breadth3", "upper_gap", "margin_change3"]
RETAIL_EVENT_COLUMNS = ["hi20", "hi20_change3", "hike_breadth3", "upper_gap", "retail_change3"]
SOURCE_MAP = {"aip": "tgp", "rba": "audusd", "eia_brent": "brent", "eia_gasoline": "gasoline_gulf"}


def normalize_daily(frame):
    out = frame.copy().rename(columns={"price_cpl": "median", "mean_cpl": "mean",
        "hiked5_share": "share_rising", "cut5_share": "share_falling", "stations": "station_count"})
    if "date" in out:
        out = out.set_index("date")
    out.index = pd.to_datetime(out.index).normalize()
    if out.index.has_duplicates:
        raise ValueError("Daily dates must be unique")
    return out.sort_index().reindex(pd.date_range(out.index.min(), out.index.max(), freq="D"))


def available_upstream(upstream, index):
    """Respect both observation time and actual known-at time, including revisions.

    Research downloads can supply assumed_available_date explicitly. The callback
    never silently manufactures one for a raw observation lacking known_at.
    """
    result = pd.DataFrame(index=index)
    if upstream.empty:
        return result
    raw = upstream.copy().rename(columns={"source_id": "source"})
    required = {"source", "observed_date", "value"}
    if not required.issubset(raw):
        raise ValueError("Upstream needs source, observed_date and value")
    if "known_at" in raw:
        # Existing collector rows use naive Brisbane local timestamps; aware
        # timestamps retain their actual offset. Never silently reinterpret local
        # 20:00 as UTC and move availability to the following Brisbane date.
        known = [pd.Timestamp(value) for value in raw.known_at]
        known = pd.to_datetime([(value.tz_localize("Australia/Brisbane") if value.tzinfo is None
                                else value).tz_convert("UTC") for value in known], utc=True)
        raw["known_sort"] = pd.Series(known, index=raw.index)
        raw["known_date"] = raw.known_sort.dt.tz_convert(
            "Australia/Brisbane").dt.tz_localize(None).dt.normalize()
    elif "assumed_available_date" in raw:
        raw["known_date"] = pd.to_datetime(raw.assumed_available_date).dt.normalize()
        raw["known_sort"] = raw.known_date
    else:
        raise ValueError("Upstream observations require actual known_at or explicit assumed_available_date")
    raw["observed_date"] = pd.to_datetime(raw.observed_date).dt.tz_localize(None).dt.normalize()
    raw["source"] = raw.source.replace(SOURCE_MAP)
    for source, group in raw.groupby("source"):
        records = list(group.sort_values("known_sort").itertuples(index=False))
        position, values, output = 0, {}, []
        for day in index:
            while position < len(records) and records[position].known_date <= day:
                row = records[position]
                values[row.observed_date] = float(row.value)
                position += 1
            eligible = [d for d in values if d <= day]
            latest = max(eligible) if eligible else None
            output.append(values[latest] if latest is not None and (day - latest).days <= 14 else np.nan)
        result[str(source)] = output
    return result


def prepare_state(daily, upstream):
    frame = normalize_daily(daily)
    external = available_upstream(upstream, frame.index)
    state = pd.DataFrame(index=frame.index)
    state["retail"] = frame["median"]
    state["tgp"] = external.get("tgp", pd.Series(np.nan, index=frame.index))
    state["margin"] = state.retail - state.tgp
    for window in (3, 7):
        state[f"tgp_change{window}"] = state.tgp - state.tgp.shift(window)
    if {"gasoline_gulf", "audusd"}.issubset(external):
        gasoline = external.gasoline_gulf / external.audusd * (100 / 3.785411784)
        state["gasoline_aud_change7"] = gasoline - gasoline.shift(7)
    else:
        state["gasoline_aud_change7"] = np.nan
    state["hi20"] = frame.get("hi20", np.nan)
    state["hi20_change3"] = state.hi20 - state.hi20.shift(3)
    state["hike_breadth3"] = frame.get("share_rising", pd.Series(np.nan, index=frame.index)).rolling(3).mean()
    state["upper_gap"] = frame.get("q90", np.nan) - frame["median"]
    state["margin_change3"] = state.margin - state.margin.shift(3)
    state["retail_change3"] = state.retail - state.retail.shift(3)
    state["margin_slope7"] = state.margin.diff().rolling(6, min_periods=6).median()
    state["retail_slope7"] = state.retail.diff().rolling(6, min_periods=6).median()
    # Origin-only regime labels; these are descriptions, not externally verified causes.
    state["regime"] = np.select([state.hi20.ge(.1), state.tgp_change7.abs().ge(5)],
        ["diffusion_visible", "wholesale_moving"], default="quiet_other")
    return state


def nonnegative_change_ridge(x, y, target, ages):
    """Weighted nonnegative regression with zero intercept and 50% shrink to zero.

    RMS scaling preserves the zero-change anchor. Unlike centering, all-zero
    upstream changes therefore imply exactly zero predicted wholesale change.
    """
    weights = np.exp(-np.asarray(ages) / 180)
    scale = np.sqrt(np.average(x * x, axis=0, weights=weights))
    scale[scale < 1e-6] = 1
    design = x / scale
    gram = design.T @ (design * weights[:, None]) + 20 * np.eye(x.shape[1])
    rhs = design.T @ (y * weights)
    step = 1 / float(np.linalg.eigvalsh(gram).max())
    beta = np.zeros(x.shape[1])
    for _ in range(80):
        beta = np.maximum(0, beta - step * (gram @ beta - rhs))
    predicted = .5 * float((target / scale) @ beta)
    return float(np.clip(predicted, -30, 30)), (0.5 * beta / scale).tolist()


def logistic_event(x, y, target):
    """Small regularized logistic model; normalization sees training rows only."""
    centre, scale = x.mean(axis=0), x.std(axis=0)
    scale[scale < 1e-6] = 1
    design = np.column_stack([np.ones(len(x)), np.clip((x - centre) / scale, -6, 6)])
    probability = (y.sum() + .5) / (len(y) + 1)
    prior = float(np.log(probability / (1 - probability)))
    beta = np.zeros(design.shape[1])
    beta[0] = prior
    penalties = np.diag([1.] + [5.] * x.shape[1])
    anchor = beta.copy()
    for _ in range(25):
        p = 1 / (1 + np.exp(-np.clip(design @ beta, -30, 30)))
        weight = np.maximum(p * (1 - p), 1e-5)
        gradient = design.T @ (p - y) + penalties @ (beta - anchor)
        hessian = design.T @ (design * weight[:, None]) + penalties
        move = np.linalg.solve(hessian, gradient)
        beta -= np.clip(move, -2, 2)
        if np.max(np.abs(move)) < 1e-6:
            break
    new = np.r_[1., np.clip((target - centre) / scale, -6, 6)]
    return float(1 / (1 + np.exp(-np.clip(new @ beta, -30, 30))))


def origin_candidates(state, origin, horizon):
    current = float(state.loc[origin, "retail"])
    slope = state.loc[origin, "retail_slope7"]
    slope = float(slope) if np.isfinite(slope) else 0.
    results = {"persistence": {"prediction": current, "state": {}},
        "damped_trend": {"prediction": current + float(np.clip(slope, -4, 4)) * sum(.8 ** k for k in range(horizon)),
                         "state": {"slope": slope}}}
    maturity = state.index + pd.Timedelta(days=horizon) <= origin
    cost_target = state.tgp.shift(-horizon) - state.tgp
    margin_target = state.margin.shift(-horizon) - state.margin
    cost_valid = state[COST_COLUMNS].notna().all(axis=1) & cost_target.notna()
    event_valid = state[EVENT_COLUMNS].notna().all(axis=1) & margin_target.notna()
    cost_train = cost_valid & maturity & (state.index >= origin - pd.Timedelta(days=365))
    event_train = event_valid & maturity & (state.index >= origin - pd.Timedelta(days=730))
    reasons = {}
    cost_delta, margin_delta = None, None
    if cost_train.sum() >= MIN_TRAINING and state.loc[origin, COST_COLUMNS].notna().all():
        cost_delta, coefficient = nonnegative_change_ridge(state.loc[cost_train, COST_COLUMNS].to_numpy(),
            cost_target.loc[cost_train].to_numpy(), state.loc[origin, COST_COLUMNS].to_numpy(dtype=float),
            (origin - state.index[cost_train]).days.to_numpy())
        results["cost_only"] = {"prediction": current + cost_delta, "state": {
            "cost_delta": cost_delta, "training_n": int(cost_train.sum()),
            "cost_features": COST_COLUMNS, "coefficients_original_units": coefficient}}
    else:
        reasons["cost_only"] = "Insufficient matured cost labels or current as-of upstream state"
    if event_train.sum() >= MIN_TRAINING and state.loc[origin, EVENT_COLUMNS].notna().all():
        event = margin_target.loc[event_train].ge(10).astype(float)
        probability = logistic_event(state.loc[event_train, EVENT_COLUMNS].to_numpy(), event.to_numpy(),
                                    state.loc[origin, EVENT_COLUMNS].to_numpy(dtype=float))
        positive = margin_target.loc[event_train & margin_target.ge(10)]
        jump = float(np.clip(positive.median(), 10, 60)) if len(positive) >= 5 else 0.
        # Quiet prices do not inherit a positive intercept or automatic calendar jump.
        visible_diffusion = bool(state.loc[origin, "hi20"] >= .1)
        jump_called = bool(visible_diffusion and probability >= .5 and jump > 0)
        margin_slope = state.loc[origin, "margin_slope7"]
        erosion = (float(np.clip(margin_slope, -3, 0)) * sum(.8 ** k for k in range(horizon))
                   if np.isfinite(margin_slope) and margin_slope < -.25 else 0.)
        margin_delta = jump if jump_called else erosion
        params = {"margin_expansion_probability": probability, "event_definition": "endpoint retail-minus-asof-TGP increase >=10 c/L",
            "conditional_margin_jump_median": jump, "jump_called": jump_called, "margin_delta": margin_delta,
            "positive_training_labels": len(positive), "training_n": int(event_train.sum()), "erosion": erosion}
        results["diffusion_hurdle"] = {"prediction": current + margin_delta, "state": params}
        if cost_delta is not None:
            results["mechanism_combined"] = {"prediction": current + cost_delta + margin_delta,
                "state": {**params, "cost_delta": cost_delta}}
    else:
        reasons["diffusion_hurdle"] = "Insufficient matured margin labels or current station/as-of TGP state"
    if "mechanism_combined" not in results:
        reasons["mechanism_combined"] = "Both separately estimated components required"
    return results, reasons


def retail_hurdle_candidate(state, origin, horizon):
    """Post-initial-results ablation: same hurdle, retail labels/slope, no TGP.

    This is deliberately separate from the original selector's frozen candidate
    pool. Availability does not imply this later variant was preregistered.
    """
    target = state.retail.shift(-horizon) - state.retail
    valid = state[RETAIL_EVENT_COLUMNS].notna().all(axis=1) & target.notna()
    train = valid & (state.index + pd.Timedelta(days=horizon) <= origin)
    train &= state.index >= origin - pd.Timedelta(days=730)
    if train.sum() < MIN_TRAINING or not state.loc[origin, RETAIL_EVENT_COLUMNS].notna().all():
        return None
    event = target.loc[train].ge(10).astype(float)
    probability = logistic_event(state.loc[train, RETAIL_EVENT_COLUMNS].to_numpy(), event.to_numpy(),
                                state.loc[origin, RETAIL_EVENT_COLUMNS].to_numpy(dtype=float))
    positive = target.loc[train & target.ge(10)]
    jump = float(np.clip(positive.median(), 10, 60)) if len(positive) >= 5 else 0.
    called = bool(state.loc[origin, "hi20"] >= .1 and probability >= .5 and jump > 0)
    slope = state.loc[origin, "retail_slope7"]
    erosion = (float(np.clip(slope, -3, 0)) * sum(.8 ** k for k in range(horizon))
               if np.isfinite(slope) and slope < -.25 else 0.)
    delta = jump if called else erosion
    return {"prediction": float(state.loc[origin, "retail"]) + delta, "state": {
        "retail_expansion_probability": probability, "event_definition": "endpoint retail increase >=10 c/L",
        "jump_called": called, "conditional_jump_median": jump, "retail_delta": delta,
        "positive_training_labels": len(positive), "training_n": int(train.sum()), "erosion": erosion,
        "research_status": "Exploratory retail-only ablation added after initial mechanism results",
        "requires_upstream": False}}


def select_from_matured_losses(results, prior_losses, origin, horizon):
    """Conservative online selection; no outcome with target after origin can vote."""
    selected = "persistence"
    diagnostic = {"selected_model": selected, "minimum_relative_gain": .1}
    if prior_losses is None or len(prior_losses) == 0:
        diagnostic["reason"] = "No matured challenger loss history"
        return selected, diagnostic
    losses = pd.DataFrame(prior_losses).rename(columns={"origin_date": "origin", "absolute_error_cpl": "absolute_error"})
    losses["origin"] = pd.to_datetime(losses.origin).dt.tz_localize(None).dt.normalize()
    losses = losses.loc[losses.horizon.eq(horizon)
        & (losses.origin + pd.Timedelta(days=horizon) <= origin)
        & losses.origin.ge(origin - pd.Timedelta(days=180))]
    # A late-recorded/corrected outcome cannot be used before it was actually scored.
    if "scored_at_utc" in losses:
        known = pd.to_datetime(losses.scored_at_utc, utc=True).dt.tz_convert("Australia/Brisbane").dt.tz_localize(None)
        losses = losses.loc[known < origin + pd.Timedelta(days=1)]
    matrix = losses.pivot_table(index="origin", columns="model", values="absolute_error", aggfunc="last").sort_index()
    if "persistence" not in matrix:
        diagnostic["reason"] = "No common persistence losses"
        return selected, diagnostic
    block = max(14, horizon)
    eligible = []
    for model in results:
        if model not in ("damped_trend", "cost_only", "diffusion_hurdle", "mechanism_combined") or model not in matrix:
            continue
        paired = matrix[["persistence", model]].dropna()
        nblocks = len(paired) // block
        if nblocks < 4:
            continue
        paired = paired.iloc[-nblocks * block:]
        # Gaps can make row blocks misleading; refuse them rather than compress time.
        if not np.all(np.diff(paired.index.values).astype("timedelta64[D]").astype(int) == 1):
            continue
        baseline = float(paired.persistence.mean())
        model_loss = float(paired[model].mean())
        diff = (paired[model] - paired.persistence).to_numpy().reshape(nblocks, block).mean(axis=1)
        upper = float(diff.mean() + 1.96 * diff.std(ddof=1) / np.sqrt(nblocks))
        if baseline > 0 and model_loss <= .9 * baseline and (diff < 0).mean() >= .75 and upper < 0:
            eligible.append((model_loss / baseline, model, len(paired), upper))
    if eligible:
        ratio, selected, n, upper = min(eligible)
        diagnostic.update(selected_model=selected, matured_pairs=n, loss_ratio=ratio,
                          block_normal_upper=upper, reason="Passed fixed matured paired-loss gate")
    else:
        diagnostic["reason"] = "No challenger passed fixed matured paired-loss gate"
    return selected, diagnostic


def predict_candidates(frame, upstream, origin, horizon, prior_losses=None):
    """Forward-logger callback; outputs are JSON serializable and skips explicit."""
    origin = pd.Timestamp(origin).normalize()
    if horizon not in HORIZONS:
        raise ValueError("Unsupported horizon")
    daily = normalize_daily(frame).loc[:origin]
    if origin not in daily.index or pd.isna(daily.loc[origin, "median"]):
        raise ValueError("Origin needs a completed daily price")
    state = prepare_state(daily, upstream)
    results, exclusions = origin_candidates(state, origin, horizon)
    retail = retail_hurdle_candidate(state, origin, horizon)
    if retail:
        results["retail_diffusion_hurdle"] = retail
    else:
        exclusions["retail_diffusion_hurdle"] = "Insufficient matured retail labels or current station state"
    chosen, diagnostic = select_from_matured_losses(results, prior_losses, origin, horizon)
    results["online_selector"] = {"prediction": results[chosen]["prediction"],
        "state": {**diagnostic, "excluded_candidates": exclusions}}
    return results


def run(daily, upstream, horizons=HORIZONS, include_retail_ablation=False):
    state = prepare_state(daily, upstream)
    rows = []
    for horizon in horizons:
        prior_losses = []
        for origin in state.index:
            target = origin + pd.Timedelta(days=horizon)
            if target not in state.index or not np.isfinite(state.loc[origin, "retail"]) or not np.isfinite(state.loc[target, "retail"]):
                continue
            results, exclusions = origin_candidates(state, origin, horizon)
            # Common qualified samples: require both components, so baselines have
            # identical dates; callback itself can still emit baselines earlier.
            if "mechanism_combined" not in results:
                continue
            if include_retail_ablation:
                retail = retail_hurdle_candidate(state, origin, horizon)
                if retail:
                    results["retail_diffusion_hurdle"] = retail
            chosen, diagnostic = select_from_matured_losses(results, prior_losses, origin, horizon)
            results["online_selector"] = {"prediction": results[chosen]["prediction"], "state": diagnostic}
            for model, result in results.items():
                actual, predicted = float(state.loc[target, "retail"]), float(result["prediction"])
                error = predicted - actual
                row = {"origin": origin, "target_date": target, "horizon": horizon, "model": model,
                    "actual": actual, "current": float(state.loc[origin, "retail"]), "prediction": predicted,
                    "error": error, "absolute_error": abs(error), "regime": state.loc[origin, "regime"],
                    "year": origin.year, "parameters": json.dumps(result["state"], sort_keys=True)}
                rows.append(row)
                if model != "online_selector":
                    prior_losses.append({"origin": origin, "horizon": horizon, "model": model, "absolute_error": abs(error)})
    return pd.DataFrame(rows), state


def summaries(predictions):
    outputs = []
    slices = {"all_diagnostic": predictions}
    for year, group in predictions.groupby("year"):
        slices[f"year_{year}"] = group.loc[group.target_date.dt.year.eq(year)]
    for regime, group in predictions.groupby("regime"):
        slices[f"origin_regime_{regime}"] = group
    slices["seen_late_2026"] = predictions.loc[predictions.origin.ge("2026-09-29")]
    for period, section in slices.items():
        for (horizon, model), group in section.groupby(["horizon", "model"]):
            baseline = section.loc[section.horizon.eq(horizon) & section.model.eq("persistence")].set_index("origin")
            paired = group.set_index("origin").join(baseline.absolute_error.rename("baseline_error"))
            outputs.append({"period": period, "horizon": horizon, "model": model, "n": len(group),
                "first_origin": str(group.origin.min().date()), "last_origin": str(group.origin.max().date()),
                "mae": float(group.absolute_error.mean()), "rmse": float(np.sqrt((group.error ** 2).mean())),
                "bias": float(group.error.mean()), "baseline_mae": float(paired.baseline_error.mean()),
                "skill": (float(1 - group.absolute_error.mean() / paired.baseline_error.mean())
                          if paired.baseline_error.mean() > 0 else None),
                "severe_error_ge10_rate": float(group.absolute_error.ge(10).mean()),
                "rise_ge10_count": int((group.actual - group.current).ge(10).sum())})
    return pd.DataFrame(outputs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily", type=Path, default=Path(".local/research-2026-11-04/extended/daily_reported.csv"))
    parser.add_argument("--upstream", type=Path, default=Path(".local/research-2026-11-04/upstream_history.csv"))
    parser.add_argument("--output", type=Path, default=Path(".local/research-2026-11-04/mechanisms"))
    parser.add_argument("--horizons", type=int, nargs="+", default=HORIZONS)
    parser.add_argument("--retail-ablation", action="store_true")
    args = parser.parse_args()
    daily = pd.read_csv(args.daily, parse_dates=["date"])
    upstream = pd.read_csv(args.upstream)
    from threadpoolctl import threadpool_limits
    with threadpool_limits(limits=1):
        predictions, state = run(daily, upstream, horizons=tuple(args.horizons), include_retail_ablation=args.retail_ablation)
    args.output.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(args.output / "predictions.csv", index=False)
    state.to_csv(args.output / "state_features.csv", index_label="date")
    metrics = summaries(predictions)
    metrics.to_csv(args.output / "metrics.csv", index=False)
    selection = predictions.loc[predictions.model.eq("online_selector"), ["origin", "horizon", "year", "regime", "parameters"]].copy()
    selection["selected_model"] = selection.parameters.map(lambda value: json.loads(value)["selected_model"])
    selection.groupby(["year", "horizon", "selected_model"]).size().rename("days").reset_index().to_csv(
        args.output / "selector_choices.csv", index=False)
    manifest = {"diagnostic_only": True, "all_2026_already_seen": True, "models": list(predictions.model.unique()),
        "horizons": args.horizons, "minimum_training": MIN_TRAINING,
        "retail_ablation_added_after_initial_results": args.retail_ablation,
        "cost_training_calendar_days": 365, "event_training_calendar_days": 730,
        "cost_zero_intercept": True, "cost_nonnegative_coefficients": True, "cost_shrinkage": .5,
        "cost_ridge_penalty": 20, "cost_delta_cap_cpl": 30, "logistic_ridge_penalty": 5,
        "event": "Endpoint retail-minus-asof-TGP increase >=10cpl", "event_probability_threshold": .5,
        "minimum_hi20_for_jump": .1, "erosion_daily_deadband": -.25,
        "selector": {"lookback_days": 180, "minimum_blocks": 4, "block_days": "max(14,horizon)",
            "minimum_gain": .1, "negative_blocks_fraction": .75, "normal_upper_multiplier": 1.96},
        "daily_sha256": hashlib.sha256(args.daily.read_bytes()).hexdigest(),
        "upstream_sha256": hashlib.sha256(args.upstream.read_bytes()).hexdigest(),
        "code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "versions": {name: importlib.metadata.version(name) for name in ["numpy", "pandas", "threadpoolctl"]},
        "limitations": ["Research upstream uses assumed publication lags, not actual vintages",
            "Year blocks are chronological diagnostics, not untouched confirmation",
            "Origin regimes are descriptive and cannot establish a cause",
            "Selector normal approximation is a conservative heuristic, not a valid causal or statistical test",
            "No calibrated price interval is claimed"]}
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(metrics.loc[metrics.period.isin(["all_diagnostic", "seen_late_2026"])].to_string(index=False))


if __name__ == "__main__":
    main()
