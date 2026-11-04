"""Add the post-result retail-only ablation on an existing run's exact origins.

No original prediction or selector is refitted or changed. This script makes
measurement sensitivity checks inexpensive while preserving paired comparisons.
"""
from pathlib import Path
import argparse
import json

import pandas as pd

from mechanism_models import retail_hurdle_candidate, summaries


def add_retail(source, output):
    state = pd.read_csv(source / "state_features.csv", parse_dates=["date"]).set_index("date")
    if "retail_change3" not in state:
        state["retail_change3"] = state.retail - state.retail.shift(3)
    existing = pd.read_csv(source / "predictions.csv", parse_dates=["origin", "target_date"])
    rows = []
    for _, row in existing.loc[existing.model.eq("persistence")].iterrows():
        result = retail_hurdle_candidate(state, row.origin, int(row.horizon))
        if result is None:
            continue
        record = row.to_dict()
        record.update(model="retail_diffusion_hurdle", prediction=result["prediction"],
                      error=result["prediction"] - row.actual,
                      absolute_error=abs(result["prediction"] - row.actual),
                      parameters=json.dumps(result["state"], sort_keys=True))
        rows.append(record)
    appended = pd.concat([existing, pd.DataFrame(rows)], ignore_index=True)
    output.mkdir(parents=True, exist_ok=True)
    appended.to_csv(output / "predictions.csv", index=False)
    summaries(appended).to_csv(output / "metrics.csv", index=False)
    state.to_csv(output / "state_features.csv", index_label="date")
    manifest = json.loads((source / "manifest.json").read_text())
    manifest.update(retail_ablation_added_after_initial_results=True,
                    original_predictions_unchanged=True, parent_run=str(source.resolve()))
    manifest["models"] = list(appended.model.unique())
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(summaries(appended).loc[lambda frame: frame.model.eq("retail_diffusion_hurdle")].to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    from threadpoolctl import threadpool_limits
    with threadpool_limits(limits=1):
        add_retail(args.source, args.output)
