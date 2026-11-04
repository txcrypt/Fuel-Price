# Brisbane fuel research

This directory contains isolated experiments. It does not replace the dashboard
forecast or change buying recommendations. Start with
[`../docs/research-design-2026-11-04.md`](../docs/research-design-2026-11-04.md).

From the repository root, using its Python environment:

```powershell
.\.venv\Scripts\python.exe -m pip install -r research/requirements.txt
.\.venv\Scripts\python.exe research/prepare_data.py
.\.venv\Scripts\python.exe research/sensitivity_data.py
.\.venv\Scripts\python.exe research/context_data.py
.\.venv\Scripts\python.exe research/context_experiments.py
.\.venv\Scripts\python.exe research/excise_experiments.py
.\.venv\Scripts\python.exe research/forecast_experiments.py --daily .local/research-2026-11-04/daily_reported.csv --upstream .local/research-2026-11-04/upstream_history.csv --nonlinear --output .local/research-2026-11-04/models-final
.\.venv\Scripts\python.exe research/forecast_experiments.py --daily .local/research-2026-11-04/daily_reported.csv --upstream .local/research-2026-11-04/upstream_history.csv --nonlinear --target mean --horizons 1 7 --output .local/research-2026-11-04/models-mean
.\.venv\Scripts\python.exe research/forecast_experiments.py --daily .local/research-2026-11-04/daily_reported_fixed_cohort.csv --upstream .local/research-2026-11-04/upstream_history.csv --nonlinear --horizons 1 7 --output .local/research-2026-11-04/models-fixed-cohort
.\.venv\Scripts\python.exe research/forecast_experiments.py --daily .local/research-2026-11-04/daily_reported_7day.csv --upstream .local/research-2026-11-04/upstream_history.csv --horizons 1 7 --output .local/research-2026-11-04/models-7day
.\.venv\Scripts\python.exe research/forecast_experiments.py --daily .local/research-2026-11-04/daily_reported_30day.csv --upstream .local/research-2026-11-04/upstream_history.csv --horizons 1 7 --output .local/research-2026-11-04/models-30day
.\.venv\Scripts\python.exe research/preserve_evidence.py --models .local/research-2026-11-04/models-final .local/research-2026-11-04/models-mean .local/research-2026-11-04/models-fixed-cohort .local/research-2026-11-04/models-7day .local/research-2026-11-04/models-30day
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_research_*.py' -v
```

`prepare_data.py` opens SQLite in read-only mode and reads the collection CSV.
It exports provenance, daily panels, ambiguity/expiry sensitivity and available
forward-log scores. `context_data.py` downloads official current histories using
the configured EIA credential, never prints the key, and writes only the research
cache. Network access and optional research dependencies are required for those
steps. The application itself does not need the research dependencies.

Primary target is the equally weighted station median reconstructed from reported
events under a 14-day report-expiry rule. Collected daily medians and the mixed
series are separate exports; do not silently substitute them as the primary
target. Quantify disagreement and expiry sensitivity before treating errors as
accuracy against the true market price.

`sensitivity_data.py` uses the frozen exports to prepare mean/cohort/report-expiry
comparisons. The cohort membership is selected using development dates only;
its historical development scores are diagnostic because membership selection
itself used that full development period. October membership uses no October
selection information.

The nonlinear walk-forward fits can take several minutes. The script limits
numeric threads to avoid oversubscribing this machine; fixed model settings and
daily refitting are preserved across primary and sensitivity runs.

All raw outputs are ignored under `.local/research-2026-11-04`. Keep them to replay
the same experiment: downloading a historical file again does not guarantee the
same values, and a running collector can add rows to the production sources.
Reports and the market-structure figure are retained in `docs/` for review.

This is a retrospective study with a short later temporal assessment, not a claim
of prospective model validation or causal identification. Results should remain
research outputs until a separate, fixed, genuinely forward trial passes the
promotion conditions in the research design.
