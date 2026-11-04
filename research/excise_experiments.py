"""Exploratory announcement-aware tax baseline, not a causal effect estimate.

Uses a fixed seven-day linear inventory pass-through assumption, never fitted
to October. Historical document dates + one day are availability proxies only.
"""
from pathlib import Path
import json

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / '.local' / 'research-2026-11-04'
# effective, assumed-known-by-EOD, excise c/L excluding GST
# Scheduled reversions are included from their announcement, then superseded.
SCHEDULE = [
    ('2025-02-03', '2025-01-31', 50.8),
    ('2025-08-04', '2025-07-31', 51.6),
    ('2026-02-02', '2026-01-29', 52.6),
    ('2026-04-01', '2026-03-31', 26.3),
    ('2026-07-01', '2026-03-31', 52.6),
    ('2026-04-01', '2026-04-03', 20.6),
    ('2026-07-01', '2026-06-22', 36.6),
    ('2026-08-03', '2026-06-22', 52.6),
    ('2026-08-03', '2026-07-30', 53.7),
]


def tax_path(day, origin):
    """Expected embodied tax under only the schedule known by the origin."""
    known = {}
    for effective, available, rate in sorted(SCHEDULE, key=lambda r: r[1]):
        if pd.Timestamp(available) <= origin:
            known[pd.Timestamp(effective)] = rate
    value, previous = 50.6, 50.6
    for effective, rate in sorted(known.items()):
        fraction = float(np.clip(((day - effective).days + 1) / 7, 0, 1))
        value += (rate - previous) * fraction
        previous = rate
    return value


def main():
    daily = pd.read_csv(OUT / 'daily_reported.csv', parse_dates=['date']).set_index('date')
    y = daily.price_cpl
    records = []
    for horizon in (1, 7, 14, 28):
        for origin in y.index:
            target = origin + pd.Timedelta(days=horizon)
            if target not in y.index:
                continue
            adjustment = 1.1 * (tax_path(target, origin) - tax_path(origin, origin))
            # Event subset describes where the rule acts; it is not independent evidence.
            baseline_error = abs(y.loc[origin] - y.loc[target])
            tax_error = abs(y.loc[origin] + adjustment - y.loc[target])
            records.append({'origin': origin, 'target_date': target, 'horizon': horizon,
                'period': ('fresh_temporal' if origin >= pd.Timestamp('2026-09-29') else
                           'development' if target < pd.Timestamp('2026-09-29') else 'embargo'),
                'adjustment_cpl': adjustment, 'baseline_error': baseline_error, 'tax_error': tax_error})
    rows = pd.DataFrame(records)
    rows.to_csv(OUT / 'excise_predictions.csv', index=False)
    summary = []
    for (period, horizon), group in rows.groupby(['period', 'horizon']):
        for subset, chosen in [('all', group), ('rule_acts', group.loc[group.adjustment_cpl.abs() > 1e-8])]:
            if chosen.empty:
                continue
            summary.append({'period': period, 'horizon': horizon, 'subset': subset, 'n': len(chosen),
                'persistence_mae': chosen.baseline_error.mean(), 'tax_baseline_mae': chosen.tax_error.mean()})
    result = pd.DataFrame(summary)
    result.to_csv(OUT / 'excise_metrics.csv', index=False)
    (OUT / 'excise_schedule.json').write_text(json.dumps({'initial_excise_cpl': 50.6,
        'schedule': SCHEDULE, 'pass_through_days': 7, 'gst_multiplier': 1.1,
        'status': 'exploratory after initial model results; no independent confirmation claimed',
        'availability': 'primary announcement/document dates +1 calendar day, not recorded vintages',
        'sources_and_timing_caveats': 'docs/research-literature-2026-11-04.md',
        'causal_effect_identified': False}, indent=2), encoding='utf-8')
    print(result.to_string(index=False))


if __name__ == '__main__':
    main()
