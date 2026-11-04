"""Upstream lead/lag diagnostics. Associations are not causal effect estimates.

Only differenced series are compared; conservative-lag and excise-window
sensitivities are reported alongside the default timing assumptions.
"""
from pathlib import Path
import json
import os

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / '.local' / 'research-2026-11-04'


def available_history(raw, index, multiplier=1):
    out = pd.DataFrame(index=index)
    lags = {'tgp': 1, 'audusd': 1, 'brent': 4, 'gasoline_gulf': 4}
    for source, group in raw.groupby('source'):
        group = group.copy()
        group['available'] = group.observed_date + pd.Timedelta(days=lags[source] * multiplier)
        joined = pd.merge_asof(pd.DataFrame({'day': index}),
            group.sort_values('available'), left_on='day', right_on='available', direction='backward')
        out[source] = joined.value.where((joined.day - joined.observed_date).dt.days <= 14).to_numpy()
    return out


def main():
    raw = pd.read_csv(OUT / 'upstream_history.csv', parse_dates=['observed_date'])
    daily = pd.read_csv(OUT / 'daily_reported.csv', parse_dates=['date']).set_index('date')
    records = []
    for multiplier in (1, 2):
        known = available_history(raw, daily.index, multiplier)
        # Same AUD unit at each origin; no future FX or USD observation is used.
        measures = {'tgp': known.tgp, 'audusd': known.audusd,
                    'brent_aud_cpl': known.brent / known.audusd * 100 / 158.987,
                    'gasoline_gulf_aud_cpl': known.gasoline_gulf / known.audusd * 100 / 3.785411784}
        targets = {'retail': daily.price_cpl, 'tgp': known.tgp}
        excise_window = pd.Series(False, index=daily.index)
        for effective in ('2025-02-03', '2025-08-04', '2026-02-02',
                          '2026-04-01', '2026-07-01', '2026-08-03'):
            excise_window |= (daily.index >= pd.Timestamp(effective) - pd.Timedelta(days=14)) & (
                daily.index <= pd.Timestamp(effective) + pd.Timedelta(days=14))
        for source, values in measures.items():
            x = values - values.shift(7)
            for target, y in targets.items():
                for horizon in (1, 7, 14, 28):
                    future = y.shift(-horizon) - y
                    for regime, mask in [('2025', daily.index.year == 2025),
                                         ('2026', daily.index.year == 2026),
                                         ('fresh_after_sep28', daily.index >= '2026-09-29')]:
                        for exclude in (False, True):
                            # Remove origins whose forecast window intersects an excise window.
                            clear = ~excise_window.copy()
                            for h in range(1, horizon + 1):
                                clear &= ~excise_window.shift(-h, fill_value=False)
                            valid = mask & x.notna() & future.notna() & (clear if exclude else True)
                            n = int(valid.sum())
                            r = float(x[valid].corr(future[valid])) if n >= 10 and x[valid].std() > 0 and future[valid].std() > 0 else None
                            records.append({'source': source, 'target': target, 'horizon': horizon,
                                'regime': regime, 'lag_multiplier': multiplier,
                                'exclude_excise_windows': exclude, 'origins': n,
                                'pearson_r': r})
    result = pd.DataFrame(records)
    result.to_csv(OUT / 'upstream_lag_correlations.csv', index=False)
    note = {'diagnostic_only': True,
            'causal_effect_identified': False,
            'feature': 'trailing 7-calendar-day change in the value available at origin',
            'target': 'endpoint change from origin to origin+horizon',
            'lag_sensitivity': 'default AIP/RBA1 EIA4 days; conservative AIP/RBA2 EIA8',
            'multiple_comparisons': len(result),
            'excise_exclusion': '2025 Feb3,Aug4;2026 Feb2,Apr1,Jul1,Aug3 +/-14d; any intersecting target window excluded',
            'caveat': 'Current files and assumed lags; overlapping changes; descriptive correlations only; no p-value or causal claim.'}
    (OUT / 'upstream_lag_notes.json').write_text(json.dumps(note, indent=2), encoding='utf-8')
    chosen = result[(result.target == 'retail') & (result.horizon == 7) &
                    (result.lag_multiplier == 1) & ~result.exclude_excise_windows]
    print(chosen.to_string(index=False))
    make_chart(daily, raw)


def make_chart(daily, raw):
    os.environ.setdefault('MPLCONFIGDIR', str(OUT / 'matplotlib-cache'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    known = available_history(raw, daily.index)
    fig, axes = plt.subplots(3, 1, figsize=(12, 8), sharex=True,
                             gridspec_kw={'height_ratios': [2, 1, 1]})
    axes[0].plot(daily.index, daily.price_cpl, label='Brisbane U91 median', color='#215a9a', lw=1.5)
    axes[0].plot(known.index, known.tgp, label='Brisbane wholesale ULP (lagged)', color='#c36a20', lw=1.2)
    axes[0].set_ylabel('cents/litre')
    axes[0].legend(loc='upper left', ncol=2)
    axes[0].set_title('Brisbane fuel: retail cycles, wholesale costs and regime change', loc='left', weight='bold')
    axes[1].plot(daily.index, daily.price_cpl - known.tgp, color='#577a4b')
    axes[1].set_ylabel('Retail minus TGP\n(c/L; not profit)')
    axes[2].plot(daily.index, 100 * daily.hi20, color='#7f559d')
    axes[2].set_ylabel('Stations ≥20 c/L\nabove median (%)')
    for ax in axes:
        ax.grid(alpha=.2)
        ax.axvspan(pd.Timestamp('2026-09-29'), daily.index.max(), color='#5f8f65', alpha=.12)
        for day in ('2026-04-01', '2026-07-01', '2026-08-03'):
            ax.axvline(pd.Timestamp(day), color='#a64040', ls=':', alpha=.65)
    axes[-1].set_xlabel('Retrospective report reconstruction; green = new temporal assessment. Red = major excise steps.')
    fig.tight_layout()
    figures = ROOT / 'docs' / 'research-assets'
    figures.mkdir(parents=True, exist_ok=True)
    fig.savefig(figures / 'brisbane_market_structure.png', dpi=160)
    plt.close(fig)


if __name__ == '__main__':
    main()
