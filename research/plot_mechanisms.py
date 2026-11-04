"""Render the complete horizon comparison, including failures, from frozen metrics."""
from pathlib import Path
import os

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / '.local' / 'research-2026-11-04'
os.environ.setdefault('MPLCONFIGDIR', str(CACHE / 'matplotlib-cache'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def main():
    state = pd.read_csv(CACHE / 'mechanisms/state_features.csv', parse_dates=['date']).set_index('date')
    metrics = pd.read_csv(CACHE / 'mechanisms/metrics.csv')
    fig, axes = plt.subplots(3, 1, figsize=(13, 10), gridspec_kw={'height_ratios': [1.6, 1, 1]})
    axes[0].plot(state.index, state.retail, color='#245e93', lw=1.2, label='Reconstructed Brisbane U91 median')
    axes[0].plot(state.index, state.tgp, color='#c2762b', lw=1.1, label='Lagged available Brisbane wholesale ULP')
    axes[0].set_ylabel('cents/litre')
    axes[0].legend(loc='upper left', ncol=2, fontsize=9)
    axes[0].set_title('Price cycles explain a useful weekly signal, but accuracy depends on horizon and regime',
                       loc='left', weight='bold', fontsize=13)
    colors = {'persistence': '#89929c', 'mechanism_combined': '#317e78', 'online_selector': '#7d61a4'}
    labels = {'persistence': 'Persistence', 'mechanism_combined': 'Cost + station diffusion',
              'online_selector': 'Online selector'}
    sections = [(metrics.loc[metrics.period.eq('all_diagnostic')], 'horizon', [1, 7, 14, 28],
                 'Forecast horizon (days); each horizon uses its own matched dates'),
                (metrics.loc[metrics.period.isin([f'year_{y}' for y in (2023, 2024, 2025, 2026)])
                        & metrics.horizon.eq(7)].assign(year=lambda d:d.period.str[-4:].astype(int)),
                 'year', [2023, 2024, 2025, 2026], 'Seven-day forecasts; origin and target lie in the labelled year')]
    for axis, (data, key, keys, xlabel) in zip(axes[1:], sections):
        for i, model in enumerate(colors):
            values = data.loc[data.model.eq(model)].set_index(key).mae.reindex(keys)
            bars = axis.bar(np.arange(4)+(i-1)*.24, values, width=.23, color=colors[model], label=labels[model])
            axis.bar_label(bars, labels=[f'{v:.1f}' for v in values], padding=2, fontsize=8)
        axis.set_xticks(np.arange(4), keys)
        axis.set_ylabel('Mean absolute error (c/L)')
        axis.set_xlabel(xlabel)
        axis.set_ylim(0, max(data.mae) * 1.22)
    axes[1].legend(ncol=3, fontsize=9)
    for axis in axes:
        axis.grid(axis='y', alpha=.2)
        axis.set_axisbelow(True)
    fig.text(.01, .005, 'Retrospective diagnostic, not independent confirmation. Four-week improvement is absent; '
             '2026-only weekly uncertainty crosses zero. Current historical upstream files use assumed publication lags.',
             fontsize=8)
    fig.tight_layout(rect=(0, .025, 1, 1))
    destination = ROOT / 'docs/research-assets/mechanism_results.png'
    fig.savefig(destination, dpi=160)
    plt.close(fig)
    print(destination)


if __name__ == '__main__':
    main()
