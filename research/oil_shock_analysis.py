"""Explore externally identified oil-supply news and Brisbane pass-through.

Retrospective economic identification exercise, never a real-time forecast input.
The author dataset is released with substantial delay. Exclusion restrictions
and instrument relevance must be assessed, not inferred from the dataset title.
"""
from pathlib import Path
from datetime import datetime, timezone
import argparse
import hashlib
import json

import numpy as np
import pandas as pd
import requests
from scipy.stats import t as student_t

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / '.local' / 'research-2026-11-04' / 'oil-shocks'
SOURCE = 'https://raw.githubusercontent.com/dkaenzig/oilsupplynews/master/oilSupplyNewsShocks_2025M12.xlsx'


def acquire():
    OUT.mkdir(parents=True, exist_ok=True)
    target = OUT / 'oilSupplyNewsShocks_2025M12.xlsx'
    if not target.exists():
        response = requests.get(SOURCE, timeout=45)
        response.raise_for_status()
        target.write_bytes(response.content)
    raw = target.read_bytes()
    metadata = {'source_url': SOURCE, 'sha256': hashlib.sha256(raw).hexdigest(),
        'file_bytes': len(raw), 'retrieved_at_utc': datetime.now(timezone.utc).isoformat(),
        'citation': 'Kaenzig (2021), AER 111(4), doi:10.1257/aer.20190964',
        'license': 'CC BY 4.0 per author repository README',
        'real_time_feature': False, 'vintage': '2025M12'}
    book = pd.ExcelFile(target)
    metadata['sheets'] = book.sheet_names
    (OUT / 'acquisition.json').write_text(json.dumps(metadata, indent=2), encoding='utf-8')
    print(json.dumps(metadata, indent=2))
    for sheet in book.sheet_names:
        print(sheet)
        print(pd.read_excel(book, sheet_name=sheet, header=None).head(9).to_string(index=False, header=False))


def calendar_covariance(x, residual, dates, bandwidth):
    """HC3-adjusted Bartlett calendar HAC; sparse events are not calendar days."""
    inv = np.linalg.pinv(x.T @ x)
    leverage = np.sum((x @ inv) * x, axis=1)
    adjusted = residual / np.maximum(1 - leverage, .01)
    score = x * adjusted[:, None]
    distance = np.abs((dates.to_numpy()[:, None] - dates.to_numpy()[None, :])
                      .astype('timedelta64[D]').astype(float))
    weights = np.clip(1 - distance / (bandwidth + 1), 0, 1)
    return inv @ (score.T @ weights @ score) @ inv


def fit_ols(y, x, dates, bandwidth):
    if np.linalg.matrix_rank(x) < x.shape[1] or len(y) <= x.shape[1] + 2:
        raise ValueError('Insufficient full-rank event observations')
    coefficients = np.linalg.lstsq(x, y, rcond=None)[0]
    residual = y - x @ coefficients
    covariance = calendar_covariance(x, residual, dates, bandwidth)
    return coefficients, residual, covariance


def quadratic_set(a, b, c):
    """Solve a*b_coeff^2+b*b_coeff+c<=0, allowing disjoint/unbounded sets."""
    if abs(a) < 1e-12:
        if abs(b) < 1e-12:
            return {'kind': 'all_real' if c <= 0 else 'empty'}
        bound = -c / b
        return {'kind': 'upper_halfline' if b > 0 else 'lower_halfline', 'bound': bound}
    discriminant = b*b - 4*a*c
    if discriminant < 0:
        return {'kind': 'all_real' if a < 0 else 'empty'}
    roots = sorted(((-b - np.sqrt(discriminant)) / (2*a),
                    (-b + np.sqrt(discriminant)) / (2*a)))
    return {'kind': 'bounded' if a > 0 else 'outside_interval', 'low': roots[0], 'high': roots[1]}


def weak_iv_set(y, treatment, controls, instrument, dates, bandwidth):
    """Invert the instrument coefficient test, rather than trust weak-IV Wald SEs.

    Calendar HAC and Student critical values are small-sample approximations,
    not a finite-sample exact confidence guarantee.
    """
    design = np.column_stack([controls, instrument])
    cy, ey, vy = fit_ols(y, design, dates, bandwidth)
    cx, ex, vx = fit_ols(treatment, design, dates, bandwidth)
    # Cross covariance follows by polarization of the same covariance estimator.
    vsum = calendar_covariance(design, ey + ex, dates, bandwidth)
    cross = (vsum - vy - vx) / 2
    critical = float(student_t.ppf(.975, len(y) - design.shape[1]))
    a, b = float(cy[-1]), float(cx[-1])
    vyy, vxx, vxy = float(vy[-1, -1]), float(vx[-1, -1]), float(cross[-1, -1])
    region = quadratic_set(b*b - critical**2*vxx,
                           -2*a*b + 2*critical**2*vxy,
                           a*a - critical**2*vyy)
    return {'reduced_form_cpl_per_pc_unit': a,
            'reduced_form_se': float(np.sqrt(max(vyy, 0))),
            'first_stage_pct_brent_per_pc_unit': b,
            'first_stage_robust_f': b*b/vxx if vxx > 0 else None,
            'iv_cpl_per_pct_brent': a/b if abs(b) > 1e-10 else None,
            'approximate_ar95': region}


def build_events(daily, raw, shocks, anchor_shift=0):
    oil = raw.loc[raw.source.eq('brent')].sort_values('observed_date').set_index('observed_date').value
    rows = []
    for _, shock in shocks.iterrows():
        event = pd.Timestamp(shock['Date/Contract']).normalize()
        previous_quotes, after_quotes = oil.loc[oil.index < event], oil.loc[oil.index >= event]
        if previous_quotes.empty or after_quotes.empty:
            continue
        first_quote = after_quotes.index[0]
        if (first_quote - event).days > 5:
            continue
        # US trading-date close precedes the following Brisbane day's close.
        anchor = first_quote + pd.Timedelta(days=1 + anchor_shift)
        baseline = event - pd.Timedelta(days=1)
        if baseline not in daily.index:
            continue
        lag7 = baseline - pd.Timedelta(days=7)
        pre14 = baseline - pd.Timedelta(days=14)
        if pre14 not in daily.index:
            continue
        old_oil = oil.loc[oil.index <= lag7]
        if old_oil.empty:
            continue
        row = {'event_date': event, 'anchor': anchor, 'baseline': baseline,
               'pc': float(shock.PC), 'front': float(shock.Front),
               'brent_response_pct': 100*np.log(after_quotes.iloc[0]/previous_quotes.iloc[-1]),
               'pre_retail_change7': float(daily.loc[baseline, 'price_cpl']-daily.loc[lag7, 'price_cpl']),
               'pre_brent_change7': 100*np.log(previous_quotes.iloc[-1]/old_oil.iloc[-1]),
               'placebo_pre14_to_pre7': float(daily.loc[lag7, 'price_cpl']-daily.loc[pre14, 'price_cpl'])}
        for horizon in (1, 7, 14, 28):
            target = anchor + pd.Timedelta(days=horizon)
            row[f'retail_h{horizon}'] = (float(daily.loc[target, 'price_cpl']-daily.loc[baseline, 'price_cpl'])
                                          if target in daily.index else np.nan)
        rows.append(row)
    return pd.DataFrame(rows)


def analyze(daily_path, target='median'):
    if not (OUT / 'acquisition.json').exists():
        raise ValueError('Acquire and inspect the source before analysis')
    daily = pd.read_csv(daily_path, parse_dates=['date']).set_index('date')
    if target == 'mean':
        daily['price_cpl'] = daily['mean_cpl']
    destination = OUT / target
    destination.mkdir(parents=True, exist_ok=True)
    raw = pd.read_csv(OUT.parent / 'upstream_history.csv', parse_dates=['observed_date'])
    shocks = pd.read_excel(OUT / 'oilSupplyNewsShocks_2025M12.xlsx', sheet_name='Daily')
    results = []
    for shift in (0, 1):
        events = build_events(daily, raw, shocks, shift)
        events.to_csv(destination / f'event_panel_shift{shift}.csv', index=False)
        # Excluding the April 2025 joint tariff/OPEC episode is a sensitivity,
        # not an assertion that all other announcements are unconfounded.
        for sensitivity, group in [('all_events', events),
                                    ('without_2025_april03', events.loc[events.event_date.ne('2025-04-03')])]:
            for horizon in (1, 7, 14, 28):
                for outcome in (f'retail_h{horizon}', 'placebo_pre14_to_pre7'):
                    use = group.dropna(subset=[outcome])
                    if len(use) < 8:
                        continue
                    controls = np.column_stack([np.ones(len(use)), use.pre_retail_change7,
                                                 use.pre_brent_change7])
                    result = weak_iv_set(use[outcome].to_numpy(), use.brent_response_pct.to_numpy(),
                        controls, use.pc.to_numpy(), pd.DatetimeIndex(use.anchor), max(14, horizon))
                    result.update({'anchor_shift_days': shift, 'sensitivity': sensitivity,
                        'outcome': outcome, 'horizon': horizon, 'events': len(use),
                        'first_event': str(use.event_date.min().date()),
                        'last_event': str(use.event_date.max().date())})
                    results.append(result)
    (destination / 'identification_results.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
    summary = pd.DataFrame([{**r, 'approximate_ar95': json.dumps(r['approximate_ar95'])} for r in results])
    summary.to_csv(destination / 'identification_results.csv', index=False)
    manifest = {'target': target, 'daily_input_sha256': hashlib.sha256(daily_path.read_bytes()).hexdigest(),
        'upstream_input_sha256': hashlib.sha256((OUT.parent/'upstream_history.csv').read_bytes()).hexdigest(),
        'instrument_file_sha256': hashlib.sha256((OUT/'oilSupplyNewsShocks_2025M12.xlsx').read_bytes()).hexdigest(),
        'code_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'confidence': 'Approximate calendar-HAC/HC3 Anderson-Rubin inversion; small event sample',
        'identification': 'Candidate instrument; exclusion and exogeneity not established for this application',
        'causal_effect_identified': False, 'forecast_feature': False}
    (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(summary.loc[summary.anchor_shift_days.eq(0) & summary.sensitivity.eq('all_events') &
                      summary.outcome.str.startswith('retail_')].to_string(index=False))
    print('Events:', len(build_events(daily, raw, shocks)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--acquire', action='store_true')
    parser.add_argument('--daily', type=Path)
    parser.add_argument('--target', choices=['median', 'mean'], default='median')
    args = parser.parse_args()
    if args.acquire:
        acquire()
    if args.daily:
        analyze(args.daily, args.target)
