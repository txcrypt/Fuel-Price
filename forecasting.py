"""Auditable forecasts with chronological selection and held-out evaluation.

Missing collection days stay missing. Target: median of the day's station prices.

Two kinds of day can enter the series and they are never merged under one label:
``collected`` days this app actually observed at the time, and ``reported`` days
reconstructed by ``qld_history_import`` from Queensland's official monthly files.
Those files are published about a month after the events they contain, so a
reported day was *not* knowable to this app on that day — but each day's value is
built only from retailer reports timestamped on or before it, and those prices
were publicly visible then through the same live API this app polls. That is what
makes a point-in-time backtest over them legitimate, and it is the only claim made
for them here. Reported days are used for model selection and held-out error
measurement, are counted separately in every output, and never contribute to
collection freshness or to the consecutive-collection run.
"""
import numpy as np
import pandas as pd

MODELS = {'persistence': 'Today’s price', 'damped_trend': 'Damped 7-day trend',
          'weekly': 'Weekly pattern', 'analog': 'Similar historical weeks'}
# ponytail: 400 origins over the 608-day backfilled series costs ~7 s per uncached
# dashboard build. Lower it, or cache the error matrix by series hash, if that hurts.
MAX_ORIGINS = 400         # Bounds backtest cost; the most recent origins are kept.
RECENT_WINDOW_DAYS = 90   # Recent-regime slice, measured on held-out origins only.
BAND_QUANTILE = .8


def predict(series, origin, horizon, model):
    history = series.loc[:origin]
    last = float(history.iloc[-1])
    if model == 'persistence':
        return last
    recent = history.reindex(pd.date_range(origin - pd.Timedelta(days=6), origin))
    if model == 'weekly':
        value = history.get(origin + pd.Timedelta(days=horizon - 7))
        return float(value) if pd.notna(value) else last
    if model == 'damped_trend':
        changes = recent.diff().dropna()
        slope = float(changes.median()) if len(changes) >= 3 else 0.0
        return float(np.clip(last + np.clip(slope, -4, 4) * sum(0.8 ** i for i in range(horizon)), 80, 350))
    if recent.isna().any():
        return last
    values = history.reindex(pd.date_range(history.index[0], origin)).to_numpy(dtype=float)
    if len(values) < 21:
        return last
    windows = np.lib.stride_tricks.sliding_window_view(values, 7)
    ends = np.arange(6, len(values))
    eligible = ends + horizon < len(values) - 8
    windows, ends = windows[eligible], ends[eligible]
    targets = values[ends + horizon]
    valid = np.isfinite(windows).all(axis=1) & np.isfinite(targets)
    windows, targets = windows[valid], targets[valid]
    if len(windows) < 5:
        return last
    distance = np.mean((np.diff(windows, axis=1) - np.diff(recent.to_numpy())) ** 2, axis=1)
    nearest = np.argsort(distance, kind='stable')[:7]
    delta = np.mean(targets[nearest] - windows[nearest, -1])
    return float(np.clip(last + delta, 80, 350))


def merge_history(daily, reported=None):
    """Collected daily observations over an optional retrospective reported series.

    Every day keeps its provenance. A day this app collected always wins over the
    same day in the backfill: the backfill records what retailers reported, not
    what this app observed, and nothing here relabels one as the other.
    """
    frames = []
    days = (reported or {}).get('days') or []
    if days:
        published = pd.DataFrame(days)
        frames.append(pd.DataFrame({'day': pd.to_datetime(published.date).dt.normalize(),
                                    'price_cpl': pd.to_numeric(published.price, errors='coerce'),
                                    'provenance': 'reported'}))
    if daily is not None and not daily.empty:
        frames.append(pd.DataFrame({'day': pd.to_datetime(daily.day).dt.normalize(),
                                    'price_cpl': pd.to_numeric(daily.price_cpl, errors='coerce'),
                                    'provenance': 'collected'}))
    if not frames:
        return pd.DataFrame({'day': pd.Series(dtype='datetime64[ns]'), 'price_cpl': pd.Series(dtype=float),
                             'provenance': pd.Series(dtype=object)})
    merged = pd.concat(frames, ignore_index=True).dropna(subset=['day', 'price_cpl'])
    # Collected rows are concatenated last, so keep='last' makes them authoritative.
    return merged.drop_duplicates('day', keep='last').sort_values('day').reset_index(drop=True)


def _per_horizon(matrix, rows):
    return [round(float(value), 2) for value in matrix[rows].mean(axis=0)]


def build_forecast(daily, today=None, reported=None):
    today = pd.Timestamp(today or pd.Timestamp.now().normalize()).normalize()
    frame = merge_history(daily, reported)
    if frame.empty:
        return {'status': 'unavailable', 'reason': 'No recorded daily prices yet.', 'points': [], 'scores': []}
    frame = frame.loc[frame.day <= today]
    if frame.empty:
        return {'status': 'unavailable', 'reason': 'No observations on or before today.', 'points': [], 'scores': []}
    series = frame.set_index('day').price_cpl
    collected = frame.set_index('day').provenance.eq('collected')
    latest = series.index[-1]
    # Freshness is a property of collection alone: a month-old official backfill
    # must never make a stopped collector look current.
    anchor = collected[collected].index.max() if collected.any() else latest
    stale = (today - anchor).days > 1
    origins = [date for date in series.index[6:-7] if
               series.reindex(pd.date_range(date - pd.Timedelta(days=6), date + pd.Timedelta(days=7))).notna().all()][-MAX_ORIGINS:]
    errors = {model: np.array([[abs(predict(series, origin, h, model) - float(series.loc[origin + pd.Timedelta(days=h)]))
                                for h in range(1, 8)] for origin in origins]).reshape(len(origins), 7)
              for model in MODELS}
    split = int(len(origins) * 0.65)
    # Purge overlapping target windows between selection and held-out evaluation.
    validation_indices = [i for i in range(split, len(origins))
                          if split and origins[i] > origins[split - 1] + pd.Timedelta(days=7)]
    enough = split >= 12 and len(validation_indices) >= 8
    chosen = min(MODELS, key=lambda m: errors[m][:split].mean()) if enough else 'persistence'
    scores = []
    for model, label in MODELS.items():
        # Band coverage is out of sample: radius fitted on the selection period,
        # hit rate measured on the held-out period.
        covered = ((errors[model][validation_indices] <= np.quantile(errors[model][:split], BAND_QUANTILE, axis=0))
                   if enough else None)
        if model == chosen:
            chosen_covered = covered
        scores.append({'id': model, 'name': label, 'selected': model == chosen,
                       'mae': round(float(errors[model][validation_indices].mean()), 2) if enough else None,
                       'horizon_mae': _per_horizon(errors[model], validation_indices) if enough else None,
                       'selection_mae': round(float(errors[model][:split].mean()), 2) if enough else None,
                       'band_coverage': round(float(covered.mean()), 3) if enough else None})
    beats = [bool(a < b) for a, b in zip(next(s for s in scores if s['selected'])['horizon_mae'],
                                         next(s for s in scores if s['id'] == 'persistence')['horizon_mae'])] if enough else None
    points = []
    for horizon in range(1, 8):
        estimate = predict(series, latest, horizon, chosen)
        radius = float(np.quantile(errors[chosen][validation_indices, horizon - 1], BAND_QUANTILE)) if enough else None
        points.append({'date': (latest + pd.Timedelta(days=horizon)).strftime('%Y-%m-%d'),
                       'price': round(estimate, 1),
                       'low': round(max(80, estimate - radius), 1) if radius is not None else None,
                       'high': round(min(350, estimate + radius), 1) if radius is not None else None})
    # An origin counts as collected only if this app collected every day it reads
    # and every day it is scored against.
    reported_origin = [not bool(collected.reindex(pd.date_range(origin - pd.Timedelta(days=6),
                                                                origin + pd.Timedelta(days=7))).eq(True).all())
                       for origin in origins]
    current_run, day = 0, latest
    while bool(collected.get(day, False)):
        current_run += 1
        day -= pd.Timedelta(days=1)
    recent_rows = [i for i in validation_indices if origins[i] > latest - pd.Timedelta(days=RECENT_WINDOW_DAYS)]
    recent = {'window_days': RECENT_WINDOW_DAYS, 'origins': len(recent_rows), 'held_out': True,
              'start': origins[recent_rows[0]].strftime('%Y-%m-%d') if recent_rows else None,
              'mae': _per_horizon(errors[chosen], recent_rows) if enough and len(recent_rows) >= 8 else None,
              'baseline_mae': _per_horizon(errors['persistence'], recent_rows) if enough and len(recent_rows) >= 8 else None}
    # Calibration is honest only out of sample: the radius is fitted on the
    # selection period and its coverage measured on the held-out period. The
    # published band above is fitted on the held-out errors themselves, so its
    # nominal 80% is in-sample; this is the number that tests it.
    calibration = {'target': BAND_QUANTILE, 'coverage': round(float(chosen_covered.mean()), 3),
                   'by_horizon': [round(float(value), 3) for value in chosen_covered.mean(axis=0)],
                   'basis': 'Band radius fitted on the selection period; coverage measured on held-out days.'
                   } if enough else None
    reason = ('Collection is stale; the forecast is withheld until fresh prices arrive.' if stale else
              'Limited validation history. Using today’s median as the baseline.' if not enough else
              'Selected on earlier dates; errors measured on later held-out dates. Lower error is better.')
    if current_run < 7 and not stale:
        reason += ' Collection recently resumed; missing recent days limit trend and pattern models.'
    if enough and any(reported_origin):
        reason += (' Model selection and held-out errors use the official retrospective reported series, '
                   'published about a month after the fact; this app did not collect those days.')
    if enough and not any(beats):
        reason += (f' {MODELS[chosen]} did not beat the persistence baseline at any held-out horizon, '
                   'so its path is not a validated prediction.')
    return {'status': 'stale' if stale else 'ready' if enough else 'limited', 'reason': reason,
            'model': MODELS[chosen], 'model_id': chosen, 'as_of': latest.strftime('%Y-%m-%d'),
            'points': [] if stale else points, 'scores': scores,
            'paths': {} if stale else {model: [round(predict(series, latest, h, model), 1) for h in range(1, 8)]
                                       for model in MODELS},
            'validation_origins': len(validation_indices) if enough else 0,
            'selection_origins': split if enough else 0, 'recent_consecutive_days': current_run,
            'validation_start': origins[validation_indices[0]].strftime('%Y-%m-%d') if enough else None,
            'validation_end': origins[validation_indices[-1]].strftime('%Y-%m-%d') if enough else None,
            'beats_baseline': beats, 'recent': recent, 'calibration': calibration,
            'provenance': {'collected_days': int(collected.sum()), 'reported_days': int((~collected).sum()),
                           'retrospective_backfill': bool((~collected).any()),
                           'selection_origins_reported': int(sum(reported_origin[:split])),
                           'validation_origins_reported': int(sum(reported_origin[i] for i in validation_indices)),
                           'note': 'Reported days come from the official monthly files published about a month late; '
                                   'they are not observations this app collected.'},
            'band_label': '80th percentile of held-out absolute errors; historical error range, not a guarantee.'}


# --- Context feeds: tested, not assumed -------------------------------------
# Wholesale, currency and overseas spot prices are stored as dated context. None of
# them enters build_forecast. `context_evaluation` is the measurement that would have
# to earn that, and it is deliberately separate: it reads only what was published and
# actually held at each origin, so nothing here can be improved by a later revision.
#
# Days a source did not publish stay unpublished. The feature is the change between
# the two most recent observations the origin could read, so a weekend gap widens the
# span it measures rather than inventing a value for the missing day.
CONTEXT_LAG_DAYS = {'aip': 1, 'rba': 1, 'eia_gasoline': 4, 'eia_brent': 4}
CONTEXT_WINDOW_DAYS = 7


def context_vintages(db, source):
    """Every recorded vintage for one source: value, observed day, and when it was held."""
    with db._connect() as connection:
        rows = connection.execute('SELECT observed_date, value, known_at FROM context_vintages '
                                  'WHERE source=? ORDER BY known_at', (source,)).fetchall()
    frame = pd.DataFrame(rows, columns=['observed_date', 'value', 'known_at'])
    frame['observed_date'] = pd.to_datetime(frame.observed_date).dt.normalize()
    frame['known_at'] = pd.to_datetime(frame.known_at)
    return frame


def context_feature(vintages, origins, lag_days, window_days=CONTEXT_WINDOW_DAYS):
    """Point-in-time change in a context series at each origin, or NaN where unknowable.

    Two independent cut-offs, both required:

    * publication lag - an observation dated within ``lag_days`` of the origin is
      discarded, because the publisher had not released it yet (EIA runs several days
      behind; the verified case was a 2026-09-15 observation for an 18 September fetch).
    * vintage - only values whose ``known_at`` falls on or before the end of the origin
      day are visible, so a revision published afterwards is invisible to that origin.
    """
    values = []
    for origin in origins:
        seen = vintages[(vintages.known_at <= origin + pd.Timedelta(days=1)) &
                        (vintages.observed_date <= origin - pd.Timedelta(days=lag_days))]
        # Last vintage wins per observed day: the newest revision this origin could hold.
        seen = seen.drop_duplicates('observed_date', keep='last').sort_values('observed_date')
        if len(seen) < 2:
            values.append(np.nan)
            continue
        latest = seen.iloc[-1]
        earlier = seen[seen.observed_date <= latest.observed_date - pd.Timedelta(days=window_days)]
        values.append(float(latest.value - earlier.iloc[-1].value) if len(earlier) else np.nan)
    return np.array(values, dtype=float)


def _fit_predict(x_selection, y_selection, x_validation, use_feature):
    """Least squares on the selection period only, applied unchanged to held-out days."""
    design = lambda x: np.column_stack([np.ones(len(x)), x] if use_feature else [np.ones(len(x))])  # noqa: E731
    coefficients, *_ = np.linalg.lstsq(design(x_selection), y_selection, rcond=None)
    return design(x_validation) @ coefficients


def _block_bootstrap(differences, draws=4000, block=14, seed=20260918):
    """Moving-block bootstrap of the mean error difference; adjacent origins overlap."""
    rng = np.random.default_rng(seed)
    n = len(differences)
    if n < block * 2:
        return None
    starts = rng.integers(0, n - block, size=(draws, int(np.ceil(n / block))))
    offsets = np.arange(block)
    means = np.array([differences[(row[:, None] + offsets).ravel()[:n]].mean() for row in starts])
    return [round(float(np.quantile(means, .025)), 2), round(float(np.quantile(means, .975)), 2)]


def context_evaluation(series, db, sources=None, today=None, max_origins=MAX_ORIGINS,
                       window_days=CONTEXT_WINDOW_DAYS):
    """Does a lagged context feed beat persistence on purged held-out days? Usually not.

    Each feed is tested on its own, so adding inputs cannot pool their way to a better
    apparent fit. Two controls, both fitted on the selection period only: ``persistence``
    (the published baseline) and ``drift``, an intercept with no feature. A feed has to
    beat *both* - otherwise its apparent gain is only the average drift it happened to
    sit next to, which the intercept already captures.
    """
    today = pd.Timestamp(today or pd.Timestamp.now().normalize()).normalize()
    series = series.loc[:today]
    origins = [date for date in series.index[6:-7] if
               series.reindex(pd.date_range(date - pd.Timedelta(days=6), date + pd.Timedelta(days=7))).notna().all()][-max_origins:]
    results = {'origins': len(origins), 'feeds': [],
               'window_days': window_days, 'lag_days': dict(CONTEXT_LAG_DAYS),
               'basis': 'Chronological 65/35 split, held-out origins whose target window overlaps the last '
                        'selection origin purged. Coefficients fitted on selection origins only. MAE in c/L.'}
    if len(origins) < 40:
        results['verdict'] = 'Not enough complete 14-day price windows to evaluate any feed.'
        return results
    actual = np.array([[float(series.loc[origin + pd.Timedelta(days=h)]) for h in range(1, 8)] for origin in origins])
    last = np.array([float(series.loc[origin]) for origin in origins])
    split = int(len(origins) * 0.65)
    validation = np.array([i for i in range(split, len(origins))
                           if origins[i] > origins[split - 1] + pd.Timedelta(days=7)])
    results.update({'selection_origins': split, 'validation_origins': int(len(validation)),
                    'validation_start': origins[validation[0]].strftime('%Y-%m-%d'),
                    'validation_end': origins[validation[-1]].strftime('%Y-%m-%d')})
    for source in (sources or list(CONTEXT_LAG_DAYS)):
        # A replay as of ``today`` sees nothing recorded after it — including the row counts
        # it reports, so a revision arriving later cannot change even the shape of the output.
        vintages = context_vintages(db, source)
        vintages = vintages[vintages.known_at <= today + pd.Timedelta(days=1)]
        feed = {'id': source, 'lag_days': CONTEXT_LAG_DAYS[source], 'vintage_rows': int(len(vintages)),
                'observed_days': int(vintages.observed_date.nunique()) if len(vintages) else 0}
        feature = (context_feature(vintages, origins, CONTEXT_LAG_DAYS[source], window_days)
                   if len(vintages) else np.full(len(origins), np.nan))
        usable = np.isfinite(feature)
        rows_selection = np.array([i for i in range(split) if usable[i]], dtype=int)
        rows_validation = np.array([i for i in validation if usable[i]], dtype=int)
        feed['usable_selection_origins'] = int(len(rows_selection))
        feed['usable_validation_origins'] = int(len(rows_validation))
        # A feed with too little published history is reported as untestable, never as a null result.
        if len(rows_selection) < 30 or len(rows_validation) < 20:
            feed['verdict'] = ('Untestable: only %d selection and %d held-out origins could read this feed at all. '
                               'Stored history is too short, not the relationship too weak.'
                               % (len(rows_selection), len(rows_validation)))
            results['feeds'].append(feed)
            continue
        baseline = np.abs(actual[rows_validation] - last[rows_validation, None])
        drift_error, context_error = np.zeros_like(baseline), np.zeros_like(baseline)
        for h in range(7):
            y = actual[:, h] - last            # what persistence gets wrong, signed
            for target, use_feature in ((drift_error, False), (context_error, True)):
                predicted = _fit_predict(feature[rows_selection], y[rows_selection],
                                         feature[rows_validation], use_feature)
                target[:, h] = np.abs(actual[rows_validation, h] - (last[rows_validation] + predicted))
        feed.update({'persistence_mae': [round(float(v), 2) for v in baseline.mean(axis=0)],
                     'drift_mae': [round(float(v), 2) for v in drift_error.mean(axis=0)],
                     'context_mae': [round(float(v), 2) for v in context_error.mean(axis=0)],
                     'beats_persistence': [bool(a < b) for a, b in zip(context_error.mean(axis=0), baseline.mean(axis=0))],
                     'beats_drift': [bool(a < b) for a, b in zip(context_error.mean(axis=0), drift_error.mean(axis=0))],
                     'bootstrap_95': [_block_bootstrap(context_error[:, h] - baseline[:, h]) for h in range(7)]})
        earned = [a and b for a, b in zip(feed['beats_persistence'], feed['beats_drift'])]
        separated = [interval is not None and interval[1] < 0 for interval in feed['bootstrap_95']]
        feed['horizons_earned'] = [h + 1 for h in range(7) if earned[h] and separated[h]]
        feed['verdict'] = ('Earned inclusion at horizons %s.' % feed['horizons_earned'] if feed['horizons_earned'] else
                           'No measured held-out improvement that is distinguishable from noise. Not included.')
        results['feeds'].append(feed)
    earned_any = [f['id'] for f in results['feeds'] if f.get('horizons_earned')]
    results['verdict'] = ('Feeds earning inclusion: %s.' % ', '.join(earned_any) if earned_any else
                          'No feed earned inclusion. None is used by build_forecast or any recommendation.')
    return results


def main(argv=None):
    """Print the held-out evaluation. Regenerates docs/forecast-evaluation-*.md:

        .venv\\Scripts\\python.exe -m forecasting --db <db holding the backfill> --today 2026-09-18

    ``--context`` adds the per-feed test behind docs/context-feature-evaluation-*.md.
    """
    import argparse
    import json
    from data_store import FuelDataStore
    from local_data import LocalData
    from qld_history_import import reported_series
    import config

    parser = argparse.ArgumentParser(description='Held-out model comparison against the persistence baseline.')
    parser.add_argument('--db', default=str(config.DB_FILE), help='SQLite file holding the reported backfill.')
    parser.add_argument('--root', default=str(config.BASE_DIR), help='Workspace holding the collected series.')
    parser.add_argument('--today', default=None)
    parser.add_argument('--context', action='store_true',
                        help='Also test each context feed as a lagged feature against persistence.')
    arguments = parser.parse_args(argv)
    result = build_forecast(LocalData(arguments.root)._history(), today=arguments.today,
                            reported=reported_series(FuelDataStore(arguments.db)))
    print(f"as_of {result['as_of']}  status {result['status']}  selected {result['model_id']}")
    print(json.dumps(result['provenance'], indent=1))
    print('model'.ljust(14) + 'select ' + ' '.join(f'h{h}'.rjust(6) for h in range(1, 8)) + '   band')
    for score in result['scores']:
        # Never print a missing score as 0.00 — no validation ran is not an error of zero.
        cell = lambda value: f'{value:6.2f}' if value is not None else '     -'  # noqa: E731
        print(f"{score['id']:<14}{cell(score['selection_mae'])} "
              + ' '.join(cell(value) for value in score['horizon_mae'] or [None] * 7)
              + f"   {cell(score['band_coverage'])}" + ('  <- selected' if score['selected'] else ''))
    print(json.dumps({'beats_baseline': result['beats_baseline'], 'recent': result['recent'],
                      'calibration': result['calibration']}, indent=1))
    print(result['reason'])
    if arguments.context:
        store = FuelDataStore(arguments.db)
        merged = merge_history(LocalData(arguments.root)._history(), reported_series(store))
        context = context_evaluation(merged.set_index('day').price_cpl, store, today=arguments.today)
        print('\ncontext feeds / lagged feature vs persistence')
        print(json.dumps(context, indent=1, default=str))


if __name__ == '__main__':
    main()
