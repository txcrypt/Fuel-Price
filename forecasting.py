"""Auditable forecasts with chronological selection and held-out evaluation.

Missing collection days stay missing. Target: median of the day's station prices.
"""
import numpy as np
import pandas as pd

MODELS = {'persistence': 'Today’s price', 'damped_trend': 'Damped 7-day trend',
          'weekly': 'Weekly pattern', 'analog': 'Similar historical weeks'}


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


def build_forecast(daily, today=None):
    today = pd.Timestamp(today or pd.Timestamp.now().normalize()).normalize()
    if daily.empty:
        return {'status': 'unavailable', 'reason': 'No recorded daily prices yet.', 'points': [], 'scores': []}
    series = daily.set_index('day').price_cpl.sort_index()
    series.index = pd.to_datetime(series.index).normalize()
    series = series.loc[series.index <= today]
    if series.empty:
        return {'status': 'unavailable', 'reason': 'No observations on or before today.', 'points': [], 'scores': []}
    latest = series.index[-1]
    stale = (today - latest).days > 1
    origins = [date for date in series.index[6:-7] if
               series.reindex(pd.date_range(date - pd.Timedelta(days=6), date + pd.Timedelta(days=7))).notna().all()][-100:]
    errors = {model: [] for model in MODELS}
    for origin in origins:
        for model in MODELS:
            errors[model].append([
                abs(predict(series, origin, h, model) - float(series.loc[origin + pd.Timedelta(days=h)]))
                for h in range(1, 8)
            ])
    split = int(len(origins) * 0.65)
    # Purge overlapping target windows between selection and held-out evaluation.
    validation_indices = [i for i in range(split, len(origins))
                          if split and origins[i] > origins[split - 1] + pd.Timedelta(days=7)]
    enough = split >= 12 and len(validation_indices) >= 8
    chosen = min(MODELS, key=lambda m: np.mean(errors[m][:split])) if enough else 'persistence'
    scores = []
    for model, label in MODELS.items():
        validation = np.array([errors[model][i] for i in validation_indices])
        scores.append({'id': model, 'name': label, 'selected': model == chosen,
                       'mae': round(float(validation.mean()), 2) if enough else None,
                       'selection_mae': round(float(np.mean(errors[model][:split])), 2) if enough else None})
    points = []
    for horizon in range(1, 8):
        estimate = predict(series, latest, horizon, chosen)
        residuals = [errors[chosen][i][horizon - 1] for i in validation_indices]
        radius = float(np.quantile(residuals, .8)) if enough else None
        points.append({'date': (latest + pd.Timedelta(days=horizon)).strftime('%Y-%m-%d'),
                       'price': round(estimate, 1),
                       'low': round(max(80, estimate - radius), 1) if radius is not None else None,
                       'high': round(min(350, estimate + radius), 1) if radius is not None else None})
    current_run = 1
    for i in range(len(series) - 1, 0, -1):
        if (series.index[i] - series.index[i - 1]).days != 1:
            break
        current_run += 1
    reason = ('Collection is stale; the forecast is withheld until fresh prices arrive.' if stale else
              'Limited validation history. Using today’s median as the baseline.' if not enough else
              'Selected on earlier dates; errors measured on later held-out dates. Lower error is better.')
    if current_run < 7 and not stale:
        reason += ' Collection recently resumed; missing recent days limit trend and pattern models.'
    return {'status': 'stale' if stale else 'ready' if enough else 'limited', 'reason': reason,
            'model': MODELS[chosen], 'model_id': chosen, 'as_of': latest.strftime('%Y-%m-%d'),
            'points': [] if stale else points, 'scores': scores,
            'validation_origins': len(validation_indices) if enough else 0,
            'selection_origins': split if enough else 0, 'recent_consecutive_days': current_run,
            'validation_start': origins[validation_indices[0]].strftime('%Y-%m-%d') if enough else None,
            'validation_end': origins[validation_indices[-1]].strftime('%Y-%m-%d') if enough else None,
            'band_label': '80th percentile of held-out absolute errors; historical error range, not a guarantee.'}
