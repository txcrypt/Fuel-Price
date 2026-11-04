"""Export isolated research evidence without modifying the operational database.

Run from the repository root with .venv/Scripts/python.exe research/prepare_data.py.
Historical station panels use prior reports only, with a 14-day expiry. They are
retrospective public-report reconstructions, not evidence of local collection.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import config
from qld_history_import import _daily_last_reports


def daily_features(panel):
    median = panel.median(axis=1)
    count = panel.notna().sum(axis=1)
    # A sparse collected panel's previous row need not be yesterday.
    previous = panel.reindex(panel.index - pd.Timedelta(days=1))
    previous.index = panel.index
    matched = panel.notna() & previous.notna()
    change = panel - previous
    out = pd.DataFrame({
        'price_cpl': median,
        'mean_cpl': panel.mean(axis=1),
        'stations': count,
        'q10': panel.quantile(.1, axis=1),
        'q25': panel.quantile(.25, axis=1),
        'q75': panel.quantile(.75, axis=1),
        'q90': panel.quantile(.9, axis=1),
        'std_cpl': panel.std(axis=1),
        'hi20': panel.ge(median + 20, axis=0).sum(axis=1) / count,
        'lo20': panel.le(median - 20, axis=0).sum(axis=1) / count,
        'hiked5_share': change.ge(5).sum(axis=1) / matched.sum(axis=1),
        'cut5_share': change.le(-5).sum(axis=1) / matched.sum(axis=1),
        'matched_stations': matched.sum(axis=1),
    })
    out.index.name = 'date'
    return out


def score_forward(output, collected, as_of):
    forward = pd.read_csv(output / 'forward_predictions.csv', parse_dates=['issued_date','target_date'])
    actual = collected.loc[collected.index < pd.Timestamp(as_of)].reset_index()
    scored = forward.merge(actual[['date','price_cpl','last_capture']], left_on='target_date', right_on='date')
    scored['horizon'] = (scored.target_date-scored.issued_date).dt.days
    scored = scored.loc[scored.horizon.gt(0)]
    scored['error_cpl'] = scored.price-scored.price_cpl
    scored['absolute_error_cpl'] = scored.error_cpl.abs()
    scored.to_csv(output/'forward_scored_collected.csv',index=False)
    rows=[]
    for (model,horizon), group in scored.groupby(['model','horizon']):
        rows.append({'model':model,'horizon':int(horizon),'n':len(group),
                     'unique_origins':group.issued_date.nunique(),
                     'unique_target_days':group.target_date.nunique(),
                     'mae_cpl':float(group.absolute_error_cpl.mean()),
                     'rmse_cpl':float(np.sqrt((group.error_cpl**2).mean())),
                     'bias_cpl':float(group.error_cpl.mean())})
    pd.DataFrame(rows).to_csv(output/'forward_scoreboard.csv',index=False)
    return {'saved_rows':len(forward),'scored_rows':len(scored),
            'unique_issued_dates':scored.issued_date.nunique(),
            'unique_target_dates':scored.target_date.nunique(),
            'table':rows,
            'limitations':['Issue date only; exact issuance time, code version and feature snapshot are absent.',
                           'First daily forecast is retained by INSERT OR IGNORE. Origin inputs can be intraday.',
                           'Target is final observed capture on each collected day, which can precede midnight.',
                           'Overlapping target windows and repeated target days are dependent.']}


def prepare(db, output, as_of):
    output.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(db.resolve().as_uri() + '?mode=ro', uri=True)
    c.execute('BEGIN')
    tables = {}
    for name, in c.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        tables[name] = c.execute('SELECT COUNT(*) FROM "' + name.replace('"','""') + '"').fetchone()[0]
    events = pd.read_sql_query("SELECT * FROM reported_price_events WHERE fuel_type='Unleaded'", c)
    sources = pd.read_sql_query('SELECT * FROM import_sources', c)
    snapshot = pd.read_sql_query("SELECT site_id,price_cpl,reported_at,lat AS latitude,lng AS longitude,state,scraped_at FROM snapshots WHERE state='QLD'", c)
    for table in ['context_observations', 'context_vintages', 'forward_predictions', 'local_forecasts', 'tgp_history']:
        pd.read_sql_query('SELECT * FROM '+table, c).to_csv(output / (table+'.csv'), index=False)
    c.close()
    events.to_csv(output / 'reported_events.csv', index=False)
    sources.to_csv(output / 'import_sources.csv', index=False)
    ambiguous = events.loc[events.duplicated(['site_id','reported_at_utc'],keep=False)]
    ambiguous.to_csv(output/'ambiguous_report_timestamps.csv',index=False)
    # Conflicting same-time reports have no recoverable ordering. Fix the tie
    # policy so future event appends cannot silently change historical prices.
    ordered = events.sort_values(['site_id','reported_at_utc','price_cpl'],kind='stable')
    _, wide = _daily_last_reports(ordered.drop_duplicates(['site_id','reported_at_utc'],keep='last'))
    held = wide.ffill(limit=14).mask(lambda f: f.eq(0))
    _, low_wide = _daily_last_reports(ordered.drop_duplicates(['site_id','reported_at_utc'],keep='first'))
    low_held = low_wide.ffill(limit=14).mask(lambda f: f.eq(0))
    tie_delta = (held.median(axis=1)-low_held.median(axis=1)).abs()
    held.index.name='date'
    held.to_csv(output / 'reported_panel.csv')
    reported = daily_features(held)
    reported['provenance'] = 'reported'
    reported['stations_carried_forward'] = held.notna().sum(axis=1) - (wide.notna() & wide.ne(0)).sum(axis=1)
    reported.to_csv(output / 'daily_reported.csv')

    collection = pd.read_csv(ROOT / 'brisbane_fuel_live_collection.csv', low_memory=False)
    combined = pd.concat([snapshot, collection], ignore_index=True)
    bounds = config.ANALYSIS_BOUNDS
    ok = combined.latitude.between(bounds['lat_min'], bounds['lat_max']) & combined.longitude.between(bounds['lng_min'], bounds['lng_max']) & combined.state.eq('QLD')
    combined = combined.loc[ok].copy()
    combined['site_id'] = combined.site_id.astype(str)
    combined['scraped'] = pd.to_datetime(combined.scraped_at, format='mixed', errors='coerce')
    combined['date'] = combined.scraped.dt.normalize()
    combined = combined.dropna(subset=['date']).sort_values('scraped').drop_duplicates(['date','site_id'], keep='last')
    combined = combined.loc[combined.price_cpl.between(80,350)]
    collected_panel = combined.pivot(index='date', columns='site_id', values='price_cpl')
    collected_panel.to_csv(output / 'collected_panel.csv')
    collected = daily_features(collected_panel)
    collected['provenance'] = 'collected'
    collected['last_capture'] = combined.groupby('date').scraped.max()
    collected['provisional'] = collected.index >= pd.Timestamp(as_of)
    collected.to_csv(output / 'daily_collected.csv')
    forward_audit = score_forward(output,collected,as_of)
    # This labelled app-compatible splice is exported only for sensitivity checks;
    # the primary experiment should keep a uniform reported-data target.
    merged = pd.concat([reported.loc[~reported.index.isin(collected.index)], collected]).sort_index()
    merged.to_csv(output / 'daily_merged.csv')
    completed = merged.loc[merged.index < pd.Timestamp(as_of)]
    overlap = reported.index.intersection(collected.index)
    delta = (reported.loc[overlap,'price_cpl'] - collected.loc[overlap,'price_cpl']).abs()
    audit = {
        'as_of_brisbane_date':as_of,
        'database_read_only':True,
        'database_table_counts':tables,
        'bounds':bounds,
        'reported':{'events':len(events),'sources':len(sources),'stations':held.shape[1], 'first_day':str(held.index.min().date()),'last_day':str(held.index.max().date()),'days':len(reported),'station_count_min':int(reported.stations.min()),'station_count_median':float(reported.stations.median()),'station_count_max':int(reported.stations.max()),'carry_forward_fraction':float(reported.stations_carried_forward.sum()/reported.stations.sum()),'unavailability_events':int(events.price_cpl.eq(0).sum())},
        'collected':{'days':len(collected),'completed_days':int((~collected.provisional).sum()),'first_day':str(collected.index.min().date()),'last_day':str(collected.index.max().date()),'dates':collected.index.strftime('%Y-%m-%d').tolist()},
        'overlap':{'days':len(overlap),'median_absolute_median_difference_cpl':float(delta.median()),'max_absolute_median_difference_cpl':float(delta.max())},
        'ambiguous_report_timestamps':{'groups':ambiguous.groupby(['site_id','reported_at_utc']).ngroups,
             'policy':'maximum price at conflicting site/report timestamp; this is deterministic, not verified true ordering',
             'median_sensitivity_days':int(tie_delta.gt(0).sum()),'max_median_sensitivity_cpl':float(tie_delta.max()),
             'sensitivity':{str(k.date()):float(v) for k,v in tie_delta.loc[tie_delta.gt(0)].items()}},
        'forward_predictions':forward_audit,
        'merged_completed':{'days':len(completed),'first_day':str(completed.index.min().date()),'last_day':str(completed.index.max().date()),'missing_calendar_days':len(pd.date_range(completed.index.min(),completed.index.max()).difference(completed.index)), 'post_2026_09_28_days':int((completed.index>pd.Timestamp('2026-09-28')).sum()), 'post_2026_09_28_collected_days':int(((collected.index>pd.Timestamp('2026-09-28')) & (~collected.provisional)).sum())},
        'warnings':['Reported data is retrospective and report timestamps are assumed genuine; it is not archived live API evidence.', 'Observed dates are not known-at timestamps. Use context_vintages.known_at for strict point-in-time evaluation.', 'Current-day observations are provisional and excluded from completed-day counts.', 'Collected daily closes can precede midnight; reported-versus-collected target differences must be quantified.', 'Legacy event-only brisbane_fuel_history_clean.csv has no collection timestamps and is excluded.'],
    }
    audit['exports']={p.name:{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size} for p in sorted(output.glob('*.csv'))}
    (output/'audit.json').write_text(json.dumps(audit,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in audit.items() if k!='exports'},indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db',type=Path,default=ROOT/'fuel_data.db')
    parser.add_argument('--output',type=Path,default=ROOT/'.local/research-2026-11-04')
    parser.add_argument('--as-of',default='2026-11-04')
    args=parser.parse_args()
    prepare(args.db,args.output,args.as_of)
