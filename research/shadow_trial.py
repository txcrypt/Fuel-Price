"""Append-only prospective research forecasts; never writes operational data.

CLI has no historical issuance-time override. Library ``now`` exists for tests.
Forecasts are daily-last-collected-price proxies, not guaranteed midnight quotes.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import importlib
import inspect
import io
import json
import math
import os
from pathlib import Path
import platform
import sqlite3
import sys
import tempfile
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import config
from research.prepare_data import daily_features

BRISBANE = ZoneInfo('Australia/Brisbane')
HORIZONS = (1, 7, 14, 28)
POLICY = {'minimum_stations': 200, 'closing_hour': 23,
          'origin_rule': 'previous completed Brisbane calendar day',
          'target': 'median of each station final collected quote on target day',
          'price_range_cpl': [80, 350], 'model_lookback_days': 7}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def sha(data):
    return hashlib.sha256(data).hexdigest()


def stamp(now=None):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('Issuance requires a timezone-aware clock.')
    return now.astimezone(timezone.utc)


def immutable_write(path, data):
    """Publish a complete file atomically; never replace an existing path."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.pending-', delete=False) as f:
        pending = Path(f.name)
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    try:
        try:
            # Windows rename is atomic and fails if destination exists. POSIX
            # rename replaces, so use a no-replace hard link there instead.
            if os.name == 'nt':
                os.rename(pending, path)
            else:
                os.link(pending, path)
            return True
        except FileExistsError:
            return False
    finally:
        pending.unlink(missing_ok=True)


def read_record(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def records(frame):
    # pandas JSON converts missing values to null and numpy scalars to JSON types.
    return json.loads(frame.to_json(orient='records', date_format='iso', double_precision=12))


def local_times(values):
    """Collector naive timestamps mean Brisbane; explicit offsets retain their meaning."""
    def convert(value):
        try:
            result = pd.Timestamp(value)
            if pd.isna(result):
                return pd.NaT
            return result.tz_localize(BRISBANE) if result.tzinfo is None else result.tz_convert(BRISBANE)
        except (ValueError, TypeError):
            return pd.NaT
    return pd.to_datetime(values.map(convert), utc=True).dt.tz_convert(BRISBANE)


def load_inputs(root, issued_at, bounds=None):
    """Read a consistent SQLite transaction and a frozen CSV byte string, no backfill."""
    root = Path(root)
    source_hashes = {}
    chunks = []
    context = pd.DataFrame(columns=['source', 'observed_date', 'value', 'known_at'])
    csv_path = root / 'brisbane_fuel_live_collection.csv'
    if csv_path.is_file():
        raw = csv_path.read_bytes()
        source_hashes['collection_csv_sha256'] = sha(raw)
        frame = pd.read_csv(io.BytesIO(raw), dtype={'site_id': str}, low_memory=False)
        frame['collection_source'] = 'collection_csv'
        chunks.append(frame)
    db = root / 'fuel_data.db'
    if db.is_file():
        with closing(sqlite3.connect(db.resolve().as_uri() + '?mode=ro', uri=True)) as c:
            c.execute('BEGIN')
            tables = {row[0] for row in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'snapshots' in tables:
                frame = pd.read_sql_query("SELECT site_id,price_cpl,reported_at,lat AS latitude,"
                    "lng AS longitude,state,scraped_at FROM snapshots WHERE state='QLD'", c)
                frame['site_id'] = frame.site_id.astype(str)
                frame['collection_source'] = 'sqlite_snapshots'
                source_hashes['sqlite_snapshot_rows_sha256'] = sha(canonical(records(frame)))
                chunks.append(frame)
            if 'context_vintages' in tables:
                context = pd.read_sql_query('SELECT source,observed_date,value,known_at '
                    'FROM context_vintages ORDER BY source,observed_date,known_at', c)
    columns = ['site_id','price_cpl','latitude','longitude','state','scraped_at','collection_source']
    raw_rows = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=columns)
    bounds = dict(config.ANALYSIS_BOUNDS if bounds is None else bounds)
    selected = raw_rows.loc[raw_rows.state.eq('QLD')
        & pd.to_numeric(raw_rows.latitude, errors='coerce').between(bounds['lat_min'],bounds['lat_max'])
        & pd.to_numeric(raw_rows.longitude, errors='coerce').between(bounds['lng_min'],bounds['lng_max'])].copy()
    selected['scraped'] = local_times(selected.scraped_at)
    selected = selected.loc[selected.scraped.notna() & (selected.scraped <= issued_at)].copy()
    selected['date'] = selected.scraped.dt.tz_localize(None).dt.normalize()
    if not context.empty:
        context['known_timestamp'] = local_times(context.known_at)
        context = context.loc[context.known_timestamp.notna()
            & (context.known_timestamp <= issued_at)].drop(columns='known_timestamp')
    return selected, context, {'source_hashes':source_hashes, 'bounds':bounds,
                              'raw_rows_are_actual_collection':True,
                              'context_known_at_timezone_if_naive':'Australia/Brisbane'}


def build_daily(raw_rows, completed_before, policy=None):
    policy=POLICY if policy is None else policy
    rows = raw_rows.loc[raw_rows.date < pd.Timestamp(completed_before)].copy()
    if rows.empty:
        return pd.DataFrame(), rows
    rows['price_cpl'] = pd.to_numeric(rows.price_cpl, errors='coerce')
    # Deterministic tie: database row wins equal collection timestamps.
    rows = rows.sort_values(['scraped','collection_source'],kind='stable')
    rows = rows.drop_duplicates(['date','site_id'],keep='last')
    rows = rows.loc[rows.price_cpl.between(*policy['price_range_cpl'])]
    if rows.empty:
        return pd.DataFrame(),rows
    panel = rows.pivot(index='date',columns='site_id',values='price_cpl')
    daily = daily_features(panel).rename(columns={'price_cpl':'median','mean_cpl':'mean',
        'stations':'station_count','hiked5_share':'share_rising','cut5_share':'share_falling'})
    daily['last_capture'] = rows.groupby('date').scraped.max()
    daily['median_station_capture'] = rows.groupby('date').scraped.median()
    daily['provenance'] = 'collected'
    return daily, rows


def day_exclusions(daily, day, policy):
    if daily.empty or pd.Timestamp(day) not in daily.index:
        return ['missing_collected_day']
    row = daily.loc[pd.Timestamp(day)]
    reasons = []
    if row.provenance != 'collected':
        reasons.append('non_collected_provenance')
    if int(row.station_count) < policy['minimum_stations']:
        reasons.append('insufficient_station_coverage')
    # Median, not maximum: one late station cannot make the whole market current.
    if pd.Timestamp(row.median_station_capture).hour < policy['closing_hour']:
        reasons.append('incomplete_closing_capture')
    if not math.isfinite(float(row['median'])):
        reasons.append('invalid_daily_price')
    return reasons


def baseline_candidates(frame, upstream, origin, horizon, prior_losses=None):
    current = float(frame.loc[origin,'median'])
    values = frame['median'].reindex(pd.date_range(origin-pd.Timedelta(days=6),origin))
    slope = float(values.diff().median())
    return {'persistence':{'prediction':current,'state':{}},
        'damped_trend':{'prediction':float(current+np.clip(slope,-4,4)*sum(.8**k for k in range(horizon))),
                        'state':{'median_daily_change_cpl':slope,'damping':.8,'slope_cap':4}}}


def code_identity(callback):
    modules = {Path(__file__).resolve(), ROOT/'research'/'prepare_data.py', ROOT/'config.py'}
    file = inspect.getsourcefile(callback)
    if file:
        modules.add(Path(file).resolve())
    hashes = {str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else path.name:
              sha(path.read_bytes()) for path in modules}
    return {'callable':callback.__module__+':'+callback.__name__, 'files':hashes,
            'sha256':sha(canonical(hashes)), 'python':platform.python_version(),
            'numpy':np.__version__, 'pandas':pd.__version__}


def make_trial_definition(identity, policy, bounds):
    definition={'schema_version':1,'callback':identity['callable'],
        'code_sha256':identity['sha256'],'code':identity,
        'policy':policy,'bounds':bounds,'horizons':list(HORIZONS),
        'training_protocol':'optional frozen provenance corpus; live collection overrides overlap'}
    return {**definition,'trial_definition_sha256':sha(canonical(definition))}


def read_trial_definition(trial):
    path=Path(trial)/'trial_definition.json'
    if not path.exists():
        return None
    definition=read_record(path)
    content={k:v for k,v in definition.items() if k!='trial_definition_sha256'}
    if sha(canonical(content)) != definition.get('trial_definition_sha256'):
        raise ValueError('Trial definition integrity check failed; preserve it and use a new trial directory.')
    return definition


def enforce_trial_definition(trial, definition, register=False):
    if register:
        immutable_write(Path(trial)/'trial_definition.json',canonical(definition))
    existing=read_trial_definition(trial)
    if existing is not None and existing != definition:
        raise ValueError('Callback, code, environment, policy, bounds or horizons changed; '
                         'use a new trial directory. Existing trial evidence is retained.')


def training_inputs(path, trial, issued_at, origin):
    """Freeze a corpus held now; its rows remain retrospective training evidence."""
    if path is None:
        return pd.DataFrame(), None
    path=Path(path)
    raw=path.read_bytes()
    digest=sha(raw)
    frozen=Path(trial)/'training'/(digest+'.csv')
    immutable_write(frozen,raw)
    if sha(frozen.read_bytes()) != digest:
        raise ValueError('Training corpus integrity check failed.')
    frame=pd.read_csv(io.BytesIO(raw),parse_dates=['date']).set_index('date').sort_index()
    frame=frame.rename(columns={'price_cpl':'median','mean_cpl':'mean','stations':'station_count',
        'hiked5_share':'share_rising','cut5_share':'share_falling'})
    if frame.index.has_duplicates or 'median' not in frame or 'provenance' not in frame:
        raise ValueError('Training corpus requires unique dates, price and explicit provenance.')
    if not frame.provenance.isin(['reported','collected']).all():
        raise ValueError('Unrecognised historical training provenance.')
    frame=frame.loc[frame.index<=origin].copy()
    frame['input_role']='historical_training'
    return frame, {'sha256':digest,'frozen_file':'training/'+digest+'.csv',
        'available_by_utc':issued_at.isoformat(),'source_filename':path.name,'rows':len(frame),
        'provenance_counts':{str(k):int(v) for k,v in frame.provenance.value_counts().items()},
        'note':'Acquired before this issuance; not proof it was held at historical training origins.'}


def freeze_inputs(trial, raw_rows, context, daily, metadata):
    raw = raw_rows.copy()
    for col in ['scraped','date']:
        if col in raw:
            raw[col] = raw[col].astype(str)
    exported = daily.reset_index().copy()
    for col in ['date','last_capture','median_station_capture']:
        if col in exported:
            exported[col] = exported[col].astype(str)
    payload = {'actual_collection_rows':records(raw), 'context_vintages':records(context),
               'derived_daily':records(exported), 'metadata':metadata}
    data = canonical(payload)
    digest = sha(data)
    target = Path(trial)/'inputs'/(digest+'.json.gz')
    immutable_write(target,gzip.compress(data,mtime=0))
    if sha(gzip.decompress(target.read_bytes())) != digest:
        raise ValueError('Frozen input integrity check failed.')
    return digest


def issue(root=ROOT, trial=None, callback=baseline_candidates, now=None, policy=None,
          training_corpus=None):
    issued_at = stamp(now)
    local = issued_at.astimezone(BRISBANE)
    origin = pd.Timestamp(local.date()-timedelta(days=1))
    trial = Path(trial or Path(root)/'.local'/'research-shadow-trial')
    destination = trial/'forecasts'/(origin.strftime('%Y-%m-%d')+'.json')
    if destination.is_file():
        return {'status':'retained','record':read_record(destination)}
    policy = dict(POLICY if policy is None else policy)
    rows, context, metadata = load_inputs(root, issued_at)
    daily, _ = build_daily(rows, local.date(),policy)
    reasons = day_exclusions(daily,origin,policy)
    if not reasons:
        for date in pd.date_range(origin-pd.Timedelta(days=policy['model_lookback_days']-1),origin):
            bad = day_exclusions(daily,date,policy)
            if bad:
                reasons.extend(f'lookback_{date:%Y-%m-%d}:{reason}' for reason in bad)
    metadata['policy'] = policy
    metadata['issued_at_utc'] = issued_at.isoformat()
    historical,training_manifest=training_inputs(training_corpus,trial,issued_at,origin)
    metadata['historical_training_corpus']=training_manifest
    digest = freeze_inputs(trial,rows,context,daily,metadata)
    identity = code_identity(callback)
    definition=make_trial_definition(identity,policy,metadata['bounds'])
    record = {'schema_version':1, 'issued_at_utc':issued_at.isoformat(),
        'issued_at_brisbane':local.isoformat(), 'origin_date':origin.strftime('%Y-%m-%d'),
        'origin_day_completed':True, 'input_sha256':digest, 'code':identity, 'policy':policy,
        'code_sha256':identity['sha256'],
        'historical_training_corpus':training_manifest,
        'provenance':('prospective_collected_origin_with_separate_training' if training_manifest
                      else 'prospective_collection_only'), 'predictions':[],
        'intervals':None, 'interval_note':'No prospective uncertainty calibration established.'}
    if reasons:
        record.update(status='withheld', exclusions=sorted(set(reasons)),
            latest_completed_collected_day=None if daily.empty else daily.index.max().strftime('%Y-%m-%d'))
    else:
        # Existing trials cannot silently switch algorithms or eligibility rules.
        # Registration waits until a nonempty valid forecast actually exists.
        enforce_trial_definition(trial,definition)
        # Only completed collected days are passed. No synthetic future rows/labels.
        model_frame = daily.loc[:origin].copy()
        model_frame['input_role']='live_collected'
        if not historical.empty:
            # An optional corpus fills historical training dates, never a date
            # observed locally, and never the pre-merge current-window gates.
            model_frame=pd.concat([historical.loc[~historical.index.isin(model_frame.index)],
                                   model_frame]).sort_index()
        callback_context=context.copy()
        if not callback_context.empty:
            callback_context['known_at']=local_times(callback_context.known_at).map(lambda t:t.isoformat())
        prior_losses = load_prior_losses(trial,issued_at,
            definition['trial_definition_sha256'],identity['sha256'])
        for horizon in HORIZONS:
            candidates = callback(model_frame.copy(),callback_context.copy(),origin,horizon,prior_losses=prior_losses)
            for name,result in candidates.items():
                result = {'prediction':result,'state':{}} if isinstance(result,(int,float,np.number)) else result
                prediction = float(result['prediction'])
                if not math.isfinite(prediction) or not 0 < prediction < 1000:
                    raise ValueError('Candidate returned an invalid price; nothing issued.')
                parameters = result.get('state',{})
                record['predictions'].append({'model':str(name),'horizon':horizon,
                    'target_date':(origin+pd.Timedelta(days=horizon)).strftime('%Y-%m-%d'),
                    'prediction_cpl':prediction,'state':parameters,
                    'model_sha256':sha(canonical({'name':str(name),'code':identity,'state':parameters}))})
        if not record['predictions']:
            record.update(status='withheld',exclusions=['no_eligible_candidates'])
        else:
            enforce_trial_definition(trial,definition,register=True)
            record.update(status='issued',exclusions=[],
                trial_definition_sha256=definition['trial_definition_sha256'])
    data = canonical(record)
    if record['status']=='withheld':
        destination = trial/'exclusions'/(issued_at.strftime('%Y%m%dT%H%M%S%fZ')+'-'+sha(data)[:12]+'.json')
    if not immutable_write(destination,data):
        return {'status':'retained','record':read_record(destination)}
    return {'status':record['status'],'record':record}


def load_prior_losses(trial, issued_at, definition_sha256, code_sha256):
    out=[]
    for path in sorted((Path(trial)/'scores').glob('*.json')):
        record=read_record(path)
        if (record.get('trial_definition_sha256') == definition_sha256
                and record.get('code_sha256') == code_sha256
                and pd.Timestamp(record['scored_at_utc']) <= issued_at):
            out.append(record)
    return out


def score(root=ROOT, trial=None, now=None):
    scored_at=stamp(now)
    trial=Path(trial or Path(root)/'.local'/'research-shadow-trial')
    definition=read_trial_definition(trial)
    evaluation_policy=definition['policy'] if definition else POLICY
    rows,context,metadata=load_inputs(root,scored_at,definition['bounds'] if definition else None)
    daily,_=build_daily(rows,scored_at.astimezone(BRISBANE).date(),evaluation_policy)
    metadata['policy']=evaluation_policy
    made=0
    withheld=[]
    digest=None
    for path in sorted((trial/'forecasts').glob('*.json')):
        forecast_bytes=path.read_bytes()
        forecast=json.loads(forecast_bytes)
        # Reject tampered/backdated files and never score outcomes known at issuance.
        if forecast.get('status')!='issued':
            continue
        if (definition is None
                or forecast.get('trial_definition_sha256') != definition['trial_definition_sha256']
                or forecast.get('code_sha256') != definition['code_sha256']):
            withheld.append({'forecast_file':path.name,'exclusions':['missing_or_mismatched_trial_definition']})
            continue
        issued=pd.Timestamp(forecast['issued_at_utc'])
        if issued > scored_at:
            continue
        for point in forecast['predictions']:
            day=pd.Timestamp(point['target_date'])
            end=day.tz_localize(BRISBANE)+pd.Timedelta(days=1)
            if end <= issued or end > scored_at:
                continue
            key=sha(canonical({'forecast_sha256':sha(forecast_bytes),'model':point['model'],
                               'horizon':point['horizon']}))
            destination=trial/'scores'/(key+'.json')
            if destination.exists():
                continue
            reasons=day_exclusions(daily,day,forecast['policy'])
            if reasons:
                withheld.append({'target_date':point['target_date'],'exclusions':reasons})
                continue
            if digest is None:
                metadata['scored_at_utc']=scored_at.isoformat()
                digest=freeze_inputs(trial,rows,context,daily,metadata)
            actual=float(daily.loc[day,'median'])
            record={'schema_version':1,'scored_at_utc':scored_at.isoformat(),
                'scored_at_brisbane':scored_at.astimezone(BRISBANE).isoformat(),
                'issued_at_utc':forecast['issued_at_utc'],'origin_date':forecast['origin_date'],
                'forecast_sha256':sha(forecast_bytes),'outcome_input_sha256':digest,
                'trial_definition_sha256':forecast['trial_definition_sha256'],
                'code_sha256':forecast['code_sha256'],
                **point,'actual_cpl':actual,'error_cpl':point['prediction_cpl']-actual,
                'absolute_error_cpl':abs(point['prediction_cpl']-actual),
                'outcome_provenance':'collected','outcome_last_capture':str(daily.loc[day,'last_capture']),
                'note':'First valid collected outcome retained; corrections require a separate audit.'}
            made+=int(immutable_write(destination,canonical(record)))
    return {'status':'scored','new_scores':made,'unscorable_outcomes':withheld,
            'scored_at_utc':scored_at.isoformat()}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['issue','score'])
    parser.add_argument('--root',type=Path,default=ROOT)
    parser.add_argument('--trial',type=Path)
    parser.add_argument('--challenger',help='Optional local module:function with predict_candidates protocol')
    parser.add_argument('--training-corpus',type=Path,
                        help='Optional daily CSV held now, frozen with provenance; never fills live-origin gates')
    args=parser.parse_args()
    callback=baseline_candidates
    if args.challenger:
        module,function=args.challenger.split(':',1)
        callback=getattr(importlib.import_module(module),function)
    result=issue(args.root,args.trial,callback,training_corpus=args.training_corpus) if args.action=='issue' else score(args.root,args.trial)
    print(json.dumps(result,indent=2))


if __name__=='__main__':
    main()
