"""Acquire official 2023/24 reports into isolated, reproducible research exports.

Never opens the operational database. Raw monthly files, filtered events, source
metadata and checksums are retained. --prepare-only uses already parsed events.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd
import requests

from prepare_data import ROOT, daily_features
import config
import qld_history_import as qhi

DATASETS={2023:'fuel-price-reporting',2024:'fuel-price-reporting-2024'}
EXACT_FORMATS=('%d/%m/%Y %H:%M','%d/%m/%Y %H:%M:%S',
               '%Y-%m-%dT%H:%M:%S','%Y-%m-%dT%H:%M:%S.%f',
               '%Y-%m-%d %H:%M:%S','%Y-%m-%dT%H:%M:%S%z',
               '%Y-%m-%dT%H:%M:%S.%f%z')


def parse_historical_at(values):
    parsed=pd.Series(pd.NaT,index=values.index,dtype='datetime64[ns, UTC]')
    for fmt in EXACT_FORMATS:
        missing=parsed.isna() & values.notna()
        if not missing.any(): break
        parsed.loc[missing]=pd.to_datetime(values.loc[missing],format=fmt,utc=True,errors='coerce')
    bad=parsed.isna() & values.notna()
    if bad.any():
        examples=values.loc[bad].drop_duplicates().head(4).tolist()
        raise ValueError(f'Unparsed historical report timestamps: {examples}')
    return parsed


def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()


def acquire(base):
    output=base/'extended';rawdir=output/'raw';eventdir=output/'monthly_events'
    rawdir.mkdir(parents=True,exist_ok=True);eventdir.mkdir(exist_ok=True)
    manifest_path=output/'acquisition_manifest.json'
    manifest=json.loads(manifest_path.read_text()) if manifest_path.exists() else {'sources':[]}
    recorded={x['resource_id']:x for x in manifest['sources']}
    original_parser=qhi.parse_reported_at
    qhi.parse_reported_at=parse_historical_at
    try:
        for year,dataset in DATASETS.items():
            metadata_path=base/f'qld_metadata_{year}.json'
            if not metadata_path.exists():
                res=requests.get(qhi.CKAN_ACTION,params={'id':dataset},timeout=60)
                res.raise_for_status();metadata_path.write_text(json.dumps(res.json()['result'],indent=2))
            metadata=json.loads(metadata_path.read_text())
            resources=[r for r in metadata['resources'] if (r.get('format') or '').upper()=='CSV']
            if len(resources)!=12: raise ValueError(f'Expected12 monthlyCSV resources for{year}, got{len(resources)}')
            for resource in resources:
                rid=resource['id'];raw=rawdir/f'{year}_{rid}.csv';filtered=eventdir/f'{year}_{rid}.csv'
                old=recorded.get(rid)
                if old and raw.exists() and filtered.exists() and sha(raw)==old['sha256']:
                    print(f'CACHED {year} {resource["name"]}',flush=True);continue
                if not raw.exists():
                    temporary=raw.with_suffix('.part')
                    digest,size=qhi.download(resource['url'],temporary)
                    temporary.replace(raw)
                else: digest,size=sha(raw),raw.stat().st_size
                events,counts=qhi.read_events(raw)
                if events.empty: raise ValueError(f'No Brisbane U91 events parsed from {resource["name"]}')
                events['source_id']=rid
                events.to_csv(filtered,index=False)
                record={'year':year,'dataset':dataset,'resource_id':rid,'name':resource['name'],
                        'url':resource['url'],'published_metadata':resource.get('last_modified') or resource.get('created'),
                        'sha256':digest,'bytes':size,**counts,
                        'download_completed_at_utc':datetime.fromtimestamp(raw.stat().st_mtime,timezone.utc).isoformat(),
                        'event_sha256':sha(filtered),'first_report':str(events.reported_at_utc.min()),
                        'last_report':str(events.reported_at_utc.max()),'filtered_file':str(filtered.relative_to(output))}
                recorded[rid]=record
                manifest={'bounds':config.ANALYSIS_BOUNDS,'fuel_type':'Unleaded','raw_price_divisor':10,
                          'unavailable_raw':9999,'parser_formats':EXACT_FORMATS,
                          'attribution':qhi.ATTRIBUTION,'sources':list(recorded.values())}
                manifest_path.write_text(json.dumps(manifest,indent=2),encoding='utf-8')
                print(f'{year} {resource["name"]} bytes={size} read={counts["rows_read"]} kept={counts["rows_kept"]}',flush=True)
    finally:qhi.parse_reported_at=original_parser


def prepare(base):
    output=base/'extended';manifest=json.loads((output/'acquisition_manifest.json').read_text())
    frames=[pd.read_csv(base/'reported_events.csv',dtype={'site_id':str})]
    frames += [pd.read_csv(output/r['filtered_file'],dtype={'site_id':str}) for r in manifest['sources']]
    raw=pd.concat(frames,ignore_index=True)
    raw.to_csv(output/'reported_events_with_sources.csv',index=False)
    keys=['site_id','fuel_type','reported_at_utc','price_cpl']
    events=raw.drop_duplicates(keys).sort_values(['reported_at_utc','site_id','price_cpl'],kind='stable')
    events.to_csv(output/'reported_events.csv',index=False)
    ambiguous=events.loc[events.duplicated(['site_id','reported_at_utc'],keep=False)]
    ambiguous.to_csv(output/'ambiguous_report_timestamps.csv',index=False)
    ordered=events.sort_values(['site_id','reported_at_utc','price_cpl'],kind='stable').drop_duplicates(['site_id','reported_at_utc'],keep='last')
    _,wide=qhi._daily_last_reports(ordered)
    panel=wide.ffill(limit=14).mask(lambda f:f.eq(0));panel.index.name='date'
    panel.to_csv(output/'reported_panel.csv')
    daily=daily_features(panel);daily['provenance']='reported'
    daily['stations_carried_forward']=panel.notna().sum(axis=1)-(wide.notna() & wide.ne(0)).sum(axis=1)
    daily.to_csv(output/'daily_reported.csv')
    original=pd.read_csv(base/'daily_reported.csv',index_col=0,parse_dates=True)
    overlap=daily.index.intersection(original.index)
    delta=(daily.loc[overlap,'price_cpl']-original.loc[overlap,'price_cpl']).abs()
    summary={'sources_acquired':len(manifest['sources']),'source_bytes':sum(r['bytes'] for r in manifest['sources']),
             'source_raw_rows':sum(r['rows_read'] for r in manifest['sources']),
             'new_filtered_events':sum(r['rows_kept'] for r in manifest['sources']),
             'combined_raw_events':len(raw),'combined_unique_events':len(events),
             'exact_duplicate_events_removed':len(raw)-len(events),
             'ambiguous_timestamp_groups':ambiguous.groupby(['site_id','reported_at_utc']).ngroups,
             'ambiguous_policy':'maximum price among same station/report timestamp; true ordering unknown',
             'first_day':str(daily.index.min().date()),'last_day':str(daily.index.max().date()),
             'days':len(daily),'stations':panel.shape[1],
             'median_daily_station_count':float(daily.stations.median()),
             'minimum_daily_station_count':int(daily.stations.min()),
             'carry_forward_fraction':float(daily.stations_carried_forward.sum()/daily.stations.sum()),
             'original_overlap_days':len(overlap),'original_overlap_median_changes':int(delta.gt(0).sum()),
             'original_overlap_max_absolute_median_change_cpl':float(delta.max()),
             'primary_source_events_sha256':sha(base/'reported_events.csv'),
             'exports':{p.name:{'sha256':sha(p),'bytes':p.stat().st_size} for p in [output/'reported_events.csv',output/'reported_panel.csv',output/'daily_reported.csv']}}
    (output/'extended_summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    print(json.dumps(summary,indent=2),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base',type=Path,default=ROOT/'.local/research-2026-11-04')
    parser.add_argument('--prepare-only',action='store_true')
    args=parser.parse_args()
    if not args.prepare_only:acquire(args.base)
    prepare(args.base)
