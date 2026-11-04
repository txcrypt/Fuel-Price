"""Prepare target-definition sensitivity inputs from frozen research exports.

Does not modify the primary panels or consult the operational database.
"""
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

from prepare_data import daily_features
from qld_history_import import _daily_last_reports


def prepare(output,cohort_start='2025-01-15',cohort_end='2026-09-28'):
    panel = pd.read_csv(output/'reported_panel.csv',index_col=0,parse_dates=True)
    dev = panel.loc[cohort_start:cohort_end]
    coverage = dev.notna().mean()
    cohort = coverage.index[coverage.ge(.90)]
    fixed = daily_features(panel[cohort])
    sufficient = fixed.stations.ge(.90*len(cohort))
    fixed.loc[~sufficient, fixed.columns.difference(['stations','matched_stations'])] = float('nan')
    fixed['provenance'] = 'reported_fixed_cohort'
    fixed['coverage_fraction'] = fixed.stations / len(cohort)
    fixed.to_csv(output/'daily_reported_fixed_cohort.csv')
    coverage.rename('development_coverage_fraction').to_csv(output/'fixed_cohort_station_coverage.csv')
    events = pd.read_csv(output/'reported_events.csv',dtype={'site_id':str})
    ordered = events.sort_values(['site_id','reported_at_utc','price_cpl'],kind='stable').drop_duplicates(['site_id','reported_at_utc'],keep='last')
    _, wide = _daily_last_reports(ordered)
    result = {'cohort_selection_start':cohort_start, 'cohort_selection_end':cohort_end,
              'station_inclusion_coverage':.90, 'row_minimum_coverage':.90,
              'cohort_size':len(cohort), 'cohort_site_ids':list(cohort),
              'complete_price_days':int(sufficient.sum()),
              'post_selection_complete_price_days':int(sufficient.loc[sufficient.index>pd.Timestamp(cohort_end)].sum()),
              'warning':'Membership uses the full specified selection window; scores inside that window are diagnostic. Membership after the selection cutoff uses no future selection data.'}
    sensitivity=[]
    baseline=panel.median(axis=1)
    for expiry,label in [(7,'7day'),(30,'30day'),(None,'noexpiry')]:
        held=(wide.ffill() if expiry is None else wide.ffill(limit=expiry)).mask(lambda f:f.eq(0))
        frame=daily_features(held)
        frame['provenance']='reported_'+label
        frame.to_csv(output/('daily_reported_'+label+'.csv'))
        delta=(frame.price_cpl-baseline).abs()
        sensitivity.append({'expiry_days':expiry,'median_stations':float(frame.stations.median()),
                            'days_median_diff_gt_1cpl':int(delta.gt(1).sum()),
                            'mean_absolute_median_diff_cpl':float(delta.mean()),
                            'max_absolute_median_diff_cpl':float(delta.max())})
    result['expiry_sensitivity']=sensitivity
    result['exports']={p.name:{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size}
                       for p in [output/'daily_reported_fixed_cohort.csv',output/'daily_reported_7day.csv',output/'daily_reported_30day.csv',output/'daily_reported_noexpiry.csv']}
    (output/'sensitivity_data_manifest.json').write_text(json.dumps(result,indent=2))
    print(json.dumps({k:v for k,v in result.items() if k!='cohort_site_ids'},indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=Path(__file__).resolve().parents[1]/'.local/research-2026-11-04')
    parser.add_argument('--cohort-start',default='2025-01-15')
    parser.add_argument('--cohort-end',default='2026-09-28')
    args=parser.parse_args()
    prepare(args.output,args.cohort_start,args.cohort_end)
