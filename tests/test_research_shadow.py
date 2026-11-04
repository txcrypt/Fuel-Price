import gzip
from contextlib import closing
import json
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from research.shadow_trial import (HORIZONS, POLICY, BRISBANE, issue, score,
    load_inputs, immutable_write, sha, load_prior_losses, canonical, code_identity)


class ShadowTrialTests(unittest.TestCase):
    def setUp(self):
        base=Path(__file__).resolve().parents[1]/'.local'/'research-shadow-tests'
        base.mkdir(parents=True,exist_ok=True)
        self.tmp=tempfile.TemporaryDirectory(dir=base)
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.trial=self.root/'trial'
        self.policy={**POLICY,'minimum_stations':2}
        self.now=datetime(2026,11,5,1,0,tzinfo=timezone.utc) # 11:00 AEST
        self.rows=[]
        for day in pd.date_range('2026-10-29','2026-11-04'):
            self.add_day(day,180+(day-pd.Timestamp('2026-10-29')).days)
        self.write()

    def add_day(self,day,price,hour=23):
        for site,offset in [('1',0),('2',2)]:
            self.rows.append({'site_id':site,'price_cpl':price+offset,'state':'QLD',
                'latitude':-27.4,'longitude':153.0,'reported_at':str(day),
                'scraped_at':f'{pd.Timestamp(day):%Y-%m-%d} {hour:02}:30:00'})

    def write(self):
        pd.DataFrame(self.rows).to_csv(self.root/'brisbane_fuel_live_collection.csv',index=False)

    def issue(self,**kwargs):
        return issue(self.root,self.trial,now=self.now,policy=self.policy,**kwargs)

    def test_exact_issuance_completed_origin_and_four_horizons(self):
        before=(self.root/'brisbane_fuel_live_collection.csv').read_bytes()
        result=self.issue()
        self.assertEqual(result['status'],'issued')
        record=result['record']
        self.assertEqual(record['issued_at_utc'],'2026-11-05T01:00:00+00:00')
        self.assertEqual(record['issued_at_brisbane'],'2026-11-05T11:00:00+10:00')
        self.assertEqual(record['origin_date'],'2026-11-04')
        self.assertEqual({p['horizon'] for p in record['predictions']},set(HORIZONS))
        self.assertEqual(len(record['predictions']),8)
        self.assertEqual(before,(self.root/'brisbane_fuel_live_collection.csv').read_bytes())
        path=self.trial/'inputs'/(record['input_sha256']+'.json.gz')
        raw=gzip.decompress(path.read_bytes())
        self.assertEqual(sha(raw),record['input_sha256'])
        frozen=json.loads(raw)
        self.assertEqual(len(frozen['actual_collection_rows']),14)
        self.assertTrue(record['code']['sha256'])
        self.assertTrue(all(p['model_sha256'] for p in record['predictions']))
        definition=json.loads((self.trial/'trial_definition.json').read_bytes())
        self.assertEqual(record['trial_definition_sha256'],definition['trial_definition_sha256'])
        self.assertEqual(record['code_sha256'],definition['code_sha256'])

    def test_first_forecast_survives_revised_inputs_and_different_callback(self):
        self.issue()
        path=self.trial/'forecasts'/'2026-11-04.json'
        first=path.read_bytes()
        self.rows[-1]['price_cpl']=300
        self.write()
        def forbidden(*args,**kwargs):
            raise AssertionError('Existing forecast must be retained before modelling.')
        self.assertEqual(self.issue(callback=forbidden)['status'],'retained')
        self.assertEqual(path.read_bytes(),first)

    def test_partial_today_cannot_replace_missing_yesterday(self):
        self.rows=[r for r in self.rows if not r['scraped_at'].startswith('2026-11-04')]
        self.add_day('2026-11-05',230,hour=10)
        self.write()
        result=self.issue()
        self.assertEqual(result['status'],'withheld')
        self.assertIn('missing_collected_day',result['record']['exclusions'])
        self.assertEqual(result['record']['latest_completed_collected_day'],'2026-11-03')
        self.assertFalse((self.trial/'forecasts'/'2026-11-04.json').exists())
        self.assertFalse((self.trial/'trial_definition.json').exists())
        self.assertEqual(len(list((self.trial/'exclusions').glob('*.json'))),1)

    def test_one_late_station_does_not_establish_market_close(self):
        self.rows=[r for r in self.rows if not r['scraped_at'].startswith('2026-11-04')]
        self.add_day('2026-11-04',180,hour=10)
        self.rows[-1]['scraped_at']='2026-11-04 23:30:00'
        self.write()
        result=self.issue()
        self.assertEqual(result['status'],'withheld')
        self.assertIn('incomplete_closing_capture',result['record']['exclusions'])

    def test_challenger_gets_only_completed_collected_rows_and_no_future_vintages(self):
        self.add_day('2026-11-05',300,hour=10)
        self.write()
        with closing(sqlite3.connect(self.root/'fuel_data.db')) as c:
            c.execute('CREATE TABLE context_vintages(source TEXT,observed_date TEXT,value REAL,known_at TEXT)')
            c.executemany('INSERT INTO context_vintages VALUES (?,?,?,?)',[
                ('aip','2026-11-03',180,'2026-11-04 17:00:00'),
                ('aip','2026-11-03',999,'2026-11-06 17:00:00')])
            c.execute('CREATE TABLE reported_price_events(day TEXT,price REAL)')
            c.execute("INSERT INTO reported_price_events VALUES ('2026-11-04',999)")
            c.commit()
        def check(frame,upstream,origin,horizon,prior_losses=None):
            self.assertTrue((frame.index<=origin).all())
            self.assertTrue(frame.provenance.eq('collected').all())
            self.assertEqual(upstream.value.tolist(),[180])
            self.assertEqual(upstream.known_at.iloc[0],'2026-11-04T17:00:00+10:00')
            self.assertEqual(prior_losses,[])
            return {'challenger':{'prediction':float(frame.loc[origin,'median']),
                                  'state':{'frozen':True}}}
        result=self.issue(callback=check)
        self.assertEqual(result['status'],'issued')
        self.assertEqual(len(result['record']['predictions']),4)

    def test_scoring_uses_separate_mature_collected_outcomes_and_retains_first(self):
        self.issue()
        forecasts=(self.trial/'forecasts'/'2026-11-04.json').read_bytes()
        self.add_day('2026-11-05',190)
        self.write()
        # Target November 5 is still incomplete at 23:00 Brisbane.
        early=datetime(2026,11,5,13,0,tzinfo=timezone.utc)
        self.assertEqual(score(self.root,self.trial,now=early)['new_scores'],0)
        later=datetime(2026,11,6,1,0,tzinfo=timezone.utc)
        result=score(self.root,self.trial,now=later)
        self.assertEqual(result['new_scores'],2)
        saved={p.name:p.read_bytes() for p in (self.trial/'scores').glob('*.json')}
        outcomes=[json.loads(data) for data in saved.values()]
        self.assertEqual({r['actual_cpl'] for r in outcomes},{191})
        self.assertTrue(all(r['outcome_provenance']=='collected' for r in outcomes))
        forecast_record=json.loads(forecasts)
        self.assertTrue(all(r['trial_definition_sha256']==forecast_record['trial_definition_sha256']
                            and r['code_sha256']==forecast_record['code_sha256'] for r in outcomes))
        self.rows[-1]['price_cpl']=300
        self.write()
        self.assertEqual(score(self.root,self.trial,now=later)['new_scores'],0)
        self.assertEqual(saved,{p.name:p.read_bytes() for p in (self.trial/'scores').glob('*.json')})
        self.assertEqual(forecasts,(self.trial/'forecasts'/'2026-11-04.json').read_bytes())

    def test_read_only_database_and_atomic_no_replace(self):
        db=self.root/'fuel_data.db'
        with closing(sqlite3.connect(db)) as c:
            c.execute('CREATE TABLE reported_price_events(day TEXT,price REAL)')
            c.commit()
        before=db.read_bytes()
        load_inputs(self.root,self.now)
        self.assertEqual(before,db.read_bytes())
        path=self.root/'immutable.json'
        self.assertTrue(immutable_write(path,b'first'))
        self.assertFalse(immutable_write(path,b'second'))
        self.assertEqual(path.read_bytes(),b'first')

    def test_naive_clock_is_rejected(self):
        with self.assertRaises(ValueError):
            issue(self.root,self.trial,now=datetime(2026,11,5))

    def test_real_mechanism_callback_records_explicit_missing_input_skips(self):
        from research.mechanism_models import predict_candidates
        result=self.issue(callback=predict_candidates)
        self.assertEqual(result['status'],'issued')
        points=result['record']['predictions']
        self.assertEqual({p['model'] for p in points},{'persistence','damped_trend','online_selector'})
        self.assertEqual(len(points),12)
        selector=next(p for p in points if p['model']=='online_selector')
        self.assertTrue(selector['state']['excluded_candidates'])

    def test_historical_training_frozen_separately_never_replaces_live_or_fills_gates(self):
        corpus=self.root/'history.csv'
        pd.DataFrame({'date':['2026-10-28','2026-11-04','2026-11-06'],
            'price_cpl':[170,999,888],'provenance':['reported']*3}).to_csv(corpus,index=False)
        def check(frame,upstream,origin,horizon,prior_losses=None):
            self.assertEqual(frame.loc[pd.Timestamp('2026-10-28'),'provenance'],'reported')
            self.assertEqual(frame.loc[origin,'median'],187)
            self.assertEqual(frame.loc[origin,'provenance'],'collected')
            self.assertNotIn(pd.Timestamp('2026-11-06'),frame.index)
            return {'challenger':170.0}
        result=self.issue(callback=check,training_corpus=corpus)
        manifest=result['record']['historical_training_corpus']
        self.assertEqual(manifest['sha256'],sha(corpus.read_bytes()))
        self.assertEqual((self.trial/manifest['frozen_file']).read_bytes(),corpus.read_bytes())
        # A new trial with yesterday absent cannot use corpus's reported row.
        self.rows=[r for r in self.rows if not r['scraped_at'].startswith('2026-11-04')]
        self.write()
        result=issue(self.root,self.root/'other-trial',now=self.now,policy=self.policy,training_corpus=corpus)
        self.assertEqual(result['status'],'withheld')
        self.assertIn('missing_collected_day',result['record']['exclusions'])

    def test_new_day_rejects_changed_callback_policy_or_code_without_replacing_definition(self):
        self.issue()
        definition=(self.trial/'trial_definition.json').read_bytes()
        self.add_day('2026-11-05',188)
        self.write()
        later=datetime(2026,11,6,1,0,tzinfo=timezone.utc)
        def changed(*args,**kwargs):
            raise AssertionError('An incompatible callback must be rejected before running.')
        with self.assertRaisesRegex(ValueError,'new trial directory'):
            issue(self.root,self.trial,callback=changed,now=later,policy=self.policy)
        with self.assertRaisesRegex(ValueError,'new trial directory'):
            issue(self.root,self.trial,now=later,policy={**self.policy,'minimum_stations':1})
        import research.shadow_trial as shadow
        original=shadow.code_identity(shadow.baseline_candidates)
        with patch('research.shadow_trial.code_identity',return_value={**original,'sha256':'changed-code'}):
            with self.assertRaisesRegex(ValueError,'new trial directory'):
                issue(self.root,self.trial,now=later,policy=self.policy)
        self.assertEqual((self.trial/'trial_definition.json').read_bytes(),definition)
        self.assertFalse((self.trial/'forecasts'/'2026-11-05.json').exists())

    def test_changing_learned_state_keeps_same_trial_definition(self):
        def adaptive(frame,upstream,origin,horizon,prior_losses=None):
            current=float(frame.loc[origin,'median'])
            return {'adaptive':{'prediction':current,'state':{'fitted_level':current}}}
        first=self.issue(callback=adaptive)['record']
        self.add_day('2026-11-05',190)
        self.write()
        second=issue(self.root,self.trial,callback=adaptive,
            now=datetime(2026,11,6,1,0,tzinfo=timezone.utc),policy=self.policy)['record']
        self.assertEqual(first['trial_definition_sha256'],second['trial_definition_sha256'])
        self.assertNotEqual(first['predictions'][0]['model_sha256'],second['predictions'][0]['model_sha256'])

    def test_prior_losses_exclude_missing_wrong_and_future_versions_but_allow_new_fitted_state(self):
        good={'model':'same_name','horizon':1,'absolute_error_cpl':1,
              'scored_at_utc':'2026-11-04T00:00:00+00:00',
              'trial_definition_sha256':'definition-A','code_sha256':'code-A',
              'model_sha256':'legitimately-varying-fitted-state'}
        cases=[good,{**good,'trial_definition_sha256':'definition-B'},
               {**good,'code_sha256':'code-B'},
               {k:v for k,v in good.items() if k not in ['trial_definition_sha256','code_sha256']},
               {**good,'scored_at_utc':'2026-11-06T00:00:00+00:00'}]
        for i,record in enumerate(cases):
            immutable_write(self.trial/'scores'/f'{i}.json',canonical(record))
        self.assertEqual(load_prior_losses(self.trial,self.now,'definition-A','code-A'),[good])

    def test_scoring_uses_registered_bounds_even_if_current_app_configuration_changes(self):
        self.issue()
        self.add_day('2026-11-05',190)
        self.write()
        changed={'lat_min':0,'lat_max':1,'lng_min':0,'lng_max':1}
        with patch('research.shadow_trial.config.ANALYSIS_BOUNDS',changed):
            result=score(self.root,self.trial,now=datetime(2026,11,6,1,0,tzinfo=timezone.utc))
        self.assertEqual(result['new_scores'],2)


if __name__=='__main__':
    unittest.main()
