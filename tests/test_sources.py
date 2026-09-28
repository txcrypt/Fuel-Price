from datetime import datetime, timedelta
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import requests

import numpy as np
import pandas as pd

from api_evidence import ApiEvidence, price_status, write_json
from context_sources import ContextSources, failure_category, observations_as_of, parse_source
from forecasting import context_evaluation
from fuel_engine import FuelEngine
from local_data import LocalData
from test_local import FakeEngine, snapshot


class Response:
    def __init__(self, text): self.text = text
    def raise_for_status(self): pass


SITE = {'S':123,'N':'Test site','A':'10 Test Road','B':7,'P':'4000','G1':10,'G2':20,
        'G3':1,'G4':0,'G5':0,'Lat':-27.4,'Lng':153.0,'M':'2026-01-01T00:00:00','GPI':'example','MO':''}
PRICE = {'SiteId':123,'FuelId':2,'Price':1800,'TransactionDateUtc':'2026-09-16T01:00:00','CollectionMethod':'Q'}
RESPONSES = {'sites':[SITE], 'prices':[PRICE], 'brands':[{'BrandId':7,'Name':'Test brand'}],
             'fuels':[{'FuelId':2,'Name':'Unleaded'},{'FuelId':3,'Name':'Diesel'}],
             'regions':[{'GeoRegionLevel':1,'GeoRegionId':10,'Name':'Test suburb'},
                        {'GeoRegionLevel':2,'GeoRegionId':20,'Name':'Brisbane'}]}
AIP = '<h2>Petrol (ULP, cents per litre, inclusive of GST)</h2><table><tr><th>Location</th><th>Tuesday, 1st September 2026</th><th>Wednesday, 2nd September 2026</th></tr><tr><td>Brisbane</td><td>160.2</td><td>161.3</td></tr></table><h2>Diesel</h2><table><tr><th>Location</th><th>Wednesday, 2nd September 2026</th></tr><tr><td>Brisbane</td><td>250</td></tr></table>'


def rss(target='USD', base='AUD', value='0.71', period='2026-09-02'):
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
 xmlns:cb="http://www.cbwiki.net/wiki/index.php/Specification_1.2/" xmlns="http://purl.org/rss/1.0/">
  <item><title>AU: {value} {target} = 1 {base}</title><cb:statistics rdf:parseType="Resource">
    <cb:exchangeRate rdf:parseType="Resource">
      <cb:observation rdf:parseType="Resource"><cb:value>{value}</cb:value><cb:unit>AUD</cb:unit></cb:observation>
      <cb:baseCurrency>{base}</cb:baseCurrency><cb:targetCurrency>{target}</cb:targetCurrency>
      <cb:observationPeriod rdf:parseType="Resource"><cb:frequency>daily</cb:frequency>
        <cb:period>{period}</cb:period></cb:observationPeriod>
    </cb:exchangeRate></cb:statistics></item>
</rdf:RDF>'''


RBA = rss()
EIA = '{"response": {"data": [{"period": "2026-09-02", "value": "130.8"}, {"period": "2026-09-01", "value": null}]}}'


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data = LocalData(self.temp.name, FakeEngine())

    def engine(self):
        return FuelEngine(token='test', cache_dir=Path(self.temp.name)/'cache')

    def fetch(self, endpoint, key):
        name = next(name for name,pair in FuelEngine.endpoints.items() if pair[0]==endpoint)
        return RESPONSES[name]

    def test_reference_cache_and_minute_price_limit_survive_restart(self):
        engine=self.engine()
        with patch.object(engine,'_get',side_effect=self.fetch) as fetch:
            first=engine.get_market_snapshot()
            second=engine.get_market_snapshot()
            self.assertEqual(fetch.call_count,5)
            self.assertEqual(first.scraped_at.iloc[0],second.scraped_at.iloc[0])
        restarted=self.engine()
        with patch.object(restarted,'_get',side_effect=AssertionError('No repeated calls')):
            restarted.get_market_snapshot()
        old=(datetime.now()-timedelta(minutes=2)).isoformat(timespec='seconds')
        write_json(engine.cache_dir/'prices.json', {'data':[PRICE],'retrieved_at':old})
        with patch.object(engine,'_get',side_effect=self.fetch) as fetch:
            engine.get_market_snapshot()
            self.assertEqual(fetch.call_count,1)
            self.assertIn('GetSitesPrices',fetch.call_args.args[0])

    def test_raw_metadata_other_grades_and_unavailability_are_preserved(self):
        raw={**RESPONSES,'retrieved_at':'2026-09-17T12:00:00',
             'prices':[PRICE,dict(PRICE,FuelId=3,Price=9999)]}
        evidence=self.data.api_evidence
        evidence.save(raw)
        evidence.save({**raw,'retrieved_at':'2026-09-17T13:00:00'})
        result=evidence.payload()
        site=result['sites'][0]
        self.assertEqual(site['brand'],'Test brand')
        self.assertEqual(site['suburb'],'Test suburb')
        self.assertEqual(site['raw_site'],SITE)
        self.assertEqual(site['fuels'][1]['status'],'unavailable')
        self.assertIsNone(site['fuels'][1]['price_cpl'])
        self.assertEqual(result['coverage']['price_events_saved'],2)
        with self.data.db._connect() as connection:
            row=connection.execute('SELECT first_seen,last_seen FROM api_price_events LIMIT 1').fetchone()
        self.assertEqual(row,('2026-09-17T12:00:00','2026-09-17T13:00:00'))

    def test_unavailable_latest_report_cannot_restore_older_price(self):
        raw={**RESPONSES,'retrieved_at':'2026-09-17T12:00:00',
             'prices':[PRICE,dict(PRICE,Price=9999,TransactionDateUtc='2026-09-17T01:00:00')]}
        self.data.api_evidence.save(raw)
        self.assertEqual(self.data.api_evidence.payload()['sites'][0]['u91_status'],'unavailable')
        engine=self.engine()
        def fetch(endpoint,key):
            return raw['prices'] if 'GetSitesPrices' in endpoint else self.fetch(endpoint,key)
        with patch.object(engine,'_get',side_effect=fetch):
            with self.assertRaisesRegex(RuntimeError,'no valid Queensland'):
                engine.get_market_snapshot()

    def test_utc_report_without_suffix_is_not_treated_as_brisbane_time(self):
        frame=snapshot()
        frame['reported_at']='2026-09-16T01:00:00'
        frame.to_csv(self.data.snapshot,index=False)
        self.assertEqual(self.data.dashboard()['stations'][0]['reported_at'],'2026-09-16T01:00:00.000000Z')

    def test_public_parsers_select_petrol_and_match_date_columns(self):
        self.assertEqual(parse_source('aip',AIP)[-1],{'date':'2026-09-02','value':161.3})
        with self.assertRaises(ValueError):
            parse_source('aip',AIP.replace('<td>161.3</td>',''))

    def test_rba_feed_is_read_as_usd_per_aud_or_rejected(self):
        self.assertEqual(parse_source('rba',RBA),[{'date':'2026-09-02','value':0.71}])
        # An inverted or re-based feed must fail rather than silently flip the rate's meaning.
        with self.assertRaises(ValueError):
            parse_source('rba',rss(base='USD',target='USD'))
        for broken in [rss(target='CNY'), '<p>Access denied</p>', RBA.replace('<cb:period>2026-09-02</cb:period>','')]:
            with self.assertRaises(ValueError):
                parse_source('rba',broken)

    def test_eia_series_skips_empty_values_and_rejects_a_missing_block(self):
        self.assertEqual(parse_source('eia_brent',EIA),[{'date':'2026-09-02','value':130.8}])
        for broken in ['{"error": "invalid api_key"}', '{"response": {"data": []}}', 'not json']:
            with self.assertRaises(ValueError):
                parse_source('eia_brent',broken)

    def test_missing_key_is_configuration_state_not_a_provider_failure(self):
        context=self.data.context
        with patch('config.EIA_API_KEY',''):
            with patch('context_sources.requests.get',side_effect=[Response(AIP),Response(RBA)]) as fetch:
                context.refresh()
                # Only the keyless sources are requested; no unauthenticated EIA call is attempted.
                self.assertEqual(fetch.call_count,2)
        self.assertEqual(context.state['eia_brent']['status'],'not_configured')
        source=next(s for s in context.payload({'history':[]})['sources'] if s['id']=='eia_brent')
        self.assertTrue(source['stale'])
        self.assertEqual(source['points'],[])

    def test_request_key_and_parameters_never_reach_stored_state_or_payload(self):
        context=self.data.context
        with patch('config.EIA_API_KEY','SECRET-KEY-VALUE'):
            with patch('context_sources.requests.get',side_effect=requests.ConnectionError('https://api.eia.gov/?api_key=SECRET-KEY-VALUE')):
                context.refresh()
            self.assertEqual(context.state['eia_brent']['status'],'error')
            serialized=json.dumps([context.state,context.payload({'history':[]})])
        self.assertNotIn('SECRET-KEY-VALUE',serialized)
        self.assertNotIn('api_key',serialized)

    def test_failed_context_refresh_retains_dates_and_pairs_only_same_day(self):
        context=self.data.context
        with patch('context_sources.requests.get',side_effect=[Response(AIP),Response(RBA),Response(EIA),Response(EIA)]) as fetch:
            context.refresh()
            context.refresh()
            self.assertEqual(fetch.call_count,4)
        for state in context.state.values():
            state['attempted_at']=(datetime.now()-timedelta(days=1)).isoformat()
        with patch('context_sources.requests.get',side_effect=requests.ConnectionError()):
            context.refresh()
        result=context.payload({'history':[{'date':'2026-09-02','price':180},{'date':'2026-09-03','price':190}]})
        self.assertEqual(result['sources'][0]['status'],'error')
        self.assertEqual(result['sources'][0]['points'][-1]['date'],'2026-09-02')
        self.assertEqual(result['paired_history'],[{'date':'2026-09-02','retail':180,'wholesale':161.3,'gap':18.7}])
        # The wholesale pairing must follow the AIP source, not whichever source is listed first.
        brent=next(s for s in result['sources'] if s['id']=='eia_brent')
        self.assertEqual(brent['points'][-1]['value'],130.8)
        self.assertEqual(brent['unit'],'USD per barrel')

    def test_source_health_reports_next_retry_and_a_category_that_cannot_echo_a_url(self):
        context = self.data.context
        failure = requests.ConnectionError('https://api.eia.gov/v2/?api_key=SECRET-KEY-VALUE refused')
        with patch('config.EIA_API_KEY', 'SECRET-KEY-VALUE'):
            with patch('context_sources.requests.get', side_effect=failure):
                context.refresh()
            payload = context.payload({'history': []})
            health = payload['health']
            self.assertEqual(sorted(health['failing']), ['aip', 'eia_brent', 'eia_gasoline', 'rba'])
            self.assertEqual(health['connected'], 0)
            brent = next(s for s in payload['sources'] if s['id'] == 'eia_brent')
            # A failing provider retries hourly, and that is readable without reading fuel health.
            self.assertEqual(brent['retry_seconds'], 3600)
            self.assertEqual(datetime.fromisoformat(brent['next_retry_at']) -
                             datetime.fromisoformat(brent['attempted_at']), timedelta(hours=1))
            self.assertEqual(brent['failure_category'], 'Network or DNS failure reaching the provider')
            self.assertEqual(brent['consecutive_failures'], 1)
            self.assertNotIn('SECRET-KEY-VALUE', json.dumps([context.state, payload, health]))
        # The category comes from the exception type and status code, never its text.
        response = type('R', (), {'status_code': 403})()
        self.assertIn('credential', failure_category(requests.HTTPError('key=SECRET', response=response)))
        self.assertIn('rate limit', failure_category(
            requests.HTTPError('x', response=type('R', (), {'status_code': 429})())))
        self.assertIn('validation', failure_category(ValueError('https://api.eia.gov/?api_key=SECRET')))

    def test_a_revision_is_archived_and_the_latest_value_still_overwrites(self):
        context = self.data.context
        with patch('context_sources.requests.get', side_effect=[Response(AIP), Response(RBA), Response(EIA), Response(EIA)]):
            context.refresh()
        for state in context.state.values():
            state['attempted_at'] = (datetime.now() - timedelta(days=1)).isoformat()
        revised = AIP.replace('<td>161.3</td>', '<td>162.9</td>')
        with patch('context_sources.requests.get', side_effect=[Response(revised), Response(RBA), Response(EIA), Response(EIA)]):
            context.refresh()
        with self.data.db._connect() as connection:
            self.assertEqual(connection.execute(
                'SELECT value FROM context_observations WHERE source="aip" AND observed_date="2026-09-02"').fetchone(), (162.9,))
            vintages = connection.execute('SELECT value, known_at FROM context_vintages '
                                          'WHERE source="aip" AND observed_date="2026-09-02" ORDER BY known_at').fetchall()
        # Both the original reading and the revision survive, each with when it was held.
        self.assertEqual([value for value, _ in vintages], [161.3, 162.9])
        self.assertLess(vintages[0][1], vintages[1][1])
        as_of_first = observations_as_of(self.data.db, 'aip', vintages[0][1])
        self.assertEqual(as_of_first['2026-09-02'], 161.3)
        self.assertEqual(observations_as_of(self.data.db, 'aip', vintages[1][1])['2026-09-02'], 162.9)
        # An unchanged republication must not grow the archive.
        self.assertEqual(next(s for s in context.payload({'history': []})['sources']
                              if s['id'] == 'eia_brent')['revisions_recorded'], 0)

    def test_later_revision_cannot_alter_a_replayed_past_forecast(self):
        """The acceptance condition for item 9: a replay reads the vintage of its own day."""
        days = pd.date_range('2026-01-01', periods=200)
        # Deterministic price cycle; the exact shape does not matter, only that the replay is stable.
        price = 180 + 12 * np.sin(np.arange(len(days)) / 4.7) + (np.arange(len(days)) % 11) * 0.4
        series = pd.Series(price, index=days)
        origin_cutoff = days[-1]
        rows = [('eia_brent', day.strftime('%Y-%m-%d'), 70 + (i % 17) * 0.3,
                 (day + timedelta(days=4)).strftime('%Y-%m-%dT12:00:00')) for i, day in enumerate(days)]
        with self.data.db._connect() as connection:
            connection.executemany('INSERT OR IGNORE INTO context_vintages VALUES (?,?,?,?)', rows)
        replay = lambda: context_evaluation(series, self.data.db, sources=['eia_brent'],  # noqa: E731
                                            today=origin_cutoff.strftime('%Y-%m-%d'))
        original = json.dumps(replay(), sort_keys=True)
        self.assertIsNotNone(json.loads(original)['feeds'][0].get('context_mae'),
                             'the feed must actually be evaluated, or this test proves nothing')
        # A revision published after every origin: known at 2027, not at any replayed day.
        with self.data.db._connect() as connection:
            connection.execute('INSERT INTO context_vintages VALUES (?,?,?,?)',
                               ('eia_brent', days[20].strftime('%Y-%m-%d'), 999.0, '2027-01-01T00:00:00'))
        self.assertEqual(json.dumps(replay(), sort_keys=True), original)
        self.assertEqual(observations_as_of(self.data.db, 'eia_brent', '2027-06-01')[days[20].strftime('%Y-%m-%d')],
                         999.0, 'the revision must really be stored, or the replay is unchanged for the wrong reason')
        # The same revision, known in time, does change the replay: the cut-off is load-bearing.
        with self.data.db._connect() as connection:
            connection.execute('INSERT INTO context_vintages VALUES (?,?,?,?)',
                               ('eia_brent', days[20].strftime('%Y-%m-%d'), 998.0,
                                (days[20] + timedelta(days=5)).strftime('%Y-%m-%dT12:00:00')))
        self.assertNotEqual(json.dumps(replay(), sort_keys=True), original)

    def test_recent_context_is_stale_after_fetch_failure(self):
        today=datetime.now().date().isoformat()
        with self.data.db._connect() as connection:
            connection.execute('INSERT INTO context_observations VALUES (?, ?, ?, ?, ?, ?)',
                               ('aip',today,180,'c/L',today,today))
        self.data.context.state['aip']={'status':'error','error':'Fetch failed'}
        result=self.data.context.payload({'history':[]})
        self.assertTrue(result['sources'][0]['stale'])


if __name__=='__main__':
    unittest.main()
