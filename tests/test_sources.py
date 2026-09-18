from datetime import datetime, timedelta
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import requests

from api_evidence import ApiEvidence, price_status, write_json
from context_sources import ContextSources, parse_source
from fuel_engine import FuelEngine
from local_data import LocalData
from test_local import FakeEngine, snapshot


SITE = {'S':123,'N':'Test site','A':'10 Test Road','B':7,'P':'4000','G1':10,'G2':20,
        'G3':1,'G4':0,'G5':0,'Lat':-27.4,'Lng':153.0,'M':'2026-01-01T00:00:00','GPI':'example','MO':''}
PRICE = {'SiteId':123,'FuelId':2,'Price':1800,'TransactionDateUtc':'2026-09-16T01:00:00','CollectionMethod':'Q'}
RESPONSES = {'sites':[SITE], 'prices':[PRICE], 'brands':[{'BrandId':7,'Name':'Test brand'}],
             'fuels':[{'FuelId':2,'Name':'Unleaded'},{'FuelId':3,'Name':'Diesel'}],
             'regions':[{'GeoRegionLevel':1,'GeoRegionId':10,'Name':'Test suburb'},
                        {'GeoRegionLevel':2,'GeoRegionId':20,'Name':'Brisbane'}]}
AIP = '<h2>Petrol (ULP, cents per litre, inclusive of GST)</h2><table><tr><th>Location</th><th>Tuesday, 1st September 2026</th><th>Wednesday, 2nd September 2026</th></tr><tr><td>Brisbane</td><td>160.2</td><td>161.3</td></tr></table><h2>Diesel</h2><table><tr><th>Location</th><th>Wednesday, 2nd September 2026</th></tr><tr><td>Brisbane</td><td>250</td></tr></table>'
RBA = '<table><tr><th></th><th>01 Sep 2026</th><th>02 Sep 2026</th></tr><tr><th>United States dollar</th><td>0.7</td><td>0.71</td></tr></table>'


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
            with self.assertRaisesRegex(RuntimeError,'no valid Brisbane'):
                engine.get_market_snapshot()

    def test_utc_report_without_suffix_is_not_treated_as_brisbane_time(self):
        frame=snapshot()
        frame['reported_at']='2026-09-16T01:00:00'
        frame.to_csv(self.data.snapshot,index=False)
        self.assertEqual(self.data.dashboard()['stations'][0]['reported_at'],'2026-09-16T01:00:00.000000Z')

    def test_public_parsers_select_petrol_and_match_date_columns(self):
        self.assertEqual(parse_source('aip',AIP)[-1],{'date':'2026-09-02','value':161.3})
        self.assertEqual(parse_source('rba',RBA)[-1],{'date':'2026-09-02','value':0.71})
        with self.assertRaises(ValueError):
            parse_source('aip',AIP.replace('<td>161.3</td>',''))
        with self.assertRaises(ValueError):
            parse_source('rba','<p>Access denied</p>')

    def test_failed_context_refresh_retains_dates_and_pairs_only_same_day(self):
        class Response:
            def __init__(self,text): self.text=text
            def raise_for_status(self): pass
        context=self.data.context
        with patch('context_sources.requests.get',side_effect=[Response(AIP),Response(RBA)]) as fetch:
            context.refresh()
            context.refresh()
            self.assertEqual(fetch.call_count,2)
        for state in context.state.values():
            state['attempted_at']=(datetime.now()-timedelta(days=1)).isoformat()
        with patch('context_sources.requests.get',side_effect=requests.ConnectionError()):
            context.refresh()
        result=context.payload({'history':[{'date':'2026-09-02','price':180},{'date':'2026-09-03','price':190}]})
        self.assertEqual(result['sources'][0]['status'],'error')
        self.assertEqual(result['sources'][0]['points'][-1]['date'],'2026-09-02')
        self.assertEqual(result['paired_history'],[{'date':'2026-09-02','retail':180,'wholesale':161.3,'gap':18.7}])

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
