from datetime import datetime, timezone, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch
import requests
from traffic import TrafficFeed, metro_feature, timing


NOW=datetime(2026,9,18,tzinfo=timezone.utc)


def feature(coords,kind='LineString',**properties):
    return {'type':'Feature','geometry':{'type':kind,'coordinates':coords},
            'properties':{'id':1,'event_type':'Roadworks','status':'Published',**properties}}


class TrafficTests(TestCase):
    def setUp(self):
        self.directory=TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.feed=TrafficFeed(self.directory.name,key='private-test-key')

    def response(self,features,status=200):
        result=Mock(status_code=status)
        result.json.return_value={'type':'FeatureCollection','features':features,'published':NOW.isoformat(),'rights':{'owner':'TMR'}}
        return result

    def test_geometries_and_crossing_region(self):
        self.assertTrue(metro_feature(feature([[152,-27.4],[154,-27.4]])))
        self.assertFalse(metro_feature(feature([151,-25],'Point')))
        f=feature([]);f['geometry']={'type':'GeometryCollection','geometries':[{'type':'Point','coordinates':[153,-27.4]}]}
        self.assertTrue(metro_feature(f))
        self.assertFalse(metro_feature(feature([float('nan'),-27.4],'Point')))

    def test_timing_does_not_claim_recurrences_are_active(self):
        self.assertEqual(timing({'duration':{'start':'2026-09-19T00:00:00Z'}},NOW),'scheduled')
        self.assertEqual(timing({'duration':{'end':'2026-09-17T00:00:00Z'}},NOW),'past_end')
        self.assertEqual(timing({'duration':{'start':'2026-09-01T00:00:00+10:00','active_days':['Monday']}},NOW),'date_window')
        self.assertEqual(timing({},NOW),'unknown')

    @patch('traffic.requests.get')
    def test_success_cache_restart_and_raw_preservation(self,get):
        items=[feature([153,-27.4],'Point',additional_field='retained'),feature([151,-25],'Point')]
        get.return_value=self.response(items)
        result=self.feed.payload(NOW)
        self.assertEqual(result['metro_events'],1)
        self.assertEqual(result['total_events'],2)
        self.assertFalse(result['stale'])
        self.assertEqual(self.feed.raw()['feed']['features'],items)
        restarted=TrafficFeed(self.directory.name,key='private-test-key')
        restarted.payload(NOW+timedelta(seconds=299))
        self.assertEqual(get.call_count,1)
        text=Path(self.directory.name,'latest.json').read_text()
        self.assertNotIn('private-test-key',text)

    @patch('traffic.requests.get')
    def test_rate_limit_retains_data_and_cooldown(self,get):
        get.side_effect=[self.response([feature([153,-27.4],'Point')]),self.response([],429)]
        self.feed.payload(NOW)
        result=self.feed.payload(NOW+timedelta(seconds=301))
        self.assertEqual(result['status'],'stale')
        self.assertEqual(result['metro_events'],1)
        self.assertIn('429',result['error'])
        self.feed.payload(NOW+timedelta(seconds=302))
        self.assertEqual(get.call_count,2)

    @patch('traffic.requests.get')
    def test_first_failure_is_unavailable_not_clear_roads(self,get):
        get.return_value=self.response([],429)
        result=self.feed.payload(NOW)
        self.assertEqual(result['status'],'unavailable')
        self.assertIsNone(result['retrieved_at'])
        self.assertTrue(result['stale'])

    @patch('traffic.requests.get')
    def test_connection_error_never_echoes_key(self,get):
        get.side_effect=requests.ConnectionError('https://example/?apikey=private-test-key')
        result=self.feed.payload(NOW)
        self.assertNotIn('private-test-key',str(result))

    @patch('traffic.requests.get')
    def test_empty_success_is_different_from_missing_data(self,get):
        get.return_value=self.response([])
        result=self.feed.payload(NOW)
        self.assertEqual(result['status'],'current')
        self.assertEqual(result['metro_events'],0)
