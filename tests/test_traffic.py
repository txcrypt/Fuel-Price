from datetime import datetime, timezone, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch
import requests
import config
from traffic import (TrafficFeed, metro_feature, timing, route_disruptions, corridor_distance,
                     HISTORY_LIMIT, INTERVAL, CORRIDOR_M, NEARBY_M)


NOW=datetime(2026,9,18,tzinfo=timezone.utc)
# A straight ~1 km west-to-east route through central Brisbane, used as the driven corridor.
ROUTE=[[153.020,-27.470],[153.030,-27.470]]
# Metres per degree here, for turning a test offset into a believable separation.
DEG_LAT=110540


def feature(coords,kind='LineString',**properties):
    return {'type':'Feature','geometry':{'type':kind,'coordinates':coords},
            'properties':{'id':1,'event_type':'Roadworks','status':'Published',**properties}}


def event(coords,kind='LineString',timing_state='date_window',**properties):
    """A feed record as payload() hands it on, with its _timing already resolved."""
    return feature(coords,kind,_timing=timing_state,**properties)


class TrafficTests(TestCase):
    def setUp(self):
        self.directory=TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.feed=TrafficFeed(self.directory.name,key='private-test-key')

    def response(self,features,status=200,retry_after=None):
        result=Mock(status_code=status)
        result.headers={'Retry-After':retry_after} if retry_after is not None else {}
        result.json.return_value={'type':'FeatureCollection','features':features,'published':NOW.isoformat(),'rights':{'owner':'TMR'}}
        return result

    @patch.dict('config.BOUNDS', config.BRISBANE_BOUNDS, clear=True)
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
    @patch.dict('config.BOUNDS', config.BRISBANE_BOUNDS, clear=True)
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

    @patch('traffic.requests.get')
    def test_retry_after_delays_but_never_shortens_the_cooldown(self,get):
        get.side_effect=[self.response([],429,retry_after='900'),self.response([],429,retry_after='30'),
                         self.response([],429,retry_after='nonsense'),self.response([],429,retry_after='999999'),
                         self.response([],429)]
        self.feed.payload(NOW)
        self.assertEqual(get.call_count,1)
        for seconds,expected in [(899,1),(900,2),          # honoured: 15 minutes, not the fixed 5
                                 (1199,2),(1200,3),        # Retry-After 30 s is ignored in favour of INTERVAL
                                 (1499,3),(1500,4),        # unparseable header falls back to INTERVAL
                                 (5099,4),(5100,5)]:       # 999999 s capped at MAX_BACKOFF (3600)
            self.feed.payload(NOW+timedelta(seconds=seconds))
            self.assertEqual(get.call_count,expected,seconds)

    @patch('traffic.requests.get')
    def test_repeated_429_makes_one_request_per_cooldown_across_restart(self,get):
        get.return_value=self.response([],429)
        # A fresh TrafficFeed every poll: a restart must reload the persisted cooldown, not bypass it.
        for second in range(0,1800,30):
            TrafficFeed(self.directory.name,key='private-test-key').payload(NOW+timedelta(seconds=second))
        self.assertEqual(get.call_count,1800//INTERVAL)

    @patch('traffic.requests.get')
    def test_availability_history_records_outcomes_and_is_bounded(self,get):
        get.side_effect=[self.response([feature([153,-27.4],'Point')]),self.response([],429),
                         requests.ConnectionError('https://example/?apikey=private-test-key'),
                         self.response([feature([153,-27.4],'Point')])]
        for index in range(4):
            result=self.feed.payload(NOW+timedelta(seconds=INTERVAL*index*4))
        history=result['availability']
        self.assertEqual([row['outcome'] for row in history],['ok','rate_limited','unreachable','ok'])
        self.assertEqual([row['http'] for row in history],[200,429,None,200])
        self.assertEqual([row['age_seconds'] for row in history],[0,INTERVAL*4,INTERVAL*8,0])
        # Recovery clears stale, and the history holds no key.
        self.assertEqual(result['status'],'current')
        self.assertNotIn('private-test-key',str(history))
        # Bounded: a long run keeps only the newest HISTORY_LIMIT attempts, oldest dropped.
        get.side_effect=None
        get.return_value=self.response([])
        for index in range(HISTORY_LIMIT+20):
            result=self.feed.payload(NOW+timedelta(seconds=100000+INTERVAL*index))
        self.assertEqual(len(result['availability']),HISTORY_LIMIT)
        self.assertEqual(result['availability_limit'],HISTORY_LIMIT)

    @patch('traffic.requests.get')
    def test_availability_history_survives_restart_and_never_implies_clear_roads(self,get):
        get.side_effect=[self.response([]),self.response([],429)]
        self.feed.payload(NOW)
        restarted=TrafficFeed(self.directory.name,key='private-test-key')
        result=restarted.payload(NOW+timedelta(seconds=INTERVAL))
        self.assertEqual([row['outcome'] for row in result['availability']],['ok','rate_limited'])
        self.assertIn('never whether roads were clear',result['availability_note'])


class CorridorTests(TestCase):
    """Exact-geometry matching against one driven route line."""

    def match(self, features):
        return route_disruptions(features, ROUTE)

    def test_a_crossing_road_matches_where_its_endpoints_are_far_away(self):
        # Endpoints sit ~440 m off the route; only a real segment intersection finds this.
        crossing=event([[153.025,-27.474],[153.025,-27.466]])
        self.assertEqual(corridor_distance(ROUTE,crossing['geometry']),(0.0,False))
        result=self.match([crossing])
        self.assertEqual(result['counts']['on_corridor'],1)
        self.assertEqual(result['events'][0]['match'],'on_corridor')

    def test_a_parallel_road_is_nearby_and_a_distant_one_is_not_reported(self):
        parallel=event([[153.020,-27.4715],[153.030,-27.4715]],id=2)     # ~166 m to the south
        distant=event([[153.020,-27.4760],[153.030,-27.4760]],id=3)      # ~663 m to the south
        result=self.match([parallel,distant])
        self.assertEqual([e['id'] for e in result['events']],[2])
        self.assertEqual(result['events'][0]['match'],'nearby')
        self.assertAlmostEqual(result['events'][0]['metres'],166,delta=3)
        self.assertEqual(result['counts']['on_corridor'],0)

    def test_a_point_on_the_route_matches_and_one_off_it_does_not(self):
        on=event([153.025,-27.470],'Point',id=4)
        off=event([153.025,-27.4735],'Point',id=5)                        # ~387 m away
        result=self.match([on,off])
        self.assertEqual([(e['id'],e['match'],e['metres']) for e in result['events']],[(4,'on_corridor',0)])

    def test_an_enclosing_area_alert_is_never_called_a_closure_on_the_route(self):
        ring=[[153.010,-27.480],[153.040,-27.480],[153.040,-27.460],[153.010,-27.460],[153.010,-27.480]]
        flagged=event([ring],'Polygon',id=6,area_alert=True,
                      impact={'impact_type':'Closures','direction':'All directions','delay':None})
        result=self.match([flagged])
        self.assertEqual(result['events'][0]['match'],'area_alert')
        self.assertTrue(result['events'][0]['area_alert'])
        self.assertEqual(result['counts']['on_corridor'],0)
        # Unflagged but still enclosing: containment alone never promotes it to the corridor.
        self.assertEqual(self.match([event([ring],'Polygon',id=7)])['events'][0]['match'],'area_alert')

    def test_a_polygon_edge_on_the_route_still_counts_as_the_corridor(self):
        # A small works site drawn as a polygon over the road is not an area-wide alert.
        site=[[153.0248,-27.4701],[153.0252,-27.4701],[153.0252,-27.4699],[153.0248,-27.4699],[153.0248,-27.4701]]
        self.assertEqual(self.match([event([site],'Polygon',id=8)])['events'][0]['match'],'on_corridor')

    def test_scheduled_and_expired_events_are_separated_from_current_ones(self):
        span=[[153.022,-27.470],[153.026,-27.470]]
        result=self.match([event(span,id=10),event(span,timing_state='scheduled',id=11),
                           event(span,timing_state='past_end',id=12)])
        self.assertEqual(sorted(e['id'] for e in result['events']),[10,11])
        self.assertEqual([e['id'] for e in result['expired']],[12])
        self.assertEqual(result['counts']['scheduled'],1)
        self.assertEqual(result['counts']['expired'],1)
        self.assertIn('not in force now',[e for e in result['events'] if e['id']==11][0]['schedule_note'])

    def test_a_recurring_restriction_is_never_reported_as_active_now(self):
        recurring=event([[153.022,-27.470],[153.026,-27.470]],id=13,
                        duration={'start':'2026-01-01T00:00:00+10:00','active_days':['Monday'],
                                  'recurrences':[{'description':'Weeknights 7pm-5am'}]})
        entry=self.match([recurring])['events'][0]
        self.assertTrue(entry['recurring'])
        self.assertIn('Active hours are not confirmed',entry['schedule_note'])
        self.assertIn('does not confirm',entry['schedule_note'])

    def test_delay_wording_is_copied_verbatim_and_never_becomes_minutes(self):
        wordy=event([[153.022,-27.470],[153.026,-27.470]],id=14,
                    impact={'impact_type':'Lanes affected','direction':'Northbound','delay':'Long delays expected'})
        entry=self.match([wordy])['events'][0]
        self.assertEqual(entry['delay_text'],'Long delays expected')
        self.assertEqual(entry['direction'],'Northbound')
        self.assertFalse([k for k in entry if 'minute' in k or 'seconds' in k or k=='delay'])
        # metres is the only number derived from the event, and it is a distance.
        self.assertEqual([k for k,v in entry.items() if isinstance(v,(int,float)) and not isinstance(v,bool)],
                         ['id','metres'])

    def test_a_route_that_is_not_a_usable_line_matches_nothing(self):
        feed=TrafficFeed(key=None)
        feed.state={'feed':{'features':[]},'retrieved_at':NOW.isoformat(),
                    'next_attempt_at':(NOW+timedelta(seconds=INTERVAL)).isoformat()}
        for geometry in [{'type':'Point','coordinates':[153.02,-27.47]},
                         {'type':'LineString','coordinates':[[153.02,-27.47]]},None]:
            result=feed.disruptions(geometry,NOW)
            self.assertFalse(result['usable'])
            self.assertEqual(result['events'],[])


class CorridorFeedTests(TestCase):
    """The wording that must survive: no invented delays, no assurance of a clear road."""

    def setUp(self):
        self.directory=TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.feed=TrafficFeed(self.directory.name,key='private-test-key')

    def response(self,features,status=200):
        result=Mock(status_code=status)
        result.headers={}
        result.json.return_value={'type':'FeatureCollection','features':features,'published':NOW.isoformat()}
        return result

    def check(self,now=NOW,route=None):
        return self.feed.disruptions({'type':'LineString','coordinates':route or ROUTE},now)

    @patch('traffic.requests.get')
    def test_no_match_is_never_an_assurance_that_the_road_is_clear(self,get):
        get.return_value=self.response([feature([[153.40,-27.10],[153.41,-27.11]],id=20)])
        result=self.check()
        self.assertEqual(result['events'],[])
        self.assertEqual(result['feed']['status'],'current')
        self.assertIn('does not establish that the road is clear',' '.join(result['notes']))
        self.assertIn('do not change the route, the estimated travel time or the fuel cost',' '.join(result['notes']))
        self.assertEqual(result['headline'],'No reported event touches this route line.')

    @patch('traffic.requests.get')
    def test_a_stale_feed_says_so_rather_than_reporting_a_quiet_corridor(self,get):
        get.side_effect=[self.response([feature([[153.022,-27.470],[153.026,-27.470]],id=21)]),
                         self.response([],429)]
        self.check()
        result=self.check(NOW+timedelta(seconds=1200))
        self.assertTrue(result['feed']['stale'])
        self.assertIn('may be missing current events entirely',' '.join(result['notes']))
        # Retained events are still reported, flagged as retained rather than dropped silently.
        self.assertEqual(result['counts']['on_corridor'],1)

    @patch('traffic.requests.get')
    def test_a_nearby_event_is_never_declared_a_closure_on_the_route(self,get):
        get.return_value=self.response([feature([[153.020,-27.4715],[153.030,-27.4715]],id=22,
                                                impact={'impact_type':'Closures','direction':'Both directions','delay':None})])
        result=self.check()
        self.assertEqual(result['counts']['on_corridor'],0)
        self.assertEqual(result['events'][0]['match'],'nearby')
        self.assertIn('not established to be on your route',' '.join(result['notes']))
        self.assertEqual(result['headline'],'No reported event touches this route line.')

    @patch('traffic.requests.get')
    @patch.dict('config.BOUNDS', config.BRISBANE_BOUNDS, clear=True)
    def test_a_route_leaving_the_analysis_area_says_where_it_stopped_looking(self,get):
        get.return_value=self.response([])
        result=self.check(route=[[153.02,-27.47],[153.90,-27.47]])
        self.assertTrue(result['route_outside_area'])
        self.assertIn('outside it and was not checked there',' '.join(result['notes']))


class CameraTests(TestCase):
    def test_provider_images_only_hourly_cache_and_keeps_last_good_list(self):
        import traffic
        traffic._cameras.update(at=None, items=[])
        point = {'coordinates': [153.0, -27.5]}
        good = {'geometry': point, 'properties': {'id': 1, 'image_url': traffic.CAMERA_HOST + 'a.jpg', 'description': 'A'}}
        foreign = {'geometry': point, 'properties': {'id': 2, 'image_url': 'https://elsewhere.example/x.jpg'}}
        with patch('traffic.requests.get') as get:
            get.return_value.json.return_value = {'features': [good, foreign]}
            self.assertEqual([c['id'] for c in traffic.cameras(NOW)], [1])
            traffic.cameras(NOW + timedelta(minutes=30))
            self.assertEqual(get.call_count, 1)
            get.side_effect = requests.ConnectionError
            self.assertEqual(len(traffic.cameras(NOW + timedelta(hours=2))), 1)
