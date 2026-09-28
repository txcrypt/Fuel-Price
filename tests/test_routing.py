import unittest
from unittest.mock import Mock, patch
import requests
from pydantic import ValidationError
from buying_plan import BuyingProfile, compare_plan
from routing import RoadRouter, RankRequest, RouteRequest, RoutingError


def response(km=10, minutes=20, status=200):
    result=Mock(status_code=status)
    result.json.return_value={'features':[{'properties':{'summary':{'distance':km*1000,'duration':minutes*60}},
                                          'geometry':{'type':'LineString','coordinates':[[153,-27.47],[153.02,-27.48]]}}]}
    return result


def fake_provider(legs, baseline=(10.0, 20.0)):
    """Answer by the coordinates asked for: 3 points is a via route, 2 is the shared baseline.

    legs maps a station longitude to its (km, minutes) via route; a status code
    instead of a tuple makes that one station fail.
    """
    def post(url, headers=None, json=None, timeout=None):
        coordinates=json['coordinates']
        if len(coordinates)==2:
            return response(*baseline)
        answer=legs[coordinates[1][0]]
        return response(status=answer) if isinstance(answer,int) else response(*answer)
    return post


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.router=RoadRouter('test-private-token')
        self.dashboard={'stations':[{'site_id':'1','name':'Test','longitude':153.02,'latitude':-27.48,'price_cpl':200}], 'health':{'fresh':True}}
        self.profile={'station_id':'1','origin':{'latitude':-27.47,'longitude':153},'litres':40,'consumption':8,'hourly_value':30}

    @patch('routing.requests.post')
    def test_round_trip_cost_and_coordinate_order(self, post):
        post.return_value=response()
        r=self.router.compare(RouteRequest(**self.profile),self.dashboard)
        self.assertEqual(post.call_args.kwargs['json']['coordinates'],[[153,-27.47],[153.02,-27.48],[153,-27.47]])
        self.assertEqual(r['extra_fuel_l'],.8)
        self.assertEqual(r['travel_fuel_cost'],1.6)
        self.assertEqual(r['travel_time_cost'],10)
        self.assertEqual(r['cost_including_travel'],91.6)
        self.assertEqual(r['effective_cpl'],229)
        self.assertIsNone(r['direct'])
        self.assertNotIn('test-private-token',str(r)+str(self.router.status()))

    @patch('routing.requests.post')
    def test_trip_compares_direct_route_and_cache(self, post):
        post.side_effect=[response(12,24),response(10,20)]
        profile=RouteRequest(**self.profile,destination={'latitude':-27.5,'longitude':153.04})
        r=self.router.compare(profile,self.dashboard)
        self.assertEqual(r['extra_km'],2)
        self.assertEqual(r['extra_minutes'],4)
        self.assertEqual(r['cost_including_travel'],82.32)
        repeated=self.router.compare(profile,self.dashboard)
        self.assertEqual(post.call_count,2)
        self.assertTrue(repeated['via']['cached'])

    @patch('routing.requests.post')
    def test_provider_errors_are_sanitized(self, post):
        for code,expected in [(401,503),(403,503),(429,429),(404,422)]:
            post.return_value=response(status=code)
            with self.assertRaises(RoutingError) as caught:
                self.router.compare(RouteRequest(**self.profile),self.dashboard)
            self.assertEqual(caught.exception.status,expected)
            self.assertNotIn('test-private-token',str(caught.exception))
        post.side_effect=requests.ConnectionError('test-private-token')
        with self.assertRaises(RoutingError) as caught:
            self.router.compare(RouteRequest(**self.profile),self.dashboard)
        self.assertNotIn('test-private-token',str(caught.exception))

    @patch('routing.requests.post')
    def test_invalid_inputs_never_reach_provider(self, post):
        with self.assertRaises(ValidationError):
            RouteRequest(**{**self.profile,'litres':0})
        with self.assertRaises(ValidationError):
            RouteRequest(**{**self.profile,'origin':{'latitude':float('nan'),'longitude':153}})
        for changes in [{'station_id':'unknown'},{'origin':{'latitude':0,'longitude':0}}]:
            with self.assertRaises(RoutingError):
                self.router.compare(RouteRequest(**{**self.profile,**changes}),self.dashboard)
        with self.assertRaises(RoutingError):
            RoadRouter('').compare(RouteRequest(**self.profile),self.dashboard)
        post.assert_not_called()

    @patch('routing.requests.post')
    def test_toll_option_and_signed_differences(self, post):
        post.side_effect=[response(10,20),response(12,24)]
        r=self.router.compare(RouteRequest(**self.profile,avoid_tolls=False,destination={'latitude':-27.5,'longitude':153.04}),self.dashboard)
        self.assertNotIn('options',post.call_args.kwargs['json'])
        self.assertEqual(r['extra_km'],-2)

    @patch('routing.requests.post')
    def test_traffic_never_reaches_the_route_arithmetic(self, post):
        """Item 7: reported disruptions are shown beside a route, never folded into it.

        routing.py has no traffic input at all, which is the guarantee. The corridor check
        lives behind its own endpoint precisely so a disruption cannot move these numbers.
        """
        post.return_value=response(10,20)
        r=self.router.compare(RouteRequest(**self.profile),self.dashboard)
        self.assertEqual((r['extra_km'],r['extra_minutes']),(10.0,20.0))
        self.assertEqual(r['cost_including_travel'],round(40*2.0+10*8/100*2.0+20*30/60,2))
        self.assertFalse([k for k in r if 'traffic' in k or 'delay' in k or 'disrupt' in k])
        self.assertIn('not live traffic',r['note'])


def planner_market(stations):
    return {'stations':stations, 'summary':{'median':210}, 'health':{'fresh':True},
            'forecast':{'status':'limited','recent_consecutive_days':1,'scores':[],
                        'points':[{'date':'2026-09-19','price':190,'low':None,'high':None}]}}


class AdoptionTests(unittest.TestCase):
    """Item 5: what the route hands to the buying planner, and what it must not."""

    def setUp(self):
        self.router=RoadRouter('test-private-token')
        self.dashboard=planner_market([{'site_id':'one','name':'Local test station',
                                        'longitude':153.02,'latitude':-27.48,'price_cpl':200}])
        self.profile={'station_id':'one','origin':{'latitude':-27.47,'longitude':153},
                      'litres':40,'consumption':8,'hourly_value':30}

    def route(self, km=12, minutes=18, **changes):
        with patch('routing.requests.post', return_value=response(km,minutes)):
            return self.router.compare(RouteRequest(**{**self.profile,**changes}),self.dashboard)

    def test_adoption_carries_quantities_never_a_cost(self):
        adopted=self.route()['adoption']
        self.assertEqual((adopted['extra_km'],adopted['extra_minutes']),(12.0,18.0))
        self.assertEqual((adopted['consumption'],adopted['hourly_value']),(8,30))
        self.assertTrue(adopted['adoptable'])
        self.assertIn('Dedicated return trip',adopted['trip'])
        self.assertIn('deferred refill',adopted['applies_to'])
        # No dollar figure may cross: buying_plan charges the detour from the quantities.
        for key,value in adopted.items():
            self.assertNotIn('cost',key)
            self.assertNotIn('$',str(value) if key!='notes' else '')
        self.assertTrue(BuyingProfile(extra_km=adopted['extra_km'],extra_minutes=adopted['extra_minutes'],
                                      extra_travel_basis=adopted['basis']))

    def test_trip_detour_and_cancelling_leg_are_distinguished(self):
        with patch('routing.requests.post',side_effect=[response(12,24),response(10,20)]):
            detour=self.router.compare(RouteRequest(**self.profile,destination={'latitude':-27.5,'longitude':153.04}),
                                       self.dashboard)['adoption']
        self.assertIn('journey you were making anyway',detour['trip'])
        self.assertEqual((detour['extra_km'],detour['extra_minutes']),(2.0,4.0))
        both=self.route(leg='both_purchases')['adoption']
        self.assertEqual((both['extra_km'],both['extra_minutes']),(0.0,0.0))
        self.assertTrue(both['adoptable'])
        self.assertIn('cancels out',both['applies_to'])

    def test_a_negative_or_oversized_detour_is_refused_not_clamped(self):
        with patch('routing.requests.post',side_effect=[response(10,20),response(12,24)]):
            shorter=self.router.compare(RouteRequest(**self.profile,destination={'latitude':-27.5,'longitude':153.04}),
                                        self.dashboard)['adoption']
        self.assertFalse(shorter['adoptable'])
        self.assertIn('negative detour',' '.join(shorter['blocked']))
        huge=self.route(km=400,minutes=400)['adoption']
        self.assertFalse(huge['adoptable'])
        self.assertIn('300 km',' '.join(huge['blocked']))

    def test_time_value_beyond_the_planner_limit_is_reported_not_silently_dropped(self):
        adopted=self.route(hourly_value=500)['adoption']
        self.assertIsNone(adopted['hourly_value'])
        self.assertIn('$200/hour',' '.join(adopted['notes']))
        self.assertTrue(adopted['adoptable'])

    def test_fingerprint_changes_with_station_origin_destination_volume_and_price(self):
        base=self.route()['adoption']['fingerprint']
        moved=dict(self.dashboard,stations=[dict(self.dashboard['stations'][0],price_cpl=201)])
        with patch('routing.requests.post',return_value=response(12,18)):
            repriced=self.router.compare(RouteRequest(**self.profile),moved)['adoption']['fingerprint']
        changes=[{'litres':41},{'origin':{'latitude':-27.471,'longitude':153}},
                 {'destination':{'latitude':-27.5,'longitude':153.04}},{'leg':'both_purchases'},
                 {'consumption':9},{'avoid_tolls':False}]
        prints={base,repriced}
        for change in changes:
            with patch('routing.requests.post',return_value=response(12,18)):
                prints.add(self.router.compare(RouteRequest(**{**self.profile,**change}),self.dashboard)['adoption']['fingerprint'])
        self.assertEqual(len(prints),len(changes)+2)
        self.assertEqual(base,self.route()['adoption']['fingerprint'])
        with self.assertRaises(RoutingError):  # A vanished station cannot keep an adopted cost alive.
            self.router.compare(RouteRequest(**{**self.profile,'station_id':'gone'}),self.dashboard)

    def test_routed_detour_is_charged_exactly_once_in_the_plan(self):
        """Equivalence proof: equal ending fuel, and waiting costs exactly the detour."""
        adopted=self.route()['adoption']
        profile=BuyingProfile(station_id='one',daily_km=35,consumption=adopted['consumption'],
                              extra_km=adopted['extra_km'],extra_minutes=adopted['extra_minutes'],
                              hourly_value=adopted['hourly_value'],extra_travel_basis=adopted['basis'],
                              scenario_change_cpl=0)
        plan=compare_plan(profile,self.dashboard)
        price=plan['price_now']/100
        # Ending fuel: same litres in the tank on the target date, both ways.
        self.assertAlmostEqual(10+plan['bridge_l']+plan['later_l']-8.4-plan['extra_fuel_l'],plan['end_l'])
        self.assertAlmostEqual(plan['end_l'],50-8.4)
        # Litres bought: the deferred plan buys exactly the detour's fuel more.
        self.assertAlmostEqual(plan['bridge_l']+plan['later_l'],plan['fill_l']+plan['extra_fuel_l'])
        # Cost: at an unchanged price the whole difference is one detour plus one time charge.
        detour=plan['extra_fuel_l']*price+plan['time_cost']
        self.assertAlmostEqual(plan['deferred_cost'],plan['fill_cost']+detour)
        self.assertAlmostEqual(plan['saving'],-detour)
        self.assertEqual((plan['extra_fuel_l'],plan['time_cost']),(0.96,9.0))
        self.assertEqual((plan['fill_cost'],plan['deferred_cost'],plan['saving']),(80.0,90.92,-10.92))
        self.assertIn(adopted['basis'],plan['warnings'])
        # And the cancelling leg costs nothing extra at all.
        cancels=compare_plan(profile.model_copy(update={'extra_km':0,'extra_minutes':0,'hourly_value':30}),self.dashboard)
        self.assertEqual(cancels['deferred_cost'],cancels['fill_cost'])

    def test_hand_typed_extra_travel_is_labelled_as_such(self):
        plan=compare_plan(BuyingProfile(station_id='one',extra_km=5,scenario_change_cpl=0),self.dashboard)
        self.assertIsNone(plan['extra_travel_basis'])
        self.assertIn('typed in by hand',plan['warnings'][0])


class RankingTests(unittest.TestCase):
    """Item 6: pump-price rank against total-trip-cost rank over a bounded shortlist."""

    def setUp(self):
        self.router=RoadRouter('test-private-token')
        self.dashboard=planner_market([
            {'site_id':'cheap-pump','name':'Far bargain','longitude':153.10,'latitude':-27.47,'price_cpl':190},
            {'site_id':'near','name':'Near corner','longitude':153.01,'latitude':-27.47,'price_cpl':196},
            {'site_id':'closest','name':'Closest','longitude':153.005,'latitude':-27.47,'price_cpl':202},
            {'site_id':'outside','name':'Out of radius','longitude':154.2,'latitude':-27.47,'price_cpl':150}])
        self.legs={153.10:(40,60), 153.01:(6,9), 153.005:(2,3)}
        self.request={'origin':{'latitude':-27.47,'longitude':153},'litres':40,'consumption':10,
                      'hourly_value':0,'radius_km':12,'limit':3}

    def rank(self, **changes):
        with patch('routing.requests.post',side_effect=fake_provider(self.legs)) as post:
            self.post=post
            return self.router.rank(RankRequest(**{**self.request,**changes}),self.dashboard)

    def test_travel_reverses_a_pump_price_bargain(self):
        result=self.rank()
        self.assertEqual(result['evaluated'],3)
        self.assertEqual([r['station_id'] for r in sorted(result['results'],key=lambda r:r['pump_rank'])],
                         ['cheap-pump','near','closest'])
        self.assertEqual([r['station_id'] for r in result['results']],['near','closest','cheap-pump'])
        self.assertTrue(result['ranks_disagree'])
        self.assertEqual(result['best_pump'],'cheap-pump')
        self.assertEqual(result['best_total'],'near')
        totals={r['station_id']:(r['cost_including_travel'],r['effective_cpl']) for r in result['results']}
        self.assertEqual(totals['cheap-pump'],(83.60,209.0))   # 76.00 pump + 4 L over 40 km
        self.assertEqual(totals['near'],(79.58,198.9))         # 78.40 pump + 0.6 L over 6 km
        self.assertEqual(totals['closest'],(81.20,203.0))
        self.assertIn('Cheapest of the 3 stations evaluated',result['headline'])
        self.assertIn('not a city-wide optimum',result['note'])

    def test_shortlist_is_bounded_described_and_identical_for_every_candidate(self):
        result=self.rank()
        self.assertEqual(self.post.call_count,3)      # One dedicated return trip each, no baseline leg.
        self.assertIn('within 12 km straight-line',result['shortlist_rule'])
        self.assertIn('from 3 stations inside that radius',result['shortlist_rule'])   # 'outside' is beyond it.
        self.assertIn('4 priced stations in the snapshot',result['shortlist_rule'])
        self.assertEqual(result['max_candidates'],8)
        self.assertFalse(result['partial'])
        self.assertTrue(all(r['litres']==40 for r in result['results']))
        self.assertTrue(result['results'][0]['via'].get('geometry'))
        self.assertNotIn('geometry',result['results'][1]['via'])
        with self.assertRaises(ValidationError):
            RankRequest(**{**self.request,'limit':9})
        with self.assertRaises(ValidationError):
            RankRequest(**{**self.request,'station_ids':['a']*9})

    def test_one_shared_baseline_journey_is_fetched_once(self):
        result=self.rank(destination={'latitude':-27.47,'longitude':153.2})
        self.assertEqual(self.post.call_count,4)      # Three via routes plus one cached direct journey.
        self.assertTrue(result['baseline_consistent'])
        self.assertEqual(result['baseline'],{'km':10.0,'minutes':20.0})
        self.assertEqual(result['mode'],'trip_detour')

    def test_failures_stay_visible_and_quota_stops_the_loop(self):
        self.legs[153.01]=404
        result=self.rank()
        self.assertEqual((result['evaluated'],result['requested']),(2,3))
        self.assertTrue(result['partial'])
        self.assertEqual([f['station_id'] for f in result['failures']],['near'])
        self.assertEqual(result['failures'][0]['status'],422)
        self.assertIsNone(result['quota_stopped'])

    def test_quota_failure_stops_the_loop_instead_of_spending_the_allowance(self):
        self.legs[153.005]=429
        stopped=self.rank(station_ids=['cheap-pump','closest','near','missing'])
        self.assertEqual(self.post.call_count,2)      # Stopped at the refusal; 'near' was never requested.
        self.assertEqual((stopped['evaluated'],stopped['requested'],stopped['partial']),(1,4,True))
        self.assertIsNotNone(stopped['quota_stopped'])
        self.assertEqual({f['station_id'] for f in stopped['failures']},{'closest','missing'})
        self.assertIn('you chose by hand',stopped['shortlist_rule'])
        self.legs[153.10]=429                         # Nothing survives: the quota status is surfaced, not masked.
        with patch('routing.requests.post',side_effect=fake_provider(self.legs)):
            with self.assertRaises(RoutingError) as caught:
                RoadRouter('test-private-token').rank(RankRequest(**self.request),self.dashboard)
        self.assertEqual(caught.exception.status,429)

    def test_no_candidate_and_no_key_never_burst(self):
        with patch('routing.requests.post') as post:
            with self.assertRaises(RoutingError):
                self.router.rank(RankRequest(**{**self.request,'radius_km':0.1}),self.dashboard)
            with self.assertRaises(RoutingError):
                RoadRouter('').rank(RankRequest(**self.request),self.dashboard)
            post.assert_not_called()
        self.legs={k:404 for k in self.legs}
        with self.assertRaises(RoutingError) as caught:
            self.rank()
        self.assertEqual(caught.exception.status,502)


if __name__=='__main__':
    unittest.main()
