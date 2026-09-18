import unittest
from unittest.mock import Mock, patch
import requests
from pydantic import ValidationError
from routing import RoadRouter, RouteRequest, RoutingError


def response(km=10, minutes=20, status=200):
    result=Mock(status_code=status)
    result.json.return_value={'features':[{'properties':{'summary':{'distance':km*1000,'duration':minutes*60}},
                                          'geometry':{'type':'LineString','coordinates':[[153,-27.47],[153.02,-27.48]]}}]}
    return result


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


if __name__=='__main__':
    unittest.main()
