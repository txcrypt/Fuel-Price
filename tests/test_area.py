from unittest import TestCase
import area


def station(name, price, lat=-27.47, lng=153.02, flag=None):
    return {'name': name, 'site_id': name, 'price_cpl': price, 'latitude': lat, 'longitude': lng, 'flag': flag}


class AreaTests(TestCase):
    def test_small_boxes_are_padded(self):
        b = area.padded(-27.471, 153.021, -27.470, 153.022)
        self.assertAlmostEqual(b['lat_max'] - b['lat_min'], area.MIN_SPAN_DEG)

    def test_fuel_ranks_only_unflagged_stations_inside(self):
        b = area.padded(-27.6, 152.9, -27.3, 153.2)
        stats = area.fuel(b, [station('A', 230.0), station('B', 240.0), station('Bad', 99.9, flag='suspect'),
                              station('Cairns', 200.0, lat=-16.9, lng=145.7)])
        self.assertEqual((stats['stations'], stats['flagged'], stats['lowest']['name'], stats['highest']['name']), (3, 1, 'A', 'B'))
        self.assertEqual((stats['median'], stats['spread'], stats['within_5c_of_lowest']), (235.0, 10.0, 1))

    def test_roads_count_current_events_and_list_closures_first(self):
        b = area.padded(-27.6, 152.9, -27.3, 153.2)
        event = lambda impact, timing='date_window', lng=153.0: {'geometry': {'type': 'Point', 'coordinates': [lng, -27.47]},  # noqa: E731
                                                                  'properties': {'event_type': 'Roadworks', '_timing': timing, 'impact': {'impact_type': impact}}}
        roads = area.roads(b, [event('Lanes affected'), event('Road closed'), event('Road closed', 'scheduled'), event('x', lng=145.0)])
        self.assertEqual((roads['current'], roads['scheduled'], roads['events'][0]['impact']), (2, 1, 'Road closed'))


class AreaRouteTests(TestCase):
    def setUp(self):
        from fastapi.testclient import TestClient
        import fuel_dashboard
        self.client = TestClient(fuel_dashboard.app)

    def test_other_websites_are_refused(self):
        response = self.client.get('/api/area?south=-27.6&west=152.9&north=-27.3&east=153.2',
                                   headers={'sec-fetch-site': 'cross-site'})
        self.assertEqual(response.status_code, 403)

    def test_box_outside_australia_is_422_not_500(self):
        response = self.client.get('/api/area?south=10&west=0&north=20&east=10')
        self.assertEqual(response.status_code, 422)


class ProxyOriginTests(TestCase):
    def test_post_from_proxied_origin_is_allowed_and_foreign_origin_refused(self):
        from fastapi.testclient import TestClient
        import fuel_dashboard
        client = TestClient(fuel_dashboard.app)
        # An empty route fails validation (422) only once the origin check has let it through.
        proxied = client.post('/api/traffic/route', json={}, headers={'origin': 'https://pc.tail1234.ts.net', 'x-forwarded-host': 'pc.tail1234.ts.net'})
        self.assertEqual(proxied.status_code, 422)
        self.assertEqual(client.post('/api/traffic/route', json={}, headers={'origin': 'https://evil.example'}).status_code, 403)
