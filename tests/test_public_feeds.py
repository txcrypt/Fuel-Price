from unittest import TestCase
from unittest.mock import Mock, patch
import requests
import public_feeds

RSS = b'''<rss><channel>
<item><title>Brent climbs - Reuters</title><link>https://example.com/a</link><pubDate>Mon, 28 Sep 2026 01:00:00 GMT</pubDate><source>Reuters</source></item>
<item><title>Brent climbs</title><link>https://example.com/dup</link><source>Other</source></item>
<item><title>Clip</title><link>https://example.com/b</link><source>facebook.com</source></item>
<item><title>Bad link</title><link>javascript:alert(1)</link></item>
</channel></rss>'''


class PublicFeedTests(TestCase):
    def setUp(self):
        public_feeds._cache.clear()

    def test_news_dedupes_filters_and_keeps_last_good_copy(self):
        with patch('public_feeds.requests.get', return_value=Mock(content=RSS, raise_for_status=Mock())):
            items = public_feeds.news()['items']
        # One headline: the duplicate, the social clip and the non-http link are dropped; the outlet suffix is trimmed.
        self.assertEqual([(i['title'], i['source']) for i in items], [('Brent climbs', 'Reuters')])
        public_feeds._cache['news']['at'] -= public_feeds.timedelta(hours=1)
        with patch('public_feeds.requests.get', side_effect=requests.ConnectionError):
            self.assertEqual(len(public_feeds.news()['items']), 1)


class LocalNewsTests(TestCase):
    def setUp(self):
        public_feeds._cache.clear()

    def test_filters_to_public_interest_groups_outlets_and_drops_old(self):
        now = public_feeds.datetime(2026, 9, 28, 6, tzinfo=public_feeds.timezone.utc)
        hours = lambda h: (now - public_feeds.timedelta(hours=h)).isoformat()  # noqa: E731
        items = [
            {'title': 'Police officer stabbed in neck in Brisbane east', 'link': 'https://a', 'published': hours(3), 'source': 'ABC'},
            {'title': 'Hero police officer stabbed in neck recovering', 'link': 'https://b', 'published': hours(5), 'source': 'Nine'},
            {'title': 'Police officer charged, Brisbane Region', 'link': 'https://c', 'published': hours(8), 'source': 'QPS'},
            {'title': 'Broncos win the grand final', 'link': 'https://d', 'published': hours(2), 'source': 'Sport'},
            {'title': 'Council budget adopted', 'link': 'https://e', 'published': hours(24 * 10), 'source': 'Old'},
        ]
        with patch('public_feeds.rss_items', return_value=items):
            news = public_feeds.local_news('Brisbane', now=now)
        # Sport has no public-interest tag and the budget story is over a week old; the two stabbing
        # headlines are one event, while "officer charged" shares only two words and stays separate.
        self.assertEqual([(n['title'][:22], n['outlets'], n['tag'], n['breaking']) for n in news],
                         [('Police officer stabbed', ['ABC', 'Nine'], 'Crime & police', True),
                          ('Police officer charged', ['QPS'], 'Crime & police', False)])

    def test_place_name_cannot_break_out_of_the_query(self):
        with patch('public_feeds.rss_items', return_value=[]) as fetch:
            public_feeds.local_news('Cairns" OR (anything')
        self.assertIn('%22Cairns+OR+anything%22', fetch.call_args[0][0])


class TaggingTests(TestCase):
    def test_whole_words_and_no_sport(self):
        untagged = ['Melbourne Storm clinch minor premiership', 'Stable sales for local business', 'Courtney wins MasterChef',
                    'Fireworks light up the river', 'Photoshoot at waterfront cafe', 'Restaurant review: new place']
        self.assertEqual([public_feeds.tag(t) for t in untagged], [None] * len(untagged))
        self.assertEqual(public_feeds.tag('Severe storm warning for Cairns'), 'Emergency')
        self.assertEqual(public_feeds.tag('Bus route changes'), 'Roads & transport')

    def test_different_events_with_shared_topic_words_stay_apart(self):
        words = lambda t: public_feeds.words(t, 'Cairns')  # noqa: E731
        self.assertFalse(public_feeds.same_story(words('Man charged after crash on Bruce Highway at Gordonvale'),
                                                 words('Woman charged after crash on Captain Cook Highway')))
        self.assertFalse(public_feeds.same_story(words('Police investigating after body found in park'),
                                                 words('Police investigating after house fire')))

    def test_region_scopes_the_search(self):
        with patch('public_feeds.rss_items', return_value=[]) as fetch:
            public_feeds.local_news('Springfield', 'Queensland')
        self.assertIn('%22Springfield%22+%22Queensland%22', fetch.call_args[0][0])


class EvChargerTests(TestCase):
    def setUp(self):
        public_feeds._cache.clear()

    def test_no_key_explains_and_records_are_trimmed(self):
        self.assertEqual(public_feeds.ev_chargers(-27.5, 153.0, -27.4, 153.1, '')['status'], 'not_configured')
        poi = [{'AddressInfo': {'Title': 'Hub', 'Latitude': -27.45, 'Longitude': 153.05, 'AddressLine1': '1 St', 'Town': 'Brisbane'},
                'Connections': [{'ConnectionType': {'Title': 'CCS2'}, 'PowerKW': 150, 'Quantity': 2}, {'ConnectionType': {'Title': 'Type 2'}, 'PowerKW': 22}],
                'OperatorInfo': {'Title': 'Op'}, 'NumberOfPoints': 4, 'StatusType': {'IsOperational': True}},
               {'AddressInfo': {'Title': 'No location'}}]
        with patch('public_feeds.requests.get', return_value=Mock(json=Mock(return_value=poi), raise_for_status=Mock())) as get:
            result = public_feeds.ev_chargers(-27.5, 153.0, -27.4, 153.1, 'secret')
        charger = result['chargers'][0]
        self.assertEqual((len(result['chargers']), charger['max_kw'], charger['address']), (1, 150, '1 St, Brisbane'))
        # The key travels in a header, never in the URL that logs or caches might keep.
        self.assertNotIn('secret', str(get.call_args.kwargs['params']))
        self.assertEqual(get.call_args.kwargs['headers']['X-API-Key'], 'secret')
