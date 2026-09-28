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
