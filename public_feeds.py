"""Public context for the dashboard: energy news headlines, Queensland weather warnings and
bushfire incidents. Displayed only; nothing here enters a forecast or a recommendation."""
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import re
import xml.etree.ElementTree as ET
import requests

GOOGLE = 'https://news.google.com/rss/search?hl=en-AU&gl=AU&ceid=AU:en&q='
NEWS_FEEDS = [  # (topic, url). Google News returns search results in relevance order.
    ('Global oil', GOOGLE + '(crude+OR+Brent+OR+OPEC)+(oil+price+OR+prices)+when:3d'),
    ('Australian fuel', GOOGLE + '(petrol+OR+fuel+OR+bowser)+(Australia+OR+Queensland+OR+Brisbane)'
                                 '+(price+OR+prices+OR+excise+OR+ACCC)+when:7d'),
    ('Supply & refining', GOOGLE + '(fuel+OR+diesel+OR+refinery)+(supply+OR+security+OR+shortage)+Australia+when:7d'),
    ('Global oil', 'https://oilprice.com/rss/main'),
]
WARNINGS_URL = 'https://www.bom.gov.au/fwo/IDZ00056.warnings_qld.xml'
BUSHFIRE_URL = ('https://publiccontent-gis-psba-qld-gov-au.s3.amazonaws.com/content/Feeds/'
                'BushfireCurrentIncidents/bushfireAlert.json')
HEADERS = {'User-Agent': 'Mozilla/5.0 (local fuel dashboard)'}
LINK = re.compile(r'^https?://')
SOCIAL = re.compile(r'facebook|youtube|instagram|tiktok|reddit|x\.com|twitter|linkedin', re.I)
_cache = {}


def cached(key, ttl, fetch, now=None):
    """Fetch at most once per ``ttl``; on failure keep serving the last good value."""
    now = now or datetime.now(timezone.utc)
    entry = _cache.get(key)
    if entry and now - entry['at'] < ttl:
        return entry['value']
    try:
        value = fetch()
    except (requests.RequestException, ET.ParseError, ValueError, KeyError, TypeError):
        value = entry['value'] if entry else None
    _cache[key] = {'at': now, 'value': value}
    return value


def rss_items(url):
    response = requests.get(url, headers=HEADERS, timeout=(6, 15))
    response.raise_for_status()
    items = []
    for item in ET.fromstring(response.content).iter('item'):
        title, link = (item.findtext('title') or '').strip(), (item.findtext('link') or '').strip()
        if not title or not LINK.match(link):
            continue
        try:
            published = parsedate_to_datetime(item.findtext('pubDate')).astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError):
            published = None
        source = item.findtext('source')
        if source and title.endswith(' - ' + source):   # Google News appends the outlet to the title.
            title = title[:-len(source) - 3]
        items.append({'title': title, 'link': link, 'published': published, 'source': source})
    return items


def news(per_topic=12):
    def fetch():
        seen, items, count = set(), [], {}
        for topic, url in NEWS_FEEDS:
            try:
                found = rss_items(url)
            except (requests.RequestException, ET.ParseError):
                continue           # One feed down must not blank the panel.
            for item in found:
                key = re.sub(r'\W+', ' ', item['title'].lower()).strip()
                # The most relevant per feed, capped, so one busy topic cannot crowd out the others.
                if key not in seen and not SOCIAL.search(item['source'] or '') and count.get(url, 0) < per_topic:
                    seen.add(key)
                    count[url] = count.get(url, 0) + 1
                    items.append({**item, 'topic': topic, 'source': item['source'] or 'OilPrice.com'})
        if not items:
            raise ValueError('No news feed answered.')
        return sorted(items, key=lambda i: i['published'] or '', reverse=True)
    return {'items': cached('news', timedelta(minutes=30), fetch) or [],
            'note': 'Headlines from Google News searches and OilPrice.com, newest first. Shown for context only; '
                    'they do not change any forecast or recommendation.'}


def warnings():
    fetch = lambda: [{'title': i['title'], 'link': i['link'].replace('http://', 'https://', 1), 'published': i['published']}  # noqa: E731
                     for i in rss_items(WARNINGS_URL)]
    return {'items': cached('warnings', timedelta(minutes=10), fetch) or [],
            'source': 'Bureau of Meteorology, Queensland warnings'}


def bushfires():
    def fetch():
        response = requests.get(BUSHFIRE_URL, headers=HEADERS, timeout=(6, 20))
        response.raise_for_status()
        features = []
        for feature in response.json()['features']:
            p = feature['properties']
            features.append({'type': 'Feature', 'geometry': feature['geometry'], 'properties': {
                'level': str(p.get('WarningLevel') or ''), 'title': str(p.get('WarningTitle') or '').strip(' -'),
                'text': str(p.get('WarningText') or '')[:600], 'status': str(p.get('CurrentStatus') or ''),
                'updated': p.get('PublishDateLocal_ISO')}})
        return {'type': 'FeatureCollection', 'features': features}
    return {**(cached('bushfires', timedelta(minutes=10), fetch) or {'type': 'FeatureCollection', 'features': []}),
            'source': 'Queensland Fire Department, current bushfire incidents'}
