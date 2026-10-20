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


# Local news for a searched place: only stories about things that affect people there. Google's
# search ignores when:3d, so age is enforced here, and an untagged story (sport, property
# listings, lifestyle) is dropped rather than shown.
# Whole words only (closing \b as well): "Courtney" is not a court, "waterfront" not water, "fireworks" not a fire.
IMPORTANT = [
    ('Emergency', r'fires?|bushfires?|blaze|flood(?:s|ing|ed)?|cyclones?|storms?|evacuat\w*|emergency|rescued?|warnings?|outages?|blackouts?|missing'),
    ('Crime & police', r'police|charged|charges|arrest(?:ed|s)?|crime|stab(?:s|bed|bing)?|shoot(?:ing|s)?|shot|murder(?:ed|s)?|assault(?:ed|s)?|stolen|robbery|court|jail(?:ed)?|sentenced?|alleged(?:ly)?'),
    ('Health', r'hospitals?|health|ambulances?|outbreaks?|virus|patients?|disease'),
    ('Government & policy', r'council|government|minister|premier|policy|budget|election|mayor|parliament|laws?|legislation|funding|inquiry'),
    ('Roads & transport', r'crash(?:es|ed)?|roads?|highway|traffic|bus(?:es)?|trains?|rail|airport|closures?|bridge'),
    ('Cost of living & housing', r'housing|rent(?:s|al)?|homeless(?:ness)?|jobs|cost of living|prices|power bills|water'),
]
# Sport reads as news ("Storm", "Broncos", "grand final") but is not what this panel is for.
SPORT = re.compile(r'\b(nrl|afl|a-league|rugby|league|cricket|netball|grand final|premiership|broncos|melbourne storm|titans|cowboys|dolphins|'
                   r'taipans|36ers|lions|roar|bullets|tipping|round \d+|coach|squad|match|fixture|draw)\b', re.I)
LOCAL_QUERY = ('(police OR crime OR court OR council OR government OR minister OR emergency OR crash OR fire OR flood '
               'OR cyclone OR storm OR hospital OR health OR policy OR election OR budget OR road OR housing) '
               '-"death notices" when:3d')
# Ignored when grouping headlines: filler, and the topic words every story in a category shares.
COMMON = set('after with from over into amid says said this that their have been will more year years woman '
             'news update region queensland australia locals local residents people police charged charges crash '
             'fire fires flood council government health hospital road roads highway court crime found investigating'.split())
LOCAL_CACHE_LIMIT = 200


def words(title, place=''):
    return {w for w in re.findall(r'[a-z]{4,}', title.lower()) if w not in place.lower() and w not in COMMON}


def same_story(a, b):
    """Several outlets' headlines on one event share most distinctive words - at least three, once filler
    and topic words are removed, since "charged after crash" alone joins unrelated stories."""
    shared = len(a & b)
    return shared >= 3 and shared / min(len(a), len(b)) >= .5


def tag(title):
    if SPORT.search(title):
        return None
    return next((label for label, pattern in IMPORTANT if re.search(r'\b(?:' + pattern + r')\b', title, re.I)), None)


def local_news(place, region='', now=None, limit=12):
    """Public-interest stories for a place. *region* (the state) keeps "Springfield" or "Richmond"
    from returning another country's town of the same name."""
    place = re.sub(r'["()]', '', place).strip()[:60]
    region = re.sub(r'["()]', '', region).strip()[:40]
    if not place:
        return []
    now = now or datetime.now(timezone.utc)

    def fetch():
        from urllib.parse import quote_plus
        items = []
        scope = f'"{place}" "{region}"' if region else f'"{place}"'
        for item in rss_items(GOOGLE + quote_plus(f'{scope} {LOCAL_QUERY}')):
            published = datetime.fromisoformat(item['published']) if item['published'] else None
            label = tag(item['title'])
            if not published or now - published > timedelta(days=7) or not label or SOCIAL.search(item['source'] or ''):
                continue
            key = words(item['title'], place)
            story = next((i for i in items if same_story(i['_words'], key)), None)
            if story:          # Another outlet on the same event: count it, keep the first (most relevant) headline.
                if item['source'] not in story['outlets']:
                    story['outlets'].append(item['source'])
                continue
            hours = (now - published).total_seconds() / 3600
            items.append({**item, 'tag': label, 'breaking': hours < 6, 'hours_old': round(hours, 1),
                          'outlets': [item['source']], '_words': key})
        for item in items:
            del item['_words']
        # Google's relevance order, but the last three days first: recent and important beats merely important.
        return sorted(items, key=lambda i: i['hours_old'] > 72)[:limit]
    # Every distinct place adds an entry; keep the newest LOCAL_CACHE_LIMIT.
    local = sorted((k for k in _cache if k.startswith('local:')), key=lambda k: _cache[k]['at'])
    for key in local[:max(0, len(local) - LOCAL_CACHE_LIMIT + 1)]:
        del _cache[key]
    return cached(f'local:{place.lower()}|{region.lower()}', timedelta(minutes=20), fetch, now) or []


# EV chargers from Open Charge Map, for one map area. The key stays on this server.
OCM_URL = 'https://api.openchargemap.io/v3/poi/'


def ev_chargers(south, west, north, east, key):
    if not key:
        return {'status': 'not_configured', 'chargers': [],
                'note': 'Add a free Open Charge Map key as OCM_API_KEY in .env, then restart, to show EV chargers.'}
    # Round outward to a quarter degree so nearby views share one cached request.
    box = tuple(round(v * 4) / 4 for v in (south - .125, west - .125, north + .125, east + .125))

    def fetch():
        response = requests.get(OCM_URL, headers={**HEADERS, 'X-API-Key': key}, timeout=(6, 20), params={
            'output': 'json', 'countrycode': 'AU', 'maxresults': 500, 'compact': 'false', 'verbose': 'false',
            'boundingbox': f'({box[0]},{box[1]}),({box[2]},{box[3]})'})
        response.raise_for_status()
        chargers = []
        for poi in response.json():
            a = poi.get('AddressInfo') or {}
            if a.get('Latitude') is None:
                continue
            connections = [{'type': (c.get('ConnectionType') or {}).get('Title'), 'kw': c.get('PowerKW'),
                            'count': c.get('Quantity')} for c in poi.get('Connections') or []]
            chargers.append({'name': a.get('Title'), 'lat': a['Latitude'], 'lng': a['Longitude'],
                             'address': ', '.join(x for x in (a.get('AddressLine1'), a.get('Town')) if x),
                             'operator': (poi.get('OperatorInfo') or {}).get('Title'), 'points': poi.get('NumberOfPoints'),
                             'cost': poi.get('UsageCost'), 'operational': (poi.get('StatusType') or {}).get('IsOperational'),
                             'max_kw': max((c['kw'] for c in connections if c['kw']), default=None), 'connections': connections})
        return chargers
    chargers = cached(f'ev:{box}', timedelta(hours=24), fetch)
    return {'status': 'ok' if chargers is not None else 'unavailable', 'chargers': chargers or [],
            'source': 'Open Charge Map (CC BY 4.0 / community data)'}

