"""Public context feeds. Kept separate from validated forecasting inputs."""
import json
from datetime import datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from xml.etree import ElementTree
import re
import threading
import requests
import config
from api_evidence import read_json, write_json

EIA_SPOT = 'https://api.eia.gov/v2/petroleum/pri/spt/data/'
EIA_PARAMS = {'frequency': 'daily', 'data[0]': 'value', 'sort[0][column]': 'period',
              'sort[0][direction]': 'desc', 'length': 60}
# RSS-CB 1.2 namespaces used by the RBA exchange-rate feed.
CB = '{http://www.cbwiki.net/wiki/index.php/Specification_1.2/}'
RSS = '{http://purl.org/rss/1.0/}'

SOURCES = {
    'aip': {'name': 'AIP Brisbane wholesale ULP', 'url': 'https://aip.com.au/pricing/terminal-gate-prices',
            'unit': 'c/L including GST', 'kind': 'Public HTML table', 'suffix': 'html', 'decimals': 1,
            'limit': 1000, 'max_age_days': 4,
            'purpose': 'Wholesale direction and the retail-wholesale price gap.'},
    'rba': {'name': 'RBA AUD/USD', 'url': 'https://www.rba.gov.au/rss/rss-cb-exchange-rates.xml',
            'unit': 'USD per AUD', 'kind': 'Official RSS-CB feed', 'suffix': 'xml', 'decimals': 4,
            'limit': 5, 'max_age_days': 4,
            'purpose': 'Currency context for USD-denominated imported fuel. One 4.00 pm observation per publication.'},
    'eia_gasoline': {'name': 'EIA US Gulf Coast petrol spot', 'url': 'https://www.eia.gov/dnav/pet/pet_pri_spt_s1_d.htm',
                     'unit': 'USD per US gallon', 'kind': 'EIA v2 API / registered key', 'suffix': 'json',
                     'decimals': 3, 'limit': 100, 'max_age_days': 8, 'key_attr': 'EIA_API_KEY',
                     'request_url': EIA_SPOT, 'params': {**EIA_PARAMS, 'facets[series][]': 'EER_EPMRU_PF4_RGC_DPG'},
                     'purpose': 'Refined-petrol cost pressure. This is US Gulf Coast conventional gasoline, NOT Singapore Mogas 95.'},
    'eia_brent': {'name': 'EIA Brent crude spot', 'url': 'https://www.eia.gov/dnav/pet/pet_pri_spt_s1_d.htm',
                  'unit': 'USD per barrel', 'kind': 'EIA v2 API / registered key', 'suffix': 'json',
                  'decimals': 2, 'limit': 1000, 'max_age_days': 8, 'key_attr': 'EIA_API_KEY',
                  'request_url': EIA_SPOT, 'params': {**EIA_PARAMS, 'facets[series][]': 'RBRTE'},
                  'purpose': 'Broader crude-cost pressure. Crude is not a substitute for a refined-petrol benchmark.'},
}


class Tables(HTMLParser):
    """Small parser for the publisher's dated tables; no browser or scraping dependency."""
    def __init__(self):
        super().__init__()
        self.tables = []
        self.table = None
        self.row = None
        self.cell = None
        self.heading = ''
        self.in_heading = False

    def handle_starttag(self, tag, attrs):
        if tag == 'h2':
            self.heading = ''
            self.in_heading = True
        if tag == 'table':
            self.table = {'heading': self.heading, 'rows': []}
        if self.table is not None and tag == 'tr':
            self.row = []
        if self.row is not None and tag in ['th', 'td']:
            self.cell = ''

    def handle_data(self, text):
        if self.in_heading:
            self.heading += text
        if self.cell is not None:
            self.cell += text

    def handle_endtag(self, tag):
        if tag == 'h2':
            self.in_heading = False
        if tag in ['th', 'td'] and self.cell is not None:
            self.row.append(' '.join(self.cell.split()))
            self.cell = None
        if tag == 'tr' and self.row is not None:
            self.table['rows'].append(self.row)
            self.row = None
        if tag == 'table' and self.table is not None:
            self.tables.append(self.table)
            self.table = None


def parse_aip(text):
    parser = Tables()
    parser.feed(text)
    table = next((t for t in parser.tables if 'Petrol (ULP' in t['heading'] and 'inclusive of GST' in t['heading']), None)
    if not table:
        raise ValueError('Expected source table was not found; layout may have changed.')
    row = next((r for r in table['rows'] if r and r[0] == 'Brisbane'), None)
    header = next((r for r in table['rows'] if any(re.search(r'\b20\d{2}\b', c) for c in r)), None)
    if not row or not header:
        raise ValueError('Source dates or values are missing.')
    labels = [c for c in header if re.search(r'\b20\d{2}\b', c)]
    if len(labels) != len(row) - 1:
        raise ValueError('Source dates do not align with the value columns.')
    return [{'date': datetime.strptime(re.sub(r'(\d+)(st|nd|rd|th)\b', r'\1', label), '%A, %d %B %Y').date().isoformat(),
             'value': float(value)} for label, value in zip(labels, row[1:])]


def parse_rba(text):
    """Read the USD observation from the RBA's published RSS-CB feed."""
    try:
        root = ElementTree.fromstring(text)
    except ElementTree.ParseError:
        raise ValueError('RBA feed is not valid XML.') from None
    for item in root.iter(f'{RSS}item'):
        rate = item.find(f'.//{CB}exchangeRate')
        if rate is None or (rate.findtext(f'{CB}targetCurrency') or '').strip() != 'USD':
            continue
        if (rate.findtext(f'{CB}baseCurrency') or '').strip() != 'AUD':
            raise ValueError('RBA feed changed its base currency; USD per AUD is no longer implied.')
        value = rate.findtext(f'.//{CB}observation/{CB}value')
        period = rate.findtext(f'.//{CB}observationPeriod/{CB}period')
        if value is None or period is None:
            raise ValueError('RBA USD entry is missing its value or observation period.')
        return [{'date': datetime.strptime(period.strip(), '%Y-%m-%d').date().isoformat(), 'value': float(value)}]
    raise ValueError('RBA feed did not contain a USD observation.')


def parse_eia(text):
    """Read dated observations from an EIA v2 series response. The body never carries the request key."""
    try:
        rows = json.loads(text)['response']['data']
    except (ValueError, KeyError, TypeError):
        raise ValueError('EIA response did not contain a series data block.') from None
    if not isinstance(rows, list) or not rows:
        raise ValueError('EIA returned no observations for this series.')
    points = [{'date': datetime.strptime(str(row['period']), '%Y-%m-%d').date().isoformat(),
               'value': float(row['value'])} for row in rows if row.get('value') not in (None, '')]
    if not points:
        raise ValueError('EIA returned only empty observations for this series.')
    return points


PARSERS = {'aip': parse_aip, 'rba': parse_rba, 'eia_brent': parse_eia, 'eia_gasoline': parse_eia}

REFRESH_SECONDS = 21600   # Normal cycle: at most every six hours.
ERROR_RETRY_SECONDS = 3600  # A failed provider is retried hourly until it answers.


def retry_seconds(status):
    """How long until the next attempt for a source in this state."""
    return ERROR_RETRY_SECONDS if status == 'error' else REFRESH_SECONDS


def failure_category(error):
    """A short, safe reason a fetch failed.

    Derived from the exception *type* and the HTTP status code only. A provider's
    exception text can echo the full request URL including its api_key, so the
    message is never read, formatted or stored — only these fixed strings are.
    """
    status = getattr(getattr(error, 'response', None), 'status_code', None)
    if isinstance(status, int):
        return ('Provider rejected the credential (HTTP %d)' % status if status in (401, 403) else
                'Provider rate limit reached (HTTP 429)' if status == 429 else
                'Provider server error (HTTP %d)' % status if status >= 500 else
                'Provider refused the request (HTTP %d)' % status)
    # requests exceptions subclass OSError, so they are tested first.
    if isinstance(error, requests.Timeout):
        return 'No response before the timeout'
    if isinstance(error, requests.ConnectionError):
        return 'Network or DNS failure reaching the provider'
    if isinstance(error, requests.RequestException):
        return 'Request failed before a response was read'
    if isinstance(error, ValueError):
        return 'Response was read but failed source validation'
    if isinstance(error, OSError):
        return 'Local cache write failed'
    return 'Fetch failed'


def observations_as_of(db, source, known_at):
    """One source's values exactly as they were known at ``known_at``.

    Returns, per observed date, the latest vintage recorded on or before that
    timestamp. A revision recorded later cannot appear, which is what makes a
    replay of a past forecast a replay rather than a rewrite.
    """
    with db._connect() as connection:
        return {date: value for date, value in connection.execute(
            '''SELECT observed_date, value FROM context_vintages AS v
               WHERE source=? AND known_at<=? AND known_at=(
                   SELECT MAX(known_at) FROM context_vintages
                   WHERE source=v.source AND observed_date=v.observed_date AND known_at<=?)
               ORDER BY observed_date''', (source, known_at, known_at))}


def parse_source(name, text):
    source = SOURCES[name]
    points = PARSERS[name](text)
    today = datetime.now().date()
    for point in points:
        if not 0 < point['value'] < source['limit'] or datetime.fromisoformat(point['date']).date() > today:
            raise ValueError('Source value or date failed validation.')
    return sorted(points, key=lambda p: p['date'])


class ContextSources:
    interval_seconds = REFRESH_SECONDS

    def __init__(self, root, db):
        self.root = Path(root) / '.local' / 'context'
        self.db = db
        self.lock = threading.Lock()
        self.state = read_json(self.root / 'status.json', {})
        with db._connect() as connection:
            connection.executescript('''
            /* Latest known value per observed day. Revisions overwrite `value` here,
               so this table alone is not a record of what was known when. */
            CREATE TABLE IF NOT EXISTS context_observations (
                source TEXT, observed_date TEXT, value REAL, unit TEXT, first_seen TEXT, last_seen TEXT,
                PRIMARY KEY(source, observed_date));
            /* Append-only revision archive: one row the first time a value is seen for an
               observed day, and one more each time the publisher revises it. `known_at` is
               when this process actually held that value. Never updated, never deleted. */
            CREATE TABLE IF NOT EXISTS context_vintages (
                source TEXT, observed_date TEXT, value REAL, known_at TEXT,
                PRIMARY KEY(source, observed_date, known_at));''')

    def refresh(self):
        with self.lock:
            for name, source in SOURCES.items():
                previous = self.state.get(name, {})
                now = datetime.now()
                last = previous.get('attempted_at')
                # Retry failed providers hourly; retain their last successful observations.
                interval = retry_seconds(previous.get('status'))
                if last and 0 <= (now - datetime.fromisoformat(last)).total_seconds() < interval:
                    continue
                attempted = now.isoformat(timespec='seconds')
                params = dict(source.get('params') or {})
                if source.get('key_attr'):
                    key = getattr(config, source['key_attr'], '')
                    if not key:
                        # A missing key is a configuration state, not a provider failure.
                        self.state[name] = {**previous, 'status': 'not_configured', 'attempted_at': attempted,
                                            'failure_category': 'Not configured',
                                            'error': f"No {source['key_attr']} configured in the local .env file."}
                        write_json(self.root / 'status.json', self.state)
                        continue
                    params['api_key'] = key
                try:
                    response = requests.get(source.get('request_url', source['url']), params=params, timeout=(10, 25),
                                            headers={'User-Agent': 'FuelObservatory/1.0 (local research)'})
                    response.raise_for_status()
                    points = parse_source(name, response.text)
                    self.root.mkdir(parents=True, exist_ok=True)
                    temporary = self.root / f'{name}.tmp'
                    temporary.write_text(response.text, encoding='utf-8')
                    temporary.replace(self.root / f"{name}.{source['suffix']}")
                    with self.db._connect() as connection:
                        # Recorded before the upsert below overwrites the previous value, and
                        # only when the value differs from the one currently held, so an
                        # unchanged republication does not grow the archive.
                        connection.executemany('''INSERT OR IGNORE INTO context_vintages
                            (source, observed_date, value, known_at) SELECT ?, ?, ?, ?
                            WHERE NOT EXISTS (SELECT 1 FROM context_observations
                                              WHERE source=? AND observed_date=? AND value=?)''',
                            # Sub-second known_at: two refreshes inside one second are rare but must
                            # not collide on the primary key and silently discard a revision.
                            [(name, p['date'], p['value'], now.isoformat(), name, p['date'], p['value'])
                             for p in points])
                        connection.executemany('''INSERT INTO context_observations VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(source, observed_date) DO UPDATE SET
                            value=excluded.value, last_seen=excluded.last_seen''',
                            [(name, p['date'], p['value'], source['unit'], attempted, attempted) for p in points])
                    self.state[name] = {'status': 'connected', 'attempted_at': attempted, 'retrieved_at': attempted,
                                        'source_date': points[-1]['date'], 'error': None, 'failure_category': None,
                                        'consecutive_failures': 0}
                except (requests.RequestException, ValueError, OSError) as error:
                    # Fixed wording: a provider exception string can echo the request URL and its key.
                    self.state[name] = {**previous, 'status': 'error', 'attempted_at': attempted,
                                        'failure_category': failure_category(error),
                                        'consecutive_failures': (previous.get('consecutive_failures') or 0) + 1,
                                        'error': 'Fetch or source validation failed; last successful values retained.'}
                write_json(self.root / 'status.json', self.state)

    def payload(self, dashboard):
        # Immutable copies keep reads independent of network refresh latency.
        state = dict(self.state)
        sources = []
        with self.db._connect() as connection:
            for name, definition in SOURCES.items():
                points = [{'date': d, 'value': v, 'first_seen': first} for d, v, first in connection.execute(
                    'SELECT observed_date,value,first_seen FROM context_observations WHERE source=? ORDER BY observed_date', (name,))]
                status = state.get(name, {'status': 'awaiting_fetch'})
                source_age = (datetime.now().date() - datetime.fromisoformat(points[-1]['date']).date()).days if points else None
                # Request parameters can carry a private key and never leave this process.
                public = {k: v for k, v in definition.items() if k not in ('params', 'key_attr', 'request_url')}
                wait = retry_seconds(status.get('status'))
                attempted = status.get('attempted_at')
                revisions, = connection.execute(
                    'SELECT COUNT(*) - COUNT(DISTINCT observed_date) FROM context_vintages WHERE source=?', (name,)).fetchone()
                sources.append({'id': name, **public, **status, 'points': points,
                                'stale': status.get('status') != 'connected' or source_age is None or source_age > definition['max_age_days'],
                                'source_age_days': source_age, 'retry_seconds': wait,
                                'next_retry_at': (datetime.fromisoformat(attempted) + timedelta(seconds=wait)
                                                  ).isoformat(timespec='seconds') if attempted else None,
                                'revisions_recorded': revisions,
                                'change': round(points[-1]['value'] - points[-2]['value'], 4) if len(points) > 1 else None})
        tgp = {p['date']: p['value'] for p in next(s for s in sources if s['id'] == 'aip')['points']}
        paired = [{'date': p['date'], 'retail': p['price'], 'wholesale': tgp[p['date']],
                   'gap': round(p['price'] - tgp[p['date']], 1)} for p in dashboard['history'] if p['date'] in tgp]
        # Source health is reported on its own terms: the fuel feed can be healthy while
        # a context provider is failing, and neither state should be read from the other.
        failing = [s['id'] for s in sources if s['status'] == 'error']
        pending = [s['next_retry_at'] for s in sources if s['status'] in ('error', 'not_configured') and s['next_retry_at']]
        health = {'total': len(sources), 'connected': sum(s['status'] == 'connected' for s in sources),
                  'failing': failing, 'not_configured': [s['id'] for s in sources if s['status'] == 'not_configured'],
                  'stale': [s['id'] for s in sources if s['stale'] and s['status'] == 'connected'],
                  'next_retry_at': min(pending) if pending else None,
                  'categories': {s['id']: s.get('failure_category') for s in sources if s.get('failure_category')},
                  'note': 'Context-source health only. It is independent of fuel collection: the Queensland '
                          'fuel feed can be current while a context provider is failing, and a failing context '
                          'source never withholds or ages the fuel data.'}
        return {'sources': sources, 'paired_history': paired, 'refresh_seconds': self.interval_seconds,
                'health': health,
                'vintage_note': 'Every first sighting and every later revision is kept in context_vintages with the '
                                'timestamp this app actually held it. A replay reads the vintage known at the origin '
                                'date, never a later revision.',
                'model_use': 'Context only; no predictive improvement claimed. Not included in buying recommendations.',
                'gap_note': 'Same-date metro retail median minus AIP average wholesale ULP, both GST-inclusive. This gap includes costs and margins; it is not station profit.',
                'benchmark_note': 'The ACCC identifies Singapore Mogas 95 as the relevant refined-petrol benchmark. No free daily Mogas feed has been verified; the EIA US Gulf Coast petrol series is a different market and is not relabelled as Mogas 95.'}
