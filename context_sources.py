"""Keyless public context feeds. Kept separate from validated forecasting inputs."""
from datetime import datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
import re
import threading
import requests
from api_evidence import read_json, write_json

SOURCES = {
    'aip': {'name':'AIP Brisbane wholesale ULP', 'url':'https://aip.com.au/pricing/terminal-gate-prices',
            'unit':'c/L including GST', 'kind':'Public HTML table', 'purpose':'Wholesale direction and the retail–wholesale price gap.'},
    'rba': {'name':'RBA AUD/USD', 'url':'https://www.rba.gov.au/statistics/frequency/exchange-rates.html',
            'unit':'USD per AUD', 'kind':'Public HTML table', 'purpose':'Currency context for USD-denominated imported fuel.'},
}


class Tables(HTMLParser):
    """Small parser for the publishers' dated tables; no browser or scraping dependency."""
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
            self.table = {'heading':self.heading, 'rows':[]}
        if self.table is not None and tag == 'tr':
            self.row = []
        if self.row is not None and tag in ['th','td']:
            self.cell = ''

    def handle_data(self, text):
        if self.in_heading:
            self.heading += text
        if self.cell is not None:
            self.cell += text

    def handle_endtag(self, tag):
        if tag == 'h2':
            self.in_heading = False
        if tag in ['th','td'] and self.cell is not None:
            self.row.append(' '.join(self.cell.split()))
            self.cell = None
        if tag == 'tr' and self.row is not None:
            self.table['rows'].append(self.row)
            self.row = None
        if tag == 'table' and self.table is not None:
            self.tables.append(self.table)
            self.table = None


def parse_source(name, html):
    parser = Tables()
    parser.feed(html)
    if name == 'aip':
        table = next((t for t in parser.tables if 'Petrol (ULP' in t['heading'] and 'inclusive of GST' in t['heading']), None)
        target = 'Brisbane'
    else:
        table = next((t for t in parser.tables if any(r and r[0]=='United States dollar' for r in t['rows'])), None)
        target = 'United States dollar'
    if not table:
        raise ValueError('Expected source table was not found; layout may have changed.')
    row = next((r for r in table['rows'] if r and r[0]==target), None)
    header = next((r for r in table['rows'] if any(re.search(r'\b20\d{2}\b', c) for c in r)), None)
    if not row or not header:
        raise ValueError('Source dates or values are missing.')
    labels = [c for c in header if re.search(r'\b20\d{2}\b', c)]
    if len(labels) != len(row)-1:
        raise ValueError('Source dates do not align with the value columns.')
    result = []
    for label, value in zip(labels, row[1:]):
        clean = re.sub(r'(\d+)(st|nd|rd|th)\b', r'\1', label)
        day = datetime.strptime(clean, '%A, %d %B %Y' if name=='aip' else '%d %b %Y').date()
        number = float(value)
        if not (0 < number < (1000 if name=='aip' else 5)) or day > datetime.now().date():
            raise ValueError('Source value or date failed validation.')
        result.append({'date':day.isoformat(), 'value':number})
    return sorted(result, key=lambda r:r['date'])


class ContextSources:
    interval_seconds = 21600

    def __init__(self, root, db):
        self.root = Path(root) / '.local' / 'context'
        self.db = db
        self.lock = threading.Lock()
        self.state = read_json(self.root / 'status.json', {})
        with db._connect() as connection:
            connection.execute('''CREATE TABLE IF NOT EXISTS context_observations (
                source TEXT, observed_date TEXT, value REAL, unit TEXT, first_seen TEXT, last_seen TEXT,
                PRIMARY KEY(source, observed_date))''')

    def refresh(self):
        with self.lock:
            for name, source in SOURCES.items():
                previous = self.state.get(name, {})
                now = datetime.now()
                last = previous.get('attempted_at')
                # Retry failed providers hourly; retain their last successful observations.
                interval = 3600 if previous.get('status')=='error' else self.interval_seconds
                if last and 0 <= (now-datetime.fromisoformat(last)).total_seconds() < interval:
                    continue
                attempted = now.isoformat(timespec='seconds')
                try:
                    response = requests.get(source['url'], timeout=(10,25), headers={'User-Agent':'FuelObservatory/1.0 (local research)'})
                    response.raise_for_status()
                    points = parse_source(name, response.text)
                    self.root.mkdir(parents=True, exist_ok=True)
                    temporary = self.root / f'{name}.tmp'
                    temporary.write_text(response.text, encoding='utf-8')
                    temporary.replace(self.root / f'{name}.html')
                    with self.db._connect() as connection:
                        connection.executemany('''INSERT INTO context_observations VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(source, observed_date) DO UPDATE SET
                            value=excluded.value, last_seen=excluded.last_seen''',
                            [(name,p['date'],p['value'],source['unit'],attempted,attempted) for p in points])
                    self.state[name] = {'status':'connected','attempted_at':attempted,'retrieved_at':attempted,
                                        'source_date':points[-1]['date'],'error':None}
                except (requests.RequestException, ValueError, OSError):
                    self.state[name] = {**previous,'status':'error','attempted_at':attempted,
                                        'error':'Fetch or source validation failed; last successful values retained.'}
                write_json(self.root / 'status.json', self.state)

    def payload(self, dashboard):
        # Immutable copies keep reads independent of network refresh latency.
        state = dict(self.state)
        sources = []
        with self.db._connect() as connection:
            for name, definition in SOURCES.items():
                points = [{'date':d,'value':v,'first_seen':first} for d,v,first in connection.execute(
                    'SELECT observed_date,value,first_seen FROM context_observations WHERE source=? ORDER BY observed_date', (name,))]
                status = state.get(name, {'status':'awaiting_fetch'})
                source_age = (datetime.now().date()-datetime.fromisoformat(points[-1]['date']).date()).days if points else None
                sources.append({'id':name, **definition, **status, 'points':points,
                                'stale':status.get('status')!='connected' or source_age is None or source_age>4,
                                'change':round(points[-1]['value']-points[-2]['value'],4) if len(points)>1 else None})
        tgp = {p['date']:p['value'] for p in sources[0]['points']}
        paired = [{'date':p['date'], 'retail':p['price'], 'wholesale':tgp[p['date']],
                   'gap':round(p['price']-tgp[p['date']],1)} for p in dashboard['history'] if p['date'] in tgp]
        return {'sources':sources, 'paired_history':paired, 'refresh_seconds':self.interval_seconds,
                'model_use':'Context only; no predictive improvement claimed. Not included in buying recommendations.',
                'gap_note':'Same-date metro retail median minus AIP average wholesale ULP, both GST-inclusive. This gap includes costs and margins; it is not station profit.'}
