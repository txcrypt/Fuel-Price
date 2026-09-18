"""Complete current API responses and deduplicated, first-seen price evidence."""
import json
import os
from pathlib import Path
import config
import pandas as pd

API_DOCS = 'https://www.fuelpricesqld.com.au/documents/FuelPricesQLDDirectAPI(OUT)v1.6.pdf'


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False), encoding='utf-8')
    os.replace(temporary, path)


def read_json(path, default=None):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return default


def price_status(value):
    try:
        price = float(value)
    except (ValueError, TypeError):
        return 'invalid'
    if price == 9999:
        return 'unavailable'
    return 'available' if 800 <= price <= 3500 else 'outside_analysis_range'


def metro_site(site):
    try:
        b = config.BOUNDS
        return b['lat_min'] <= float(site['Lat']) <= b['lat_max'] and b['lng_min'] <= float(site['Lng']) <= b['lng_max']
    except (KeyError, TypeError, ValueError):
        return False


class ApiEvidence:
    def __init__(self, root, db):
        self.path = Path(root) / '.local' / 'qld-api' / 'latest.json'
        self.db = db
        with db._connect() as connection:
            connection.execute('''CREATE TABLE IF NOT EXISTS api_price_events (
                site_id TEXT, fuel_id INTEGER, reported_at TEXT, raw_price REAL,
                collection_method TEXT, first_seen TEXT, last_seen TEXT,
                PRIMARY KEY(site_id, fuel_id, reported_at, raw_price))''')

    def save(self, evidence):
        ids = {str(s['S']) for s in evidence['sites'] if metro_site(s)}
        captured = evidence['retrieved_at']
        rows = [(str(p['SiteId']), p['FuelId'], p.get('TransactionDateUtc'), p.get('Price'),
                 p.get('CollectionMethod'), captured, captured)
                for p in evidence['prices'] if str(p['SiteId']) in ids]
        with self.db._connect() as connection:
            connection.executemany('''INSERT INTO api_price_events VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(site_id, fuel_id, reported_at, raw_price)
                DO UPDATE SET last_seen=excluded.last_seen''', rows)
        write_json(self.path, evidence)

    def payload(self):
        raw = read_json(self.path)
        if not raw:
            return {'status':'awaiting_capture', 'sites':[], 'coverage':{}, 'references':{}, 'fields':{}}
        brands = {str(r['BrandId']):r['Name'] for r in raw.get('brands', [])}
        fuels = {str(r['FuelId']):r['Name'] for r in raw.get('fuels', [])}
        regions = {(r['GeoRegionLevel'],r['GeoRegionId']):r['Name'] for r in raw.get('regions', [])}
        by_site = {}
        price_frame = pd.DataFrame(raw['prices'])
        price_frame['report_time'] = pd.to_datetime(price_frame.TransactionDateUtc, format='mixed', utc=True, errors='coerce')
        latest_prices = price_frame.sort_values('report_time', na_position='first').drop_duplicates(['SiteId','FuelId'], keep='last')
        for p in latest_prices.to_dict('records'):
            status = price_status(p.get('Price'))
            by_site.setdefault(str(p['SiteId']), []).append({
                'fuel_id':p['FuelId'], 'fuel':fuels.get(str(p['FuelId']), f"Fuel {p['FuelId']}"),
                'status':status, 'price_cpl':round(float(p['Price'])/10, 1) if status=='available' else None,
                'raw_price':p.get('Price'), 'reported_at':p.get('TransactionDateUtc'),
                'collection_method':p.get('CollectionMethod')})
        sites = []
        for s in raw['sites']:
            if not metro_site(s):
                continue
            prices = sorted(by_site.get(str(s['S']), []), key=lambda p:p['fuel_id'])
            u91 = next((p for p in prices if p['fuel_id']==2), None)
            sites.append({'site_id':str(s['S']), 'name':s.get('N'), 'address':s.get('A'),
                'brand':brands.get(str(s.get('B')), f"Brand {s.get('B')}"), 'brand_id':s.get('B'),
                'postcode':s.get('P'), 'suburb':regions.get((1,s.get('G1'))), 'city':regions.get((2,s.get('G2'))),
                'latitude':s.get('Lat'), 'longitude':s.get('Lng'), 'metadata_modified_at':s.get('M'),
                'google_place_id':s.get('GPI'), 'u91_status':u91['status'] if u91 else 'not_listed',
                'fuels':prices, 'raw_site':s})
        coverage = {status:sum(s['u91_status']==status for s in sites)
                    for status in ['available','unavailable','not_listed','outside_analysis_range','invalid']}
        with self.db._connect() as connection:
            events = connection.execute('SELECT COUNT(*) FROM api_price_events').fetchone()[0]
        return {'status':'recorded', 'retrieved_at':raw['retrieved_at'], 'docs_url':API_DOCS,
                'references':raw.get('references', {}), 'sites':sites,
                'coverage':{'qld_sites':len(raw['sites']), 'qld_price_rows':len(raw['prices']),
                            'metro_sites':len(sites), 'metro_price_rows':sum(len(s['fuels']) for s in sites),
                            'price_events_saved':events, **coverage},
                'fields':{name:[{'field':key, 'populated':sum(row.get(key) not in [None,''] for row in rows), 'rows':len(rows)}
                                for key in sorted(set().union(*(r.keys() for r in rows)))]
                          for name in ['sites','prices','brands','fuels','regions'] if (rows:=raw.get(name, []))}}
