"""Fetch real Brisbane unleaded 91 prices from the Queensland fuel API."""
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd
import requests
import config
from api_evidence import read_json, write_json


class FuelEngine:
    base_url = 'https://fppdirectapi-prod.fuelpricesqld.com.au'

    endpoints = {
        'sites':('/Subscriber/GetFullSiteDetails', 'S'),
        'prices':('/Price/GetSitesPrices', 'SitePrices'),
        'brands':('/Subscriber/GetCountryBrands', 'Brands'),
        'fuels':('/Subscriber/GetCountryFuelTypes', 'Fuels'),
        'regions':('/Subscriber/GetCountryGeographicRegions', 'GeographicRegions'),
    }

    def __init__(self, token=None, cache_dir=None):
        self.token = config.FUEL_API_TOKEN if token is None else token
        self.cache_dir = Path(cache_dir or config.BASE_DIR / '.local' / 'qld-api')
        self.last_evidence = None

    def _get(self, endpoint, key):
        if not self.token:
            raise RuntimeError('FUEL_API_TOKEN is missing. Add it to the local .env file.')
        try:
            response = requests.get(
                self.base_url + endpoint,
                headers={'Authorization': f'FPDAPI SubscriberToken={self.token}'},
                params={'countryId':21, **({'geoRegionLevel':3,'geoRegionId':1}
                        if endpoint.endswith(('GetFullSiteDetails','GetSitesPrices')) else {})},
                timeout=(10, 30),
            )
            response.raise_for_status()
            data = response.json().get(key)
        except requests.HTTPError as exc:
            raise RuntimeError(f'Queensland API returned HTTP {exc.response.status_code}.') from None
        except (requests.RequestException, ValueError):
            raise RuntimeError('Queensland API could not be reached or returned invalid JSON.') from None
        if not isinstance(data, list) or not data:
            raise RuntimeError(f'Queensland API returned no {key} records.')
        return data

    def _cached(self, name):
        path = self.cache_dir / f'{name}.json'
        cached = read_json(path)
        ttl = 60 if name=='prices' else 86400
        if cached:
            age = (datetime.now()-datetime.fromisoformat(cached['retrieved_at'])).total_seconds()
            if 0 <= age < ttl:
                return {**cached, 'status':'cached', 'error':None}
        try:
            result = {'data':self._get(*self.endpoints[name]),
                      'retrieved_at':datetime.now().isoformat(timespec='seconds')}
            write_json(path, result)
            return {**result, 'status':'fresh', 'error':None}
        except (RuntimeError, OSError) as exc:
            if name=='prices' or (name=='sites' and not cached):
                raise
            return {**(cached or {'data':[], 'retrieved_at':None}),
                    'status':'stale' if cached else 'unavailable', 'error':str(exc)}

    def get_market_snapshot(self):
        responses = {name:self._cached(name) for name in self.endpoints}
        self.last_evidence = {name:response['data'] for name,response in responses.items()}
        self.last_evidence.update(retrieved_at=responses['prices']['retrieved_at'],
            references={name:{k:v for k,v in response.items() if k!='data'} for name,response in responses.items()})
        sites = pd.DataFrame(responses['sites']['data']).rename(columns={
            'S': 'site_id', 'N': 'name', 'Lat': 'latitude', 'Lng': 'longitude',
            'B': 'brand_id', 'P': 'postcode',
        })
        prices = pd.DataFrame(responses['prices']['data'])
        # Validate after choosing the latest report: unavailable must not resurrect an older price.
        prices['report_time'] = pd.to_datetime(prices.TransactionDateUtc, format='mixed', utc=True, errors='coerce')
        prices = prices.sort_values('report_time', na_position='first').drop_duplicates(['SiteId','FuelId'], keep='last')
        prices = prices.loc[prices['FuelId'] == 2].rename(columns={
            'SiteId': 'site_id', 'TransactionDateUtc': 'reported_at',
        }).copy()
        prices['price_cpl'] = pd.to_numeric(prices['Price'], errors='coerce') / 10
        prices = prices.loc[prices.price_cpl.between(80, 350)]
        b = config.BOUNDS
        sites = sites.loc[
            sites.latitude.between(b['lat_min'], b['lat_max']) &
            sites.longitude.between(b['lng_min'], b['lng_max'])
        ]
        df = prices[['site_id', 'price_cpl', 'reported_at']].merge(
            sites[['site_id', 'name', 'latitude', 'longitude', 'brand_id', 'postcode']],
            on='site_id', how='inner',
        ).drop_duplicates('site_id')
        if df.empty:
            raise RuntimeError('Queensland API returned no valid Brisbane U91 prices.')
        df['region'] = np.where(df.latitude > -27.470, 'North', 'South')
        df['state'] = 'QLD'
        df['scraped_at'] = responses['prices']['retrieved_at'].replace('T',' ')
        return df
