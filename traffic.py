"""Public QLDTraffic events; bounded request cadence independent of fuel collection."""
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import Counter
import math
import threading
import requests
import config
from api_evidence import read_json, write_json

SOURCE = 'https://qldtraffic.qld.gov.au/more/Developers-and-Data/'
URL = 'https://api.qldtraffic.qld.gov.au/v2/events'
INTERVAL = 300


def timestamp(value):
    try:
        result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return result if result.tzinfo else result.replace(tzinfo=timezone(timedelta(hours=10)))
    except (ValueError, TypeError):
        return None


def positions(geometry):
    """Read supported GeoJSON positions, including nested area-alert geometries."""
    if not isinstance(geometry, dict):
        return []
    if geometry.get('type') == 'GeometryCollection':
        geometries=geometry.get('geometries')
        return [p for g in geometries for p in positions(g)] if isinstance(geometries,list) else []
    if geometry.get('type') not in ['Point','MultiPoint','LineString','MultiLineString','Polygon','MultiPolygon']:
        return []

    def walk(values):
        if not isinstance(values, list) or not values:
            return []
        if isinstance(values[0], (int,float)):
            if len(values)<2 or not all(isinstance(v,(int,float)) and math.isfinite(v) for v in values[:2]):
                return []
            return [values[:2]] if abs(values[0])<=180 and abs(values[1])<=90 else []
        return [p for child in values for p in walk(child)]
    return walk(geometry.get('coordinates'))


def metro_feature(feature):
    points=positions(feature.get('geometry'))
    if not points:
        return False
    b=config.BOUNDS
    # Include crossing lines and enclosing area alerts, not just vertices inside the region.
    return (min(p[0] for p in points)<=b['lng_max'] and max(p[0] for p in points)>=b['lng_min']
            and min(p[1] for p in points)<=b['lat_max'] and max(p[1] for p in points)>=b['lat_min'])


def timing(properties, now):
    duration=properties.get('duration') or {}
    start,end=timestamp(duration.get('start')),timestamp(duration.get('end'))
    if end and end<now:
        return 'past_end'
    if start and start>now:
        return 'scheduled'
    return 'date_window' if start else 'unknown'


class TrafficFeed:
    def __init__(self, directory=None, key=None):
        self.path=Path(directory or config.BASE_DIR/'.local'/'traffic')/'latest.json'
        self.key=config.QLD_TRAFFIC_API_KEY if key is None else key
        self.state=read_json(self.path,{})
        self.lock=threading.Lock()

    def refresh(self, now):
        due=timestamp(self.state.get('next_attempt_at'))
        if due and now<due:
            return
        self.state['last_attempt_at']=now.isoformat()
        self.state['next_attempt_at']=(now+timedelta(seconds=INTERVAL)).isoformat()
        try:
            if not self.key:
                raise ValueError('QLD Traffic key is not configured.')
            response=requests.get(URL,params={'apikey':self.key},timeout=(8,20))
            if response.status_code==429:
                raise ValueError('QLDTraffic shared quota is busy (HTTP 429). Retrying after the five-minute cooldown.')
            if response.status_code in [401,403]:
                raise ValueError('QLDTraffic rejected the configured key.')
            response.raise_for_status()
            feed=response.json()
            if not isinstance(feed,dict) or feed.get('type')!='FeatureCollection' or not isinstance(feed.get('features'),list):
                raise ValueError('QLDTraffic returned an unexpected feed structure.')
            if any(not isinstance(f,dict) or f.get('type')!='Feature' or not isinstance(f.get('properties'),dict) for f in feed['features']):
                raise ValueError('QLDTraffic returned an invalid event record.')
            self.state.update(feed=feed,retrieved_at=now.isoformat(),error=None)
        except requests.RequestException:
            self.state['error']='QLDTraffic could not be reached. Any retained events are from the previous successful response.'
        except ValueError as exc:
            # These messages are either local schema errors or JSON decoder errors, never request URLs.
            self.state['error']=str(exc) if not isinstance(exc,requests.exceptions.JSONDecodeError) else 'QLDTraffic returned invalid JSON.'
        try:
            write_json(self.path,self.state)
        except OSError:
            self.state['error']='Traffic cache could not be saved; retained data may be stale.'

    def payload(self, now=None, refresh=True):
        now=now or datetime.now(timezone.utc)
        with self.lock:
            if refresh:
                self.refresh(now)
            feed=self.state.get('feed') or {}
            retrieved=timestamp(self.state.get('retrieved_at'))
            age=max(0,(now-retrieved).total_seconds()) if retrieved else None
            stale=bool(self.state.get('error')) or age is None or age>INTERVAL*2
            features=[]
            unlocated=0
            for feature in feed.get('features',[]):
                if not positions(feature.get('geometry')):
                    unlocated+=1
                    continue
                if metro_feature(feature):
                    properties=feature['properties']
                    features.append({**feature,'properties':{**properties,'_timing':timing(properties,now)}})
            return {'type':'FeatureCollection','features':features,
                    'status':'unavailable' if retrieved is None else 'stale' if stale else 'current',
                    'stale':stale,'retrieved_at':self.state.get('retrieved_at'),'age_seconds':age,
                    'published':feed.get('published'),'error':self.state.get('error'),
                    'next_attempt_at':self.state.get('next_attempt_at'),'refresh_seconds':INTERVAL,
                    'total_events':len(feed.get('features',[])),'metro_events':len(features),'unlocated_events':unlocated,
                    'counts':dict(Counter(f['properties'].get('event_type','Unknown') for f in features)),
                    'rights':feed.get('rights'), 'source':SOURCE,
                    'attribution':'State of Queensland (Department of Transport and Main Roads) and attributed feed providers · CC BY 4.0',
                    'scope':'Events whose geometry bounds overlap the Brisbane analysis area. Includes scheduled works and area alerts. Date windows do not confirm active hours.',
                    'routing_note':'QLDTraffic incidents do not change openrouteservice routes, estimated travel times or fuel costs. No reported event does not establish that a road is clear.'}

    def raw(self):
        with self.lock:
            return {'retrieved_at':self.state.get('retrieved_at'),'error':self.state.get('error'),'feed':self.state.get('feed')}
