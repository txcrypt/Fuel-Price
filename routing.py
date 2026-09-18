"""On-demand road routes. Coordinates and cache remain in memory only."""
from collections import OrderedDict, deque
from datetime import datetime
import math
import threading
import time
import requests
from pydantic import BaseModel, ConfigDict, Field
import config


class Point(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)

    def coordinates(self):
        return [self.longitude, self.latitude]


class RouteRequest(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    station_id: str = Field(min_length=1, max_length=40)
    origin: Point
    destination: Point | None = None
    avoid_tolls: bool = True
    litres: float = Field(default=40, gt=0, le=200)
    consumption: float = Field(default=8, gt=0, le=40)
    hourly_value: float = Field(default=0, ge=0, le=1000)


class RoutingError(Exception):
    def __init__(self, message, status=502):
        super().__init__(message)
        self.status = status


def haversine(a, b):
    lat1, lat2 = math.radians(a[1]), math.radians(b[1])
    dlat, dlon = lat2-lat1, math.radians(b[0]-a[0])
    h = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return 6371*2*math.asin(min(1, math.sqrt(h)))


class RoadRouter:
    url = 'https://api.openrouteservice.org/v2/directions/driving-car/geojson'

    def __init__(self, key=None):
        self.key = config.ORS_API_KEY if key is None else key
        self.cache = OrderedDict()
        self.calls = deque()
        self.lock = threading.Lock()
        self.last_success = None
        self.last_error = None

    def status(self):
        return {'configured':bool(self.key), 'last_success':self.last_success, 'error':self.last_error,
                'provider':'openrouteservice', 'cache_seconds':300,
                'coordinates':'Sent to openrouteservice only when Calculate road cost is selected; not saved to disk.'}

    def _directions(self, coordinates, avoid_tolls):
        key = (tuple(tuple(p) for p in coordinates), avoid_tolls)
        with self.lock:
            now = time.monotonic()
            cached = self.cache.get(key)
            if cached and now-cached[0] < 300:
                self.cache.move_to_end(key)
                return {**cached[1], 'cached':True}
            while self.calls and now-self.calls[0] > 60:
                self.calls.popleft()
            if len(self.calls)>=30:
                raise RoutingError('Local routing rate limit reached. Try again in one minute.',429)
            self.calls.append(now)
            body={'coordinates':coordinates, 'instructions':False, 'units':'m', 'preference':'recommended',
                  'radiuses':[100]*len(coordinates)}
            if avoid_tolls:
                body['options']={'avoid_features':['tollways']}
            try:
                response=requests.post(self.url, headers={'Authorization':self.key},json=body,timeout=(10,30))
                if response.status_code in [401,403]:
                    raise RoutingError('openrouteservice rejected the key or its permissions.',503)
                if response.status_code==429:
                    raise RoutingError('openrouteservice quota reached. Try again later.',429)
                if response.status_code in [400,404]:
                    raise RoutingError('No driving route found. Place the start and destination on accessible roads.',422)
                response.raise_for_status()
                feature=response.json()['features'][0]
                summary=feature['properties']['summary']
                distance, duration=float(summary['distance']), float(summary['duration'])
                geometry=feature['geometry']
                if not all(math.isfinite(v) and v>=0 for v in [distance,duration]) or geometry['type']!='LineString':
                    raise ValueError('Invalid route')
                result={'km':distance/1000,'minutes':duration/60,'geometry':geometry,
                        'retrieved_at':datetime.now().isoformat(timespec='seconds'),'cached':False}
            except RoutingError:
                raise
            except (requests.RequestException,ValueError,KeyError,IndexError,TypeError):
                # Never echo provider responses, request headers or coordinates into errors/logs.
                raise RoutingError('openrouteservice could not return a usable route. Try again later.') from None
            self.cache[key]=(now,result)
            while len(self.cache)>64:
                self.cache.popitem(last=False)
            self.last_success=result['retrieved_at']
            self.last_error=None
            return result

    def compare(self, request, dashboard):
        if not self.key:
            raise RoutingError('Add ORS_API_KEY to the local .env configuration and restart.',503)
        station=next((s for s in dashboard['stations'] if str(s['site_id'])==request.station_id),None)
        if station is None:
            raise RoutingError('Station is not in the current available U91 snapshot.',422)
        site=[station['longitude'],station['latitude']]
        origin=request.origin.coordinates()
        destination=request.destination.coordinates() if request.destination else origin
        if any(haversine(p,site)>250 for p in [origin,destination]):
            raise RoutingError('This local planner supports points within 250 km of the Brisbane station.',422)
        try:
            via=self._directions([origin,site,destination],request.avoid_tolls)
            direct=self._directions([origin,destination],request.avoid_tolls) if request.destination else None
        except RoutingError as exc:
            self.last_error=str(exc)
            raise
        extra_km=via['km']-(direct['km'] if direct else 0)
        extra_minutes=via['minutes']-(direct['minutes'] if direct else 0)
        price=station['price_cpl']
        fuel_cost=extra_km*request.consumption/100*price/100
        time_cost=extra_minutes*request.hourly_value/60
        purchase=request.litres*price/100
        return {'station_id':request.station_id,'station_name':station.get('name'), 'price_cpl':price,
                'mode':'trip_detour' if direct else 'dedicated_return_trip', 'via':via,'direct':direct,
                'extra_km':round(extra_km,3),'extra_minutes':round(extra_minutes,2),
                'extra_fuel_l':round(extra_km*request.consumption/100,3),
                'travel_fuel_cost':round(fuel_cost,2),'travel_time_cost':round(time_cost,2),
                'purchase_cost':round(purchase,2),'cost_including_travel':round(purchase+fuel_cost+time_cost,2),
                'effective_cpl':round((purchase+fuel_cost+time_cost)/request.litres*100,1),
                'litres':request.litres,'fresh_prices':dashboard['health']['fresh'],
                'avoid_tolls':request.avoid_tolls,
                'note':'Driving-time estimate, not live traffic. Stop/queue time and any toll charges are excluded. Travel fuel is valued at the selected station price. Compare stations using identical trip and purchase inputs.',
                'attribution':'Routing © openrouteservice / HeiGIT; map data © OpenStreetMap contributors'}
