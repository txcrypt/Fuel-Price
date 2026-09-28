"""On-demand road routes. Coordinates and cache remain in memory only."""
from collections import OrderedDict, deque
from datetime import datetime
import hashlib
import json
import math
import threading
import time
from typing import Annotated, Literal
import requests
from pydantic import BaseModel, ConfigDict, Field
import config

# The buying planner charges extra travel to the deferred plan only. A route can
# therefore be adopted in exactly two honest ways.
LEGS = {
    'later_refill': 'Only the deferred refill drives this trip. Filling now is assumed to need no extra driving.',
    'both_purchases': 'Both options drive this trip, so it cancels out of the comparison and adds nothing.',
}
# Buying-planner input limits and steps; adopted values must land inside them.
PLAN_MAX_KM = PLAN_MAX_MINUTES = 300
PLAN_MAX_HOURLY = 200
MAX_CANDIDATES = 8


class Point(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)

    def coordinates(self):
        return [self.longitude, self.latitude]


class TripInputs(BaseModel):
    """Everything that defines the journey and the purchase being priced."""
    model_config = ConfigDict(allow_inf_nan=False)
    origin: Point
    destination: Point | None = None
    avoid_tolls: bool = True
    litres: float = Field(default=40, gt=0, le=200)
    consumption: float = Field(default=8, gt=0, le=40)
    hourly_value: float = Field(default=0, ge=0, le=1000)


class RouteRequest(TripInputs):
    station_id: str = Field(min_length=1, max_length=40)
    leg: Literal['later_refill', 'both_purchases'] = 'later_refill'


class RankRequest(TripInputs):
    station_ids: list[Annotated[str, Field(min_length=1, max_length=40)]] | None = Field(default=None, max_length=MAX_CANDIDATES)
    radius_km: float = Field(default=8, gt=0, le=50)
    limit: int = Field(default=5, ge=2, le=MAX_CANDIDATES)


class RoutingError(Exception):
    def __init__(self, message, status=502):
        super().__init__(message)
        self.status = status


def haversine(a, b):
    lat1, lat2 = math.radians(a[1]), math.radians(b[1])
    dlat, dlon = lat2-lat1, math.radians(b[0]-a[0])
    h = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return 6371*2*math.asin(min(1, math.sqrt(h)))


def adoption(request, result):
    """Describe the extra trip in buying-planner terms: physical quantities only.

    Deliberately carries no dollar figure. routing.py values the detour at the
    station price; buying_plan.py values it again from these kilometres and
    minutes. Passing a cost across as well would charge the same detour twice.
    """
    cancels = request.leg == 'both_purchases'
    km = 0.0 if cancels else round(result['extra_km'], 1)
    minutes = 0.0 if cancels else float(round(result['extra_minutes']))
    dedicated = result['mode'] == 'dedicated_return_trip'
    blocked, notes = [], []
    if not cancels and (result['extra_km'] < 0 or result['extra_minutes'] < 0):
        blocked.append('Routing via this station is no longer than the direct journey, so there is no extra trip to charge. '
                       'The buying planner cannot price a negative detour.')
    if km > PLAN_MAX_KM or minutes > PLAN_MAX_MINUTES:
        blocked.append(f'The extra trip exceeds the buying planner limits of {PLAN_MAX_KM} km and {PLAN_MAX_MINUTES} minutes.')
    if not cancels and (km, minutes) != (round(result['extra_km'], 3), round(result['extra_minutes'], 2)):
        notes.append('Rounded to the buying planner input steps of 0.1 km and 1 minute.')
    hourly = request.hourly_value
    if hourly > PLAN_MAX_HOURLY:
        hourly = None
        notes.append(f'A time value above ${PLAN_MAX_HOURLY}/hour cannot be carried into the buying planner; '
                     'set it there by hand or lower it here.')
    name = (result['station_name'] or result['station_id'])[:40]
    trip = ('Dedicated return trip made only to buy fuel: the whole trip is extra driving.' if dedicated
            else 'Fuel stop on a journey you were making anyway: only the detour past the station is extra driving.')
    basis = (f'Road route to {name}: no extra driving, both options drive it.' if cancels
             else f'Road route to {name}: {km:g} km / {minutes:g} min extra, charged to the later refill '
                  f'({"dedicated trip" if dedicated else "detour on a planned journey"}).')
    return {'leg': request.leg, 'applies_to': LEGS[request.leg], 'trip': trip,
            'extra_km': km, 'extra_minutes': minutes,
            'consumption': request.consumption, 'hourly_value': hourly, 'station_id': request.station_id, 'station_name': result['station_name'],
            'adoptable': not blocked, 'blocked': blocked, 'notes': notes,
            'basis': basis[:160],
            'fingerprint': fingerprint(request, result['price_cpl'])}


def fingerprint(request, price):
    """Identity of the priced journey. Any change here invalidates an adopted cost."""
    raw = json.dumps([request.station_id, price, request.origin.coordinates(),
                      request.destination.coordinates() if request.destination else None,
                      request.litres, request.consumption, request.hourly_value,
                      request.avoid_tolls, request.leg], sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


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
        result={'station_id':request.station_id,'station_name':station.get('name'), 'price_cpl':price,
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
        result['adoption']=adoption(request,result)
        return result

    def rank(self, request, dashboard):
        """Rank a bounded shortlist on total trip cost using one identical journey.

        Every candidate reuses compare(), so the money arithmetic is not duplicated.
        The origin->destination baseline leg is one cached provider call shared by all.
        """
        if not self.key:
            raise RoutingError('Add ORS_API_KEY to the local .env configuration and restart.',503)
        stations=[s for s in dashboard['stations']
                  if isinstance(s.get('price_cpl'),(int,float)) and math.isfinite(s['price_cpl'])
                  and all(isinstance(s.get(k),(int,float)) and math.isfinite(s[k]) for k in ('latitude','longitude'))]
        unknown=[]
        if request.station_ids:
            wanted=list(dict.fromkeys(request.station_ids))
            known={str(s['site_id']):s for s in stations}
            pool=[known[i] for i in wanted if i in known]
            unknown=[i for i in wanted if i not in known]
            rule=f'The {len(pool)} stations you chose by hand.'
        else:
            origin=request.origin.coordinates()
            near=[s for s in stations if haversine(origin,[s['longitude'],s['latitude']])<=request.radius_km]
            pool=sorted(near,key=lambda s:(s['price_cpl'],str(s['site_id'])))[:request.limit]
            rule=(f'The {len(pool)} lowest pump prices within {request.radius_km:g} km straight-line of the start, '
                  f'chosen from {len(near)} stations inside that radius and {len(stations)} priced stations in the snapshot.')
        pool=pool[:MAX_CANDIDATES]
        if not pool:
            raise RoutingError('No priced station matched that shortlist. Widen the radius or choose stations by hand.',422)
        results,failures,stopped=[],[],None
        for s in pool:
            try:
                results.append(self.compare(RouteRequest(station_id=str(s['site_id']),leg='later_refill',
                                                         **request.model_dump(exclude={'station_ids','radius_km','limit'})),
                                            dashboard))
            except RoutingError as exc:
                failures.append({'station_id':str(s['site_id']),'name':s.get('name'),'status':exc.status,'reason':str(exc)})
                if exc.status in (429,503):
                    # Quota or key failure: stop instead of burning the remaining allowance.
                    stopped=str(exc)
                    break
        for site in unknown:
            failures.append({'station_id':site,'name':None,'status':422,
                             'reason':'Not in the current priced U91 snapshot; it was not evaluated.'})
        if not results:
            raise RoutingError(stopped or 'No candidate station could be routed. Check the start point and try again.',
                               429 if stopped else 502)
        by_pump=sorted(results,key=lambda r:(r['price_cpl'],r['station_id']))
        by_total=sorted(results,key=lambda r:(r['cost_including_travel'],r['station_id']))
        for position,row in enumerate(by_pump,1):
            row['pump_rank']=position
        for position,row in enumerate(by_total,1):
            row['total_rank']=position
        best=by_total[0]
        for row in results:
            if row is not best:  # One drawable route; the rest keep their distances without the geometry payload.
                for leg in ('via','direct'):
                    if row.get(leg):
                        row[leg]={k:v for k,v in row[leg].items() if k!='geometry'}
        baselines={None if r['direct'] is None else round(r['direct']['km'],3) for r in results}
        return {'evaluated':len(results),'requested':len(pool)+len(unknown),'partial':bool(failures),
                'failures':failures,'quota_stopped':stopped,'shortlist_rule':rule,
                'max_candidates':MAX_CANDIDATES,'litres':request.litres,'consumption':request.consumption,
                'hourly_value':request.hourly_value,'avoid_tolls':request.avoid_tolls,
                'mode':best['mode'],'fresh_prices':dashboard['health']['fresh'],
                'headline':f'Cheapest of the {len(results)} stations evaluated — not the cheapest in Brisbane.',
                'ranks_disagree':by_pump[0]['station_id']!=best['station_id'],
                'best_pump':by_pump[0]['station_id'],'best_total':best['station_id'],
                'baseline':None if best['direct'] is None else {'km':round(best['direct']['km'],3),'minutes':round(best['direct']['minutes'],2)},
                'baseline_consistent':len(baselines)==1,
                'results':by_total,
                'note':('Only the listed shortlist was evaluated, so this is not a city-wide optimum: a cheaper total may exist at a '
                        'station that was never routed. Every candidate uses the same start, destination, purchase volume, consumption '
                        'and time value. Driving times are estimates, not live traffic. Toll charges and queue or stop time are not priced.'),
                'attribution':'Routing © openrouteservice / HeiGIT; map data © OpenStreetMap contributors'}
