"""Public QLDTraffic events; bounded request cadence independent of fuel collection."""
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import Counter
from typing import Literal
import math
import threading
import requests
from pydantic import BaseModel, ConfigDict, Field
import config
from api_evidence import read_json, write_json

SOURCE = 'https://qldtraffic.qld.gov.au/more/Developers-and-Data/'
URL = 'https://api.qldtraffic.qld.gov.au/v2/events'
INTERVAL = 300
MAX_BACKOFF = 3600
# ~24 hours of five-minute attempts. Oldest entries are dropped; this is not an archive.
HISTORY_LIMIT = 288
OUTCOMES = {'ok':'Feed returned','rate_limited':'Rate limited (HTTP 429)','rejected':'Key rejected',
            'http_error':'Upstream HTTP error','unreachable':'Host unreachable',
            'bad_payload':'Unusable response','not_configured':'No key configured'}

# Route-corridor matching tolerances, in metres.
# ponytail: flat local projection and a naive O(route x event) scan behind a bounding-box
# prefilter. A city route against ~60 metro events runs in milliseconds; if that stops being
# true, index the route segments into a grid rather than reaching for a geometry dependency.
CORRIDOR_M = 40     # nominal carriageway half-width plus feed geometry error
NEARBY_M = 250      # past this an event is not reported against the route at all
MAX_ROUTE_POINTS = 6000

COST_NOTE = ('These events do not change the route, the estimated travel time or the fuel cost. '
             'Nothing here is added to or subtracted from those figures.')
DELAY_NOTE = ('Any delay wording below is the provider\'s own text, reproduced exactly. It is not a measured delay '
              'and no number of minutes is derived from it.')
DIRECTION_NOTE = ('Matching is geometric only. It does not establish that an event affects your direction of travel, '
                  'your carriageway, or the far side of a divided road. Read each event\'s own stated direction.')
AREA_NOTE = ('Area alerts describe a region, not a specific road. An area alert whose polygon contains your route '
             'is not a reported closure on that route.')
NEARBY_NOTE = (f'Events more than {CORRIDOR_M} m but within {NEARBY_M} m of the route line are listed separately as nearby. '
               'A nearby event may be on a parallel road, a service road, a bridge over or under your route, or a different '
               'street entirely. It is not established to be on your route.')
CROSSING_NOTE = ('A map line that touches yours may belong to an intersecting street, an overpass or an underpass rather '
                 'than to the road you are driving. Check each event\'s road name before treating it as yours.')
NO_MATCH_NOTE = ('No reported event matched this corridor. That does not establish that the road is clear: QLDTraffic lists '
                 'reported events only, the feed can be incomplete or stale, and this match is geometric.')
SCOPE_NOTE = ('Only events overlapping the Brisbane analysis area are considered.')
STALE_NOTE = ('The retained feed is stale or unavailable, so this corridor check may be missing current events entirely.')
SCHEDULE = {'date_window':'Inside its stated date range. Active hours are not confirmed, so it may or may not be in force right now.',
            'scheduled':'Starts after the current time; it is not in force now.',
            'past_end':'Past its stated end.',
            'unknown':'No schedule was supplied.'}
RECURRENCE_NOTE = (' A recurrence or active-day pattern is stated. Membership of the date window does not confirm that the '
                   'restriction applies at this moment.')


class RouteCorridor(BaseModel):
    """A driving route line to test events against. Never stored; used for one response."""
    model_config = ConfigDict(allow_inf_nan=False)
    type: Literal['LineString'] = 'LineString'
    coordinates: list[tuple[float, float]] = Field(min_length=2, max_length=MAX_ROUTE_POINTS)


def retry_after(response):
    """Provider-supplied delay, only ever used to wait LONGER than the fixed cooldown."""
    try:
        seconds = int(float(response.headers.get('Retry-After', '')))
    except (AttributeError, TypeError, ValueError):
        return INTERVAL
    return min(max(seconds, INTERVAL), MAX_BACKOFF)


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
        # Tuples as well as lists: a validated RouteCorridor arrives as tuples of floats.
        if not isinstance(values, (list, tuple)) or not values:
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


def shapes(geometry):
    """Structured parts of a geometry: ('point'|'line'|'ring', positions).

    positions() flattens everything into one cloud, which cannot tell a road line from an
    area-alert polygon. Corridor matching needs exactly that difference.
    """
    if not isinstance(geometry, dict):
        return []
    kind = geometry.get('type')
    if kind == 'GeometryCollection':
        children = geometry.get('geometries')
        return [s for g in children for s in shapes(g)] if isinstance(children, list) else []
    coordinates = geometry.get('coordinates')
    if not isinstance(coordinates, list):
        return []

    def part(role, values):
        # positions() re-validates finiteness and lat/lng range for every vertex.
        points = positions({'type': 'LineString', 'coordinates': values})
        return [(role, points)] if len(points) >= (1 if role == 'point' else 2) else []
    if kind == 'Point':
        return part('point', [coordinates])
    if kind == 'LineString':
        return part('line', coordinates)
    if kind == 'MultiPoint':
        return [s for c in coordinates for s in part('point', [c])]
    if kind == 'MultiLineString':
        return [s for c in coordinates for s in part('line', c)]
    # ponytail: polygon holes are treated as ordinary rings, so an alert with a hole
    # over-matches rather than under-matches. No feed record has one.
    if kind == 'Polygon':
        return [s for c in coordinates for s in part('ring', c)]
    if kind == 'MultiPolygon':
        return [s for polygon in coordinates if isinstance(polygon, list) for ring in polygon for s in part('ring', ring)]
    return []


def project(points, reference):
    """Local equirectangular metres. Sub-metre over a city; never used for long distances."""
    kx = 111320 * math.cos(math.radians(reference))
    return [(p[0]*kx, p[1]*110540) for p in points]


def point_segment(p, a, b):
    dx, dy = b[0]-a[0], b[1]-a[1]
    if dx == 0 and dy == 0:
        return math.dist(p, a)
    t = max(0.0, min(1.0, ((p[0]-a[0])*dx + (p[1]-a[1])*dy)/(dx*dx + dy*dy)))
    return math.dist(p, (a[0]+t*dx, a[1]+t*dy))


def segments_cross(a, b, c, d):
    """Proper crossing. Two long roads can intersect with every endpoint far apart."""
    def side(p, q, r):
        return (q[0]-p[0])*(r[1]-p[1]) - (q[1]-p[1])*(r[0]-p[0])
    d1, d2, d3, d4 = side(c, d, a), side(c, d, b), side(a, b, c), side(a, b, d)
    return (d1 > 0) != (d2 > 0) and (d3 > 0) != (d4 > 0)


def segment_distance(a, b, c, d):
    if segments_cross(a, b, c, d):
        return 0.0
    return min(point_segment(a, c, d), point_segment(b, c, d), point_segment(c, a, b), point_segment(d, a, b))


def inside_ring(point, ring):
    """Even-odd ray cast. Containment by an area alert is still not a closure on the road."""
    x, y = point
    result = False
    for (x1, y1), (x2, y2) in zip(ring, ring[1:]+ring[:1]):
        if (y1 > y) != (y2 > y) and x < x1 + (y-y1)/(y2-y1)*(x2-x1):
            result = not result
    return result


def corridor_distance(route, geometry, limit=NEARBY_M):
    """(metres, encloses) for an event geometry against a route line, or None if too far.

    Exact geometry, not bounding-box overlap: it reports how close the event actually comes
    to the driven line. It says where, never which carriageway, which direction of travel,
    or whether the event is in force now.
    """
    parts = shapes(geometry)
    if len(route) < 2 or not parts:
        return None
    reference = sum(p[1] for p in route)/len(route)
    line = project(route, reference)
    segments = list(zip(line, line[1:]))
    x0, x1 = min(p[0] for p in line), max(p[0] for p in line)
    y0, y1 = min(p[1] for p in line), max(p[1] for p in line)
    best, encloses = None, False
    for role, points in parts:
        flat = project(points, reference)
        if (min(p[0] for p in flat) > x1+limit or max(p[0] for p in flat) < x0-limit
                or min(p[1] for p in flat) > y1+limit or max(p[1] for p in flat) < y0-limit):
            continue
        if role == 'ring' and len(flat) >= 3 and any(inside_ring(p, flat) for p in line):
            encloses = True
        if role == 'point':
            distance = min(point_segment(flat[0], a, b) for a, b in segments)
        else:
            distance = min(segment_distance(c, d, a, b) for c, d in zip(flat, flat[1:]) for a, b in segments)
        best = distance if best is None else min(best, distance)
    if best is None:
        return (0.0, True) if encloses else None
    return best, encloses


def disruption(feature, metres, encloses, corridor_m=CORRIDOR_M):
    properties = feature['properties']
    impact = properties.get('impact') or {}
    road = properties.get('road_summary') or {}
    duration = properties.get('duration') or {}
    recurring = bool(duration.get('recurrences') or duration.get('active_days'))
    # The feed flags its own area alerts; a polygon enclosing the route is treated as one too.
    area = bool(properties.get('area_alert')) or (encloses and metres > corridor_m)
    schedule = properties.get('_timing') if properties.get('_timing') in SCHEDULE else 'unknown'
    return {'id': properties.get('id'), 'event_type': properties.get('event_type'),
            'event_subtype': properties.get('event_subtype'), 'road_name': road.get('road_name'),
            'locality': road.get('locality'), 'impact_type': impact.get('impact_type'),
            'impact_subtype': impact.get('impact_subtype'), 'direction': impact.get('direction'),
            # Verbatim provider wording. Nothing downstream may turn it into minutes.
            'delay_text': impact.get('delay'), 'priority': properties.get('event_priority'),
            'description': properties.get('description'), 'advice': properties.get('advice'),
            'last_updated': properties.get('last_updated'), 'url': properties.get('url'),
            'timing': schedule, 'recurring': recurring,
            'schedule_note': SCHEDULE[schedule] + (RECURRENCE_NOTE if recurring else ''),
            'area_alert': area,
            'match': 'area_alert' if area else 'on_corridor' if metres <= corridor_m else 'nearby',
            'metres': round(metres)}


def route_disruptions(features, route, corridor_m=CORRIDOR_M, nearby_m=NEARBY_M):
    """Group feed events by how their geometry relates to one route line."""
    matched = []
    for feature in features:
        found = corridor_distance(route, feature.get('geometry'), nearby_m)
        if found is None:
            continue
        metres, encloses = found
        if metres > nearby_m and not encloses:
            continue
        matched.append(disruption(feature, metres, encloses, corridor_m))
    matched.sort(key=lambda e: ({'on_corridor': 0, 'area_alert': 1, 'nearby': 2}[e['match']], e['metres']))
    # A past-end event is not a current disruption; it is counted, not listed as one.
    events = [e for e in matched if e['timing'] != 'past_end']
    expired = [e for e in matched if e['timing'] == 'past_end']
    counts = {name: sum(1 for e in events if e['match'] == name) for name in ('on_corridor', 'area_alert', 'nearby')}
    counts['scheduled'] = sum(1 for e in events if e['timing'] == 'scheduled')
    counts['expired'] = len(expired)
    return {'events': events, 'expired': expired, 'counts': counts}


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
        outcome,http='unreachable',None
        try:
            if not self.key:
                outcome='not_configured'
                raise ValueError('QLD Traffic key is not configured.')
            response=requests.get(URL,params={'apikey':self.key},timeout=(8,20))
            http=response.status_code
            if response.status_code==429:
                # Honour Retry-After when the provider sends one; never retry sooner than INTERVAL.
                wait=retry_after(response)
                self.state['next_attempt_at']=(now+timedelta(seconds=wait)).isoformat()
                outcome='rate_limited'
                raise ValueError(f'QLDTraffic shared quota is busy (HTTP 429). Retrying after a {round(wait/60)}-minute cooldown.')
            if response.status_code in [401,403]:
                outcome='rejected'
                raise ValueError('QLDTraffic rejected the configured key.')
            outcome='http_error'
            response.raise_for_status()
            outcome='bad_payload'
            feed=response.json()
            if not isinstance(feed,dict) or feed.get('type')!='FeatureCollection' or not isinstance(feed.get('features'),list):
                raise ValueError('QLDTraffic returned an unexpected feed structure.')
            if any(not isinstance(f,dict) or f.get('type')!='Feature' or not isinstance(f.get('properties'),dict) for f in feed['features']):
                raise ValueError('QLDTraffic returned an invalid event record.')
            self.state.update(feed=feed,retrieved_at=now.isoformat(),error=None)
            outcome='ok'
        except requests.exceptions.JSONDecodeError:
            self.state['error']='QLDTraffic returned invalid JSON.'
        except requests.RequestException:
            self.state['error']='QLDTraffic could not be reached. Any retained events are from the previous successful response.'
        except ValueError as exc:
            # These messages are local schema errors raised above, never provider text or request URLs.
            self.state['error']=str(exc)
        self.record(now,outcome,http)
        try:
            write_json(self.path,self.state)
        except OSError:
            self.state['error']='Traffic cache could not be saved; retained data may be stale.'

    def record(self, now, outcome, http):
        """Bounded log of fetch attempts only. Nothing here describes road conditions."""
        retrieved=timestamp(self.state.get('retrieved_at'))
        history=self.state.get('availability')
        history=list(history) if isinstance(history,list) else []
        history.append({'at':now.isoformat(),'outcome':outcome,'http':http,
                        'age_seconds':round(max(0,(now-retrieved).total_seconds())) if retrieved else None})
        self.state['availability']=history[-HISTORY_LIMIT:]

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
                    'impact_counts':dict(Counter((f['properties'].get('impact') or {}).get('impact_type') or 'Not stated' for f in features)),
                    'availability':self.state.get('availability') or [],'availability_limit':HISTORY_LIMIT,
                    'availability_outcomes':OUTCOMES,
                    'availability_note':'Upstream fetch attempts only, newest last, oldest dropped past the limit. This records whether the feed answered, never whether roads were clear: a gap is a period nobody asked, not a period without incidents.',
                    'rights':feed.get('rights'), 'source':SOURCE,
                    'attribution':'State of Queensland (Department of Transport and Main Roads) and attributed feed providers · CC BY 4.0',
                    'scope':'Events whose geometry bounds overlap the Brisbane analysis area. Includes scheduled works and area alerts. Date windows do not confirm active hours.',
                    'routing_note':'QLDTraffic incidents do not change openrouteservice routes, estimated travel times or fuel costs. No reported event does not establish that a road is clear.'}

    def disruptions(self, geometry, now=None):
        """Reported events whose geometry touches one route corridor.

        Information for the reader only. Nothing here changes a route, an estimated travel
        time or a fuel cost, and an empty result is never an assurance that a road is clear.
        """
        now = now or datetime.now(timezone.utc)
        feed = self.payload(now)  # honours the existing cooldown; adds no upstream request
        route = positions(geometry) if isinstance(geometry, dict) and geometry.get('type') == 'LineString' else []
        result = route_disruptions(feed['features'], route)
        bounds = config.BOUNDS
        outside = any(not (bounds['lng_min'] <= p[0] <= bounds['lng_max']
                           and bounds['lat_min'] <= p[1] <= bounds['lat_max']) for p in route)
        notes = [COST_NOTE, DIRECTION_NOTE, DELAY_NOTE]
        if result['counts']['on_corridor']:
            notes.append(CROSSING_NOTE)
        if result['counts']['area_alert']:
            notes.append(AREA_NOTE)
        if result['counts']['nearby']:
            notes.append(NEARBY_NOTE)
        if not result['events']:
            notes.append(NO_MATCH_NOTE)
        notes.append(SCOPE_NOTE + (' Part of this route lies outside it and was not checked there.' if outside else ''))
        if feed['stale'] or feed['status'] == 'unavailable':
            notes.append(STALE_NOTE)
        on = result['counts']['on_corridor']
        later = sum(1 for e in result['events'] if e['match'] == 'on_corridor' and e['timing'] == 'scheduled')
        return {**result, 'usable': len(route) >= 2, 'route_points': len(route),
                'route_outside_area': outside, 'corridor_m': CORRIDOR_M, 'nearby_m': NEARBY_M,
                'headline': (f'{on} reported event{"" if on == 1 else "s"} '
                             f'{"touches" if on == 1 else "touch"} this route line within {CORRIDOR_M} m'
                             + (f'; {later} of them scheduled to start later.' if later else '.')
                             if on else 'No reported event touches this route line.'),
                'feed': {'status': feed['status'], 'stale': feed['stale'], 'error': feed['error'],
                         'retrieved_at': feed['retrieved_at'], 'age_seconds': feed['age_seconds'],
                         'published': feed['published'], 'metro_events': feed['metro_events']},
                'notes': notes, 'source': SOURCE, 'attribution': feed['attribution']}

    def raw(self):
        with self.lock:
            return {'retrieved_at':self.state.get('retrieved_at'),'error':self.state.get('error'),'feed':self.state.get('feed')}
