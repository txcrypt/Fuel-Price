"""Local-only Brisbane fuel dashboard. Run with start-local.ps1."""
import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import config
from local_data import LocalData
from buying_plan import BuyingProfile, compare_horizons
from observatory import Observatory
from fuel_engine import FuelEngine
from routing import RoadRouter, RankRequest, RouteRequest, RoutingError
from traffic import TrafficFeed, RouteCorridor, cameras
import public_feeds
import area
from insights import Insights
from alerts import Alerts
from fuel_security import FuelSecurity
import ships
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def refresh_and_build(data):
    data.refresh()
    # Archive forward predictions even when no browser is open.
    try:
        data.dashboard()
    except Exception:
        # A forecast/rendering failure must not stop future collection attempts.
        logging.exception('Could not prepare dashboard after collection')
    if isinstance(data.engine, FuelEngine):
        try:
            data.context.refresh()
        except Exception:
            logging.exception('Could not refresh public context; price collection remains independent')


@asynccontextmanager
async def lifespan(app):
    app.state.data = LocalData()
    app.state.observatory = Observatory(app.state.data)
    app.state.router = RoadRouter()
    app.state.traffic = TrafficFeed()
    app.state.insights = Insights(app.state.data, app.state.observatory)
    app.state.alerts = Alerts(config.BASE_DIR)
    app.state.fuel_security = FuelSecurity(config.BASE_DIR / '.local' / 'fuel_security')
    app.state.ais = ships.Ais()
    app.state.refresh_task = None

    def build_all():
        refresh_and_build(app.state.data)
        try:
            app.state.insights.wide()   # warm the daily table so no reader waits ~2 s for it
        except Exception:
            logging.exception('Could not prepare the insights table; collection is unaffected')
        try:
            public_feeds.cached('qships', timedelta(minutes=30), ships.port_movements)   # ~8 s; never on a reader's request
        except Exception:
            logging.exception('Qships refresh failed; collection is unaffected')
        try:
            app.state.fuel_security.refresh()   # government/port files, at most twice a day
        except Exception:
            logging.exception('Fuel security refresh failed; collection is unaffected')
        try:
            sent = app.state.alerts.check(app.state.data.dashboard())
            if sent:
                logging.info('Phone alerts sent: %s', ', '.join(sent))
        except Exception:
            logging.exception('Phone alert check failed; collection is unaffected')

    async def collect():
        await run_in_threadpool(build_all)
        while True:
            due = datetime.fromisoformat(app.state.data.status['next_refresh'])
            remaining = (due - datetime.now()).total_seconds()
            if remaining <= 0:
                await run_in_threadpool(build_all)
            else:
                await asyncio.sleep(min(15, remaining))

    worker = asyncio.create_task(collect())
    # Live ship positions: one WebSocket to aisstream.io for the whole Queensland coast.
    ais_task = asyncio.create_task(app.state.ais.run_forever(config.AISSTREAM_API_KEY)) if config.AISSTREAM_API_KEY else None
    yield
    if ais_task:
        ais_task.cancel()
        with suppress(asyncio.CancelledError):
            await ais_task
    worker.cancel()
    with suppress(asyncio.CancelledError):
        await worker
    manual = app.state.refresh_task
    if manual:
        await manual


app = FastAPI(title='Brisbane Fuel / Local', version='5.0.0', lifespan=lifespan)


@app.middleware('http')
async def local_requests(request: Request, call_next):
    # Reject cross-origin mutation requests to the local collector.
    if request.method == 'POST':
        origin = request.headers.get('origin')
        # Behind an HTTPS proxy (tailscale serve, Cloudflare Tunnel) Host can be the local address while
        # the page's origin is the public name; the proxy reports that name in X-Forwarded-Host.
        hosts = {request.headers.get('host'), request.headers.get('x-forwarded-host')}
        if origin and urlparse(origin).netloc not in hosts:
            from fastapi.responses import JSONResponse
            return JSONResponse({'detail': 'Refresh must come from this local dashboard.'}, status_code=403)
    response = await call_next(request)
    response.headers['Cache-Control'] = 'no-store'
    return response


@app.get('/api/dashboard')
@app.get('/api/bootstrap', include_in_schema=False)
def dashboard(request: Request):
    return request.app.state.data.dashboard()


@app.get('/api/health')
@app.get('/api/data-health', include_in_schema=False)
def health(request: Request):
    data = request.app.state.data.dashboard()
    return {'ok': True, **data['health'], 'files': data['files']}


@app.get('/api/stations')
def stations(request: Request):
    return request.app.state.data.dashboard()['stations']


@app.get('/api/forecast')
def forecast(request: Request):
    return request.app.state.data.dashboard()['forecast']


@app.post('/api/refresh', status_code=202)
async def refresh(request: Request):
    data = request.app.state.data
    task = request.app.state.refresh_task
    if data.lock.locked() or (task and not task.done()):
        return {'accepted': False, 'message': 'A refresh is already running.'}
    request.app.state.refresh_task = asyncio.create_task(run_in_threadpool(refresh_and_build, data))
    return {'accepted': True, 'message': 'Refresh started.'}


@app.get('/api/source-evidence')
def source_evidence(request: Request):
    with request.app.state.data.data_lock:
        return request.app.state.data.api_evidence.payload()


@app.get('/api/source-evidence/summary')
def source_evidence_summary(request: Request):
    with request.app.state.data.data_lock:
        return request.app.state.data.api_evidence.summary()


@app.get('/api/source-evidence/site/{site_id}')
def source_evidence_site(site_id: str, request: Request):
    with request.app.state.data.data_lock:
        site = request.app.state.data.api_evidence.site(site_id)
    if site is None:
        raise HTTPException(404, 'No site with that id in the latest capture.')
    return site


@app.get('/api/market-context')
def market_context(request: Request):
    data = request.app.state.data
    return data.context.payload(data.dashboard())


@app.get('/api/routing/status')
def routing_status(request: Request):
    return request.app.state.router.status()


@app.get('/api/traffic')
def traffic(request: Request):
    # JSONResponse skips FastAPI's per-value encoder: 53 ms -> 9 ms on this ~0.8 MB payload.
    return JSONResponse(request.app.state.traffic.payload(display=True))


@app.post('/api/traffic/route')
def traffic_route(corridor: RouteCorridor, request: Request):
    """Events touching a route corridor. Reporting only: it never adjusts a route or a cost."""
    return request.app.state.traffic.disruptions(corridor.model_dump())


@app.get('/api/cameras')
def traffic_cameras():
    state = cameras()
    return {'cameras': state['items'] or [], 'retrieved_at': state['retrieved_at'], 'error': state['error'],
            'source': 'QLDTraffic / Department of Transport and Main Roads'}


@app.get('/api/news')
def energy_news():
    return {**public_feeds.news(), 'warnings': public_feeds.warnings()}


@app.get('/api/area')
def area_summary(request: Request, south: float = Query(ge=-90, le=90), west: float = Query(ge=-180, le=180),
                 north: float = Query(ge=-90, le=90), east: float = Query(ge=-180, le=180),
                 name: str = Query('', max_length=80), region: str = Query('', max_length=40)):
    """Fuel, roads, cameras, bushfires, local news and warnings for one place's bounding box."""
    # It triggers an outbound news search, so another website must not be able to fire it from the
    # user's browser. Browsers label cross-site requests; same-origin page loads pass.
    if request.headers.get('sec-fetch-site', 'same-origin') not in ('same-origin', 'none'):
        raise HTTPException(403, 'Area summaries are only served to this dashboard.')
    # Clamp to Australia rather than reject: a zoomed-out view or an offshore island is still a valid ask.
    south, north = max(-45.0, min(south, north)), min(-9.0, max(south, north))
    west, east = max(112.0, min(west, east)), min(155.0, max(west, east))
    if south > north or west > east:
        raise HTTPException(422, 'That area is outside Australia.')
    box = area.padded(south, west, north, east)
    data = request.app.state.data
    dashboard = data.dashboard()
    result = area.brief(box, dashboard['stations'], request.app.state.traffic.payload(display=True)['features'],
                        cameras()['items'] or [], public_feeds.bushfires()['features'])
    place = name.strip()
    return {**result, 'name': place, 'grades': data.api_evidence.grades(box),
            'reference': {'median': dashboard['summary']['median'], 'area': dashboard['summary']['area']},
            'news': public_feeds.local_news(place, region) if place else [],
            'warnings': [w for w in public_feeds.warnings()['items'] if place and place.lower() in w['title'].lower()],
            'note': 'Observed data for this box only. Station prices marked old or suspect are counted but never ranked. '
                    'News is filtered to public-interest topics and grouped across outlets; it is not verified here.'}


def _regions(request):
    """site_id -> city, from the latest full capture (cached until the next one)."""
    with request.app.state.data.data_lock:
        return {s['site_id']: s['city'] for s in request.app.state.data.api_evidence.payload()['sites']}


def _analysis_sites(request):
    b = config.ANALYSIS_BOUNDS
    return [str(s['site_id']) for s in request.app.state.data.dashboard()['stations']
            if s.get('latitude') is not None and b['lat_min'] <= s['latitude'] <= b['lat_max']
            and b['lng_min'] <= s['longitude'] <= b['lng_max']]


@app.get('/api/insights/station/{site_id}')
def insight_station(site_id: str, request: Request, days: int = Query(180, ge=14, le=700)):
    regions = _regions(request)
    city = regions.get(site_id)
    peers = [s for s, c in regions.items() if c == city] if city else []
    return {**request.app.state.insights.station(site_id, peers, days), 'city': city}


@app.get('/api/insights/watch')
def insight_watch(request: Request, ids: str = Query('', max_length=2000)):
    sites = [i for i in ids.split(',') if i.strip()][:50]
    names = {str(s['site_id']): (s.get('name'), s.get('flag')) for s in request.app.state.data.dashboard()['stations']}
    rows = request.app.state.insights.watch(sites, _regions(request))
    return {'stations': [{**r, 'name': names.get(r['site_id'], (None, None))[0], 'flag': names.get(r['site_id'], (None, None))[1]}
                         for r in rows]}


@app.get('/api/insights/timing')
def insight_timing(request: Request, city: str = Query('Brisbane', max_length=60)):
    sites = [s for s, c in _regions(request).items() if c == city]
    return {**request.app.state.insights.timing(sites), 'city': city}


@app.get('/api/insights/cities')
def insight_cities(request: Request):
    with request.app.state.data.data_lock:
        sites = request.app.state.data.api_evidence.payload()['sites']
    return {'cities': Insights.leaderboard(sites), 'note': 'Latest capture. U91 unless marked; cities with 5 or more priced stations.'}


@app.get('/api/insights/jumps')
def insight_jumps(request: Request):
    return {'jumps': request.app.state.insights.jumps(_analysis_sites(request)), 'area': config.ANALYSIS_AREA_NAME}


@app.get('/api/insights/replay')
def insight_replay(request: Request, start: str = Query(pattern=r'^\d{4}-\d{2}-\d{2}$')):
    return JSONResponse(request.app.state.insights.replay(_analysis_sites(request), start))


@app.get('/api/insights/month')
def insight_month(request: Request, month: str = Query(pattern=r'^\d{4}-(0[1-9]|1[0-2])$')):
    return JSONResponse(request.app.state.insights.month(_analysis_sites(request), month))


@app.get('/api/ev')
def ev_chargers(south: float = Query(ge=-45, le=-9), west: float = Query(ge=112, le=155),
                north: float = Query(ge=-45, le=-9), east: float = Query(ge=112, le=155)):
    if north - south > 3 or east - west > 3:
        return {'status': 'zoom_in', 'chargers': [], 'note': 'Zoom in to load EV chargers.'}
    return public_feeds.ev_chargers(south, west, north, east, config.OCM_API_KEY)


class AlertSettings(BaseModel):
    topic: str = Field('', max_length=64)
    jump: bool = True
    watches: list[dict] = Field(default_factory=list, max_length=20)
    link: str = Field('', max_length=200)


@app.get('/api/alerts')
def alert_settings(request: Request):
    return request.app.state.alerts.settings()


@app.post('/api/alerts')
def save_alerts(settings: AlertSettings, request: Request):
    try:
        return request.app.state.alerts.save(settings.topic, settings.jump, settings.watches, settings.link)
    except (ValueError, KeyError, TypeError) as exc:
        raise HTTPException(422, str(exc) or 'Invalid alert settings.') from exc


@app.post('/api/alerts/test')
def test_alert(request: Request):
    alerts = request.app.state.alerts
    if not alerts.settings()['topic']:
        raise HTTPException(422, 'Save a topic first.')
    if not alerts.send('Fuel alerts are working', 'You will get a push when a price jump builds or a watched station hits your target.'):
        raise HTTPException(502, 'ntfy.sh could not be reached.')
    return {'sent': True}


@app.get('/api/fuel-security')
def fuel_security_summary(request: Request):
    return request.app.state.fuel_security.payload()


@app.get('/api/ships')
def live_ships(request: Request, everything: bool = False):
    if not config.AISSTREAM_API_KEY:
        return {'ships': [], 'counts': {}, 'connected': False,
                'error': 'Add a free aisstream.io key as AISSTREAM_API_KEY in .env, then restart, to show live ships.'}
    return JSONResponse(request.app.state.ais.payload(everything))


@app.get('/api/shipping')
def port_shipping():
    """Scheduled tanker arrivals and tankers at berth (Qships), refreshed in the background every 30 minutes."""
    result = public_feeds.cached('qships', timedelta(minutes=30), ships.port_movements)
    return result or {'arrivals': [], 'at_berth': [], 'counts': {}, 'error': 'Port schedules are not available yet.'}


@app.get('/api/bushfires')
def bushfire_incidents():
    return public_feeds.bushfires()


@app.get('/api/traffic/raw')
def traffic_raw(request: Request):
    return request.app.state.traffic.raw()


@app.post('/api/routing/compare')
def route_compare(profile: RouteRequest, request: Request):
    try:
        return request.app.state.router.compare(profile, request.app.state.data.dashboard())
    except RoutingError as exc:
        raise HTTPException(exc.status,str(exc)) from None


@app.post('/api/routing/rank')
def route_rank(profile: RankRequest, request: Request):
    try:
        return request.app.state.router.rank(profile, request.app.state.data.dashboard())
    except RoutingError as exc:
        raise HTTPException(exc.status,str(exc)) from None


@app.post('/api/buying-plan')
def buying_plan(profile: BuyingProfile, request: Request):
    try:
        return compare_horizons(profile, request.app.state.data.dashboard())
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.get('/api/export/{dataset}')
def export(dataset: str, request: Request):
    files = {'history': request.app.state.data.collection, 'snapshot': request.app.state.data.snapshot,
             'api': request.app.state.data.api_evidence.path}
    path = files.get(dataset)
    if path is None or not path.exists():
        raise HTTPException(404, 'Dataset not found.')
    return FileResponse(path, media_type='application/json' if dataset=='api' else 'text/csv', filename=path.name)


@app.get('/')
def root():
    return FileResponse(config.BASE_DIR / 'static' / 'observatory.html')


@app.get('/planner')
def planner():
    return FileResponse(config.BASE_DIR / 'static' / 'index.html')


@app.get('/api/observatory')
def observatory(request: Request):
    return request.app.state.observatory.overview()


@app.get('/api/observatory/station/{site_id}')
def station_evidence(site_id: str, request: Request):
    return request.app.state.observatory.station(site_id)


@app.get('/api/observatory/coverage')
def observatory_coverage(request: Request):
    return request.app.state.observatory.coverage()


@app.get('/api/observatory/rows')
def raw_rows(request: Request, source: str = Query('live', pattern='^(live|archive)$'),
             site_id: str = '', state: str = '', offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200)):
    return request.app.state.observatory.rows(source, site_id, state, offset, limit)


app.mount('/static', StaticFiles(directory=config.BASE_DIR / 'static'), name='static')
