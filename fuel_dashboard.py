"""Local-only Brisbane fuel dashboard. Run with start-local.ps1."""
import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import config
from local_data import LocalData
from buying_plan import BuyingProfile, compare_horizons
from observatory import Observatory
from fuel_engine import FuelEngine
from routing import RoadRouter, RouteRequest, RoutingError
from traffic import TrafficFeed

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
    app.state.refresh_task = None

    async def collect():
        await run_in_threadpool(refresh_and_build, app.state.data)
        while True:
            due = datetime.fromisoformat(app.state.data.status['next_refresh'])
            remaining = (due - datetime.now()).total_seconds()
            if remaining <= 0:
                await run_in_threadpool(refresh_and_build, app.state.data)
            else:
                await asyncio.sleep(min(15, remaining))

    worker = asyncio.create_task(collect())
    yield
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
        if origin and urlparse(origin).netloc != request.headers.get('host'):
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


@app.get('/api/market-context')
def market_context(request: Request):
    data = request.app.state.data
    return data.context.payload(data.dashboard())


@app.get('/api/routing/status')
def routing_status(request: Request):
    return request.app.state.router.status()


@app.get('/api/traffic')
def traffic(request: Request):
    return request.app.state.traffic.payload()


@app.get('/api/traffic/raw')
def traffic_raw(request: Request):
    return request.app.state.traffic.raw()


@app.post('/api/routing/compare')
def route_compare(profile: RouteRequest, request: Request):
    try:
        return request.app.state.router.compare(profile, request.app.state.data.dashboard())
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


@app.get('/api/observatory/rows')
def raw_rows(request: Request, source: str = Query('live', pattern='^(live|archive)$'),
             site_id: str = '', state: str = '', offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200)):
    return request.app.state.observatory.rows(source, site_id, state, offset, limit)


app.mount('/static', StaticFiles(directory=config.BASE_DIR / 'static'), name='static')
