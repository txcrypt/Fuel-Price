"""Ships off Queensland: live AIS positions (aisstream.io, free key) and scheduled port movements
(Maritime Safety Queensland Qships, keyless). Display only.

AIS: one WebSocket subscription covering the Queensland coast keeps the latest position and
identity of each vessel in memory. Identity (name, type, destination) arrives every few minutes,
separately from positions, so it is saved to disk and a restart still knows which ships are
tankers. A small standard-library WebSocket client avoids a new dependency.

Qships: the public ship-movement and at-berth grids behind https://qships.tmr.qld.gov.au/webx/,
fetched at most every 30 minutes.
"""
import asyncio
import base64
import json
import os
import re
import ssl
import struct
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import config
from api_evidence import read_json, write_json

# -- AIS -----------------------------------------------------------------------------------------
AIS_HOST, AIS_PATH = 'stream.aisstream.io', '/v0/stream'
QLD_BOXES = [[[-9.0, 141.0], [-28.5, 155.0]]]   # Torres Strait and Weipa to the Gold Coast, coast and reef
MSG_TYPES = ['PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport',
             'ShipStaticData', 'StaticDataReport']
STALE_AFTER = 3 * 3600          # a vessel not heard from in 3 hours is dropped
IDENTITY_FIELDS = ('name', 'type', 'imo', 'callsign', 'destination', 'draught_m', 'length_m', 'eta')
NAV_STATUS = {0: 'under way (engine)', 1: 'at anchor', 2: 'not under command', 3: 'restricted manoeuvrability',
              5: 'moored', 7: 'fishing', 8: 'under way (sailing)'}


def category(ship_type):
    t = ship_type or 0
    if 80 <= t <= 89:
        return 'tanker'
    if 70 <= t <= 79:
        return 'cargo'
    if 60 <= t <= 69:
        return 'passenger'
    if t in (31, 32, 52):
        return 'tug'
    if t == 30:
        return 'fishing'
    if t in (36, 37):
        return 'pleasure'
    return 'other' if t else 'unknown'


class Ais:
    def __init__(self, directory=None):
        self.path = Path(directory or config.BASE_DIR / '.local' / 'ships') / 'identities.json'
        self.vessels, self.stats = {}, Counter()
        self.status = {'connected': False, 'error': None, 'last_message': None, 'since': None}
        for mmsi, identity in (read_json(self.path, {}) or {}).items():
            self.vessels[int(mmsi)] = {'mmsi': int(mmsi), **identity}
        self._saved = time.time()

    def handle(self, raw, now=None):
        """Merge one aisstream envelope."""
        now = now or time.time()
        envelope = json.loads(raw)
        if 'error' in envelope:            # bad key or subscription
            raise RuntimeError(str(envelope['error'])[:200])
        kind = envelope.get('MessageType')
        self.stats[kind] += 1
        self.status['last_message'] = now
        meta, body = envelope.get('MetaData', {}), envelope.get('Message', {}).get(kind, {})
        mmsi = meta.get('MMSI') or body.get('UserID')
        if not mmsi:
            return
        v = self.vessels.setdefault(mmsi, {'mmsi': mmsi})
        if (meta.get('ShipName') or '').strip():
            v.setdefault('name', meta['ShipName'].strip())
        if kind in ('PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport'):
            if body.get('Valid', True) and abs(body.get('Latitude', 91)) <= 90 and abs(body.get('Longitude', 181)) <= 180:
                v.update(lat=body['Latitude'], lon=body['Longitude'], sog=body.get('Sog'), cog=body.get('Cog'),
                         heading=body.get('TrueHeading'), nav_status=body.get('NavigationalStatus'), seen=now)
            if kind == 'ExtendedClassBPositionReport' and body.get('Type'):
                v['type'] = body['Type']
        elif kind == 'ShipStaticData':
            eta = body.get('Eta') or {}
            v.update(name=(body.get('Name') or '').strip() or v.get('name'), type=body.get('Type') or v.get('type'),
                     imo=body.get('ImoNumber') or None, callsign=(body.get('CallSign') or '').strip(),
                     destination=(body.get('Destination') or '').strip(), draught_m=body.get('MaximumStaticDraught'),
                     length_m=sum((body.get('Dimension') or {}).get(k, 0) for k in 'AB') or None,
                     # AIS ETA has no year; month 0 means "not available".
                     eta=f"{eta['Month']:02d}-{eta['Day']:02d} {eta['Hour']:02d}:{eta['Minute']:02d}" if eta.get('Month') else None)
        elif kind == 'StaticDataReport':
            report = body.get('ReportB') or {}
            if report.get('Valid') and report.get('ShipType'):
                v['type'] = report['ShipType']
        if now - self._saved > 600:
            self.save(now)

    def save(self, now=None):
        """Identities only (positions go stale within hours); keeps tanker labels across restarts."""
        self._saved = now or time.time()
        known = {str(m): {f: v[f] for f in IDENTITY_FIELDS if v.get(f) is not None}
                 for m, v in self.vessels.items() if v.get('type') or v.get('name')}
        try:
            write_json(self.path, known)
        except OSError:
            pass

    def payload(self, everything=False, now=None):
        now = now or time.time()
        ships = []
        for v in self.vessels.values():
            if v.get('lat') is None or now - v.get('seen', 0) > STALE_AFTER:
                continue
            kind = category(v.get('type'))
            if not everything and kind != 'tanker':
                continue
            ships.append({'mmsi': v['mmsi'], 'name': v.get('name'), 'kind': kind, 'type': v.get('type'),
                          'lat': round(v['lat'], 5), 'lon': round(v['lon'], 5), 'sog': v.get('sog'), 'cog': v.get('cog'),
                          'heading': v.get('heading') if v.get('heading') not in (None, 511) else None,
                          'status': NAV_STATUS.get(v.get('nav_status')), 'destination': v.get('destination') or None,
                          'eta': v.get('eta'), 'draught_m': v.get('draught_m'), 'length_m': v.get('length_m'),
                          'imo': v.get('imo'), 'minutes_ago': round((now - v['seen']) / 60, 1)})
        live = {k: sum(1 for v in self.vessels.values() if v.get('lat') is not None and now - v.get('seen', 0) <= STALE_AFTER
                       and category(v.get('type')) == k) for k in ('tanker', 'cargo', 'passenger', 'tug', 'fishing', 'pleasure', 'other', 'unknown')}
        last = self.status['last_message']
        return {'ships': ships, 'counts': live, 'connected': self.status['connected'], 'error': self.status['error'],
                'last_message_seconds': round(now - last) if last else None,
                'source': 'aisstream.io (AIS); positions within the Queensland coast box'}

    # -- minimal RFC 6455 client: masked text frames out, unmasked frames in -------------------------
    @staticmethod
    def _frame(opcode, payload):
        mask, n = os.urandom(4), len(payload)
        head = bytes([0x80 | opcode]) + (bytes([0x80 | n]) if n < 126 else struct.pack('!BH', 0x80 | 126, n)
                                         if n < 65536 else struct.pack('!BQ', 0x80 | 127, n))
        return head + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload))

    async def _session(self, key):
        reader, writer = await asyncio.open_connection(AIS_HOST, 443, ssl=ssl.create_default_context(), limit=2 ** 22)
        try:
            handshake = base64.b64encode(os.urandom(16)).decode()
            writer.write((f'GET {AIS_PATH} HTTP/1.1\r\nHost: {AIS_HOST}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                          f'Sec-WebSocket-Key: {handshake}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode())
            head = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), 20)
            if b' 101 ' not in head.split(b'\r\n', 1)[0]:
                raise ConnectionError('aisstream refused the connection')
            # The subscription must arrive within 3 s of connecting.
            writer.write(self._frame(1, json.dumps({'APIKey': key, 'BoundingBoxes': QLD_BOXES,
                                                    'FilterMessageTypes': MSG_TYPES}).encode()))
            self.status.update(connected=True, error=None, since=time.time())
            buffer = b''
            while True:
                # 120 s without any frame means a dead connection; reconnect.
                b0, b1 = await asyncio.wait_for(reader.readexactly(2), 120)
                n = b1 & 0x7F
                if n == 126:
                    n = struct.unpack('!H', await reader.readexactly(2))[0]
                elif n == 127:
                    n = struct.unpack('!Q', await reader.readexactly(8))[0]
                payload, op = await reader.readexactly(n), b0 & 0x0F
                if op == 9:
                    writer.write(self._frame(10, payload))
                elif op == 8:
                    return
                elif op in (0, 1, 2):
                    buffer += payload
                    if b0 & 0x80:
                        self.handle(buffer)
                        buffer = b''
        finally:
            self.status['connected'] = False
            writer.close()

    async def run_forever(self, key):
        """Background task: reconnect with backoff; a rejected key backs off for 5 minutes."""
        delay = 5
        while True:
            try:
                await self._session(key)
                delay = 5
            except asyncio.CancelledError:
                self.save()
                raise
            except RuntimeError as exc:
                self.status['error'], delay = f'aisstream rejected the request: {exc}', 300
            except Exception as exc:
                self.status['error'], delay = f'{type(exc).__name__}; reconnecting', min(delay * 2, 300)
            await asyncio.sleep(delay)


# -- Qships: scheduled movements and vessels at berth --------------------------------------------
QSHIPS = 'https://qships.tmr.qld.gov.au/webx/'
PORTS = {'Brisbane': 67, 'Gladstone': 40, 'Townsville': 296004, 'Mackay': 295993,
         'Cairns': 295994, 'Weipa': 296002, 'Port Alma': 69, 'Bundaberg': 66}


def _qships_date(value):
    """'/Date(1790778615107+1000)/' -> ISO UTC; the millisecond epoch is already UTC."""
    if not value:
        return None
    match = re.match(r'/Date\((-?\d+)', value)
    return datetime.fromtimestamp(int(match[1]) / 1000, timezone.utc).isoformat() if match else None


def _report(session, code, domain=-1, window=None):
    param = {'__type': 'ParameterValueDTO:#WebX.Core.DTO', 'sName': 'DOMAIN_ID', 'iValueType': 0,
             'aoValues': [{'__type': 'ValueItemDTO:#WebX.Core.DTO', 'Value': domain}]}
    r = session.post(QSHIPS + 'services/wxdata.svc/GetDataX', timeout=60, json={
        'token': None, 'reportCode': code, 'dataSource': None, 'filterName': window, 'parameters': [param], 'metaVersion': 0})
    r.raise_for_status()
    table = r.json()['d']['Tables'][0]
    columns = [c['Name'] for c in table['MetaData']['Columns']]
    return [dict(zip(columns, row)) for row in table['Data']]


def port_movements(now=None):
    """Next 7 days of movements at the fuel-relevant ports, plus tankers at berth now."""
    session = requests.Session()
    session.headers['User-Agent'] = 'Mozilla/5.0 (personal Queensland fuel dashboard)'
    session.get(QSHIPS, timeout=30).raise_for_status()      # guest session cookies
    now_iso = (now or datetime.now(timezone.utc)).isoformat()
    moves = []
    for port, domain in PORTS.items():
        for r in _report(session, 'MSQ-WEB-0001', domain, 'Next 7 days'):
            moves.append({'port': port, 'job': r.get('JOB_TYPE_CODE'), 'status': r.get('STATUS_TYPE_CODE'),
                          'vessel': (r.get('VESSEL_NAME') or '').strip(), 'vessel_id': r.get('VESSEL_ID'),
                          'ship_type': r.get('MSQ_SHIP_TYPE'), 'length_m': r.get('LOA'),
                          'start': _qships_date(r.get('START_TIME')), 'to': (r.get('TO_LOCATION_NAME') or '').strip(),
                          'last_port': (r.get('LASTPORT_NAME') or '').strip() or None, 'agent': r.get('AGENCY_NAME')})
    types = {m['vessel_id']: m['ship_type'] for m in moves}
    berth = [{'port': r.get('PORT'), 'vessel': (r.get('SHIP') or '').strip(), 'berth': r.get('BERTH'),
              'arrived': r.get('ARR_DATE'), 'departs': r.get('DEP_DATE') or None, 'ship_type': types.get(r.get('VESSEL_ID'))}
             for r in _report(session, 'MSQ-WEB-0018')]
    tanker = lambda t: t == 'TANKER'  # noqa: E731  (oil, product and chemical tankers; LNG/LPG carriers are separate)
    arrivals = sorted((m for m in moves if tanker(m['ship_type']) and m['job'] == 'ARR'
                       and m['status'] not in ('COMP', 'CANC') and (m['start'] or '') >= now_iso), key=lambda m: m['start'])
    return {'arrivals': arrivals, 'at_berth': [b for b in berth if tanker(b['ship_type'])],
            'counts': {'movements_7d': len(moves), 'tanker_arrivals_7d': len(arrivals),
                       'arrivals_by_port': dict(Counter(m['port'] for m in arrivals))},
            'source': 'Maritime Safety Queensland, Qships ship movements (next 7 days) and vessels at berth'}
