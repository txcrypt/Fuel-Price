import json
from tempfile import TemporaryDirectory
from unittest import TestCase
import ships


def envelope(kind, mmsi, body, name=''):
    return json.dumps({'MessageType': kind, 'MetaData': {'MMSI': mmsi, 'ShipName': name}, 'Message': {kind: body}})


class AisTests(TestCase):
    def test_positions_identity_staleness_and_restart(self):
        with TemporaryDirectory() as d:
            ais = ships.Ais(d)
            ais.handle(envelope('ShipStaticData', 503000001, {'Name': 'TEST TANKER', 'Type': 80, 'Destination': 'AU BNE',
                                                              'MaximumStaticDraught': 11.2, 'Dimension': {'A': 150, 'B': 33},
                                                              'Eta': {'Month': 10, 'Day': 3, 'Hour': 6, 'Minute': 0}}), now=1000)
            ais.handle(envelope('PositionReport', 503000001, {'Valid': True, 'Latitude': -27.3, 'Longitude': 153.2,
                                                              'Sog': 10.5, 'Cog': 90, 'TrueHeading': 511}), now=1000)
            ais.handle(envelope('PositionReport', 503000002, {'Valid': True, 'Latitude': -27.4, 'Longitude': 153.1}, 'TUG'), now=1000)
            tanker = ais.payload(now=1060)['ships']
            self.assertEqual([(s['name'], s['kind'], s['length_m'], s['heading'], s['eta']) for s in tanker],
                             [('TEST TANKER', 'tanker', 183, None, '10-03 06:00')])   # 511 = heading not available
            self.assertEqual(len(ais.payload(everything=True, now=1060)['ships']), 2)
            self.assertEqual(ais.payload(now=1000 + ships.STALE_AFTER + 1)['ships'], [])   # gone quiet: dropped
            ais.save(1100)
            # A restart remembers which ship is a tanker, before its identity is broadcast again.
            again = ships.Ais(d)
            again.handle(envelope('PositionReport', 503000001, {'Valid': True, 'Latitude': -27.31, 'Longitude': 153.21}), now=2000)
            self.assertEqual(again.payload(now=2000)['ships'][0]['name'], 'TEST TANKER')

    def test_error_envelope_raises_and_frames_are_masked(self):
        with self.assertRaises(RuntimeError):
            ships.Ais(TemporaryDirectory().name).handle(json.dumps({'error': 'Api Key Is Not Valid'}))
        frame = ships.Ais._frame(1, b'x' * 200)
        self.assertEqual((frame[0], frame[1]), (0x81, 0x80 | 126))

    def test_ship_categories(self):
        self.assertEqual([ships.category(t) for t in (80, 84, 70, 60, 52, 30, 36, 0)],
                         ['tanker', 'tanker', 'cargo', 'passenger', 'tug', 'fishing', 'pleasure', 'unknown'])

    def test_qships_dates_are_utc(self):
        self.assertEqual(ships._qships_date('/Date(1790778615107+1000)/'), '2026-09-30T14:30:15.107000+00:00')
