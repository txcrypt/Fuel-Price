from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch, Mock
from alerts import Alerts


def dashboard(level='quiet', price=230.0, flag=None):
    return {'restoration': {'level': level, 'restored_share': .15, 'message': 'Building.'},
            'stations': [{'site_id': '1', 'name': 'Cheap St', 'price_cpl': price, 'flag': flag}]}


class AlertTests(TestCase):
    def setUp(self):
        self.dir = TemporaryDirectory(); self.addCleanup(self.dir.cleanup)
        self.alerts = Alerts(Path(self.dir.name))
        self.alerts.save('fuel-test-topic', True, [{'site_id': '1', 'name': 'Cheap St', 'below': 225}])

    def run_check(self, board):
        with patch('alerts.requests.post', return_value=Mock(raise_for_status=Mock())) as post:
            sent = self.alerts.check(board)
        return sent, post

    def test_each_alert_fires_once_and_rearms_when_it_clears(self):
        self.assertEqual(self.run_check(dashboard())[0], [])
        self.assertEqual(self.run_check(dashboard('building', 224.0))[0], ['jump:building', 'watch:1'])
        self.assertEqual(self.run_check(dashboard('building', 223.0))[0], [])          # still true: no repeat
        self.assertEqual(self.run_check(dashboard('imminent', 226.0))[0], ['jump:imminent'])
        self.assertEqual(self.run_check(dashboard('quiet', 224.0))[0], ['watch:1'])    # re-armed above target

    def test_flagged_price_never_triggers_and_topic_is_validated(self):
        self.assertEqual(self.run_check(dashboard(price=99.9, flag='suspect'))[0], [])
        with self.assertRaises(ValueError):
            self.alerts.save('bad topic!', True, [])
        with self.assertRaises(ValueError):
            self.alerts.save('fuel-test-topic', True, [{'site_id': '1', 'below': 5}])

    def test_messages_go_to_the_topic(self):
        _, post = self.run_check(dashboard('building'))
        self.assertEqual(post.call_args.args[0], 'https://ntfy.sh/fuel-test-topic')
        self.assertEqual(post.call_args.kwargs['headers']['Priority'], 'high')
