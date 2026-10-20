"""Phone push alerts through ntfy (https://ntfy.sh): a price jump building, or a watched station at or
below a target price. Checked after every collection, so they work with no browser open.

Each alert fires once and re-arms only when its condition clears, so a price sitting under a
target does not notify every 30 minutes. Anyone who knows the topic name can read its messages,
so the dashboard suggests a long random one.
"""
import re
import secrets
import threading
import requests
from api_evidence import read_json, write_json

NTFY = 'https://ntfy.sh/'
TOPIC = re.compile(r'^[A-Za-z0-9_-]{8,64}$')
LEVEL_RANK = {'quiet': 0, 'building': 1, 'imminent': 2}


class Alerts:
    def __init__(self, root):
        self.settings_path = root / '.local' / 'alerts.json'
        self.state_path = root / '.local' / 'alerts_state.json'
        self.lock = threading.Lock()

    def settings(self):
        saved = read_json(self.settings_path, {})
        return {'topic': saved.get('topic') or '', 'jump': bool(saved.get('jump', True)),
                'watches': saved.get('watches') or [], 'link': saved.get('link') or '',
                'suggested_topic': saved.get('suggested_topic') or self._suggest()}

    def _suggest(self):
        topic = 'fuel-' + secrets.token_urlsafe(12).replace('-', 'x').replace('_', 'y')
        saved = read_json(self.settings_path, {})
        write_json(self.settings_path, {**saved, 'suggested_topic': topic})
        return topic

    def save(self, topic, jump, watches, link=''):
        topic = (topic or '').strip()
        if topic and not TOPIC.match(topic):
            raise ValueError('The topic must be 8-64 letters, digits, - or _.')
        clean = []
        for w in watches[:20]:
            below = float(w.get('below'))
            if not 80 <= below <= 350:
                raise ValueError('Target prices must be between 80 and 350 c/L.')
            clean.append({'site_id': str(w['site_id'])[:20], 'name': str(w.get('name') or '')[:80], 'below': round(below, 1)})
        if link and not re.match(r'^https?://', link):
            raise ValueError('The link must start with http:// or https://.')
        with self.lock:
            saved = read_json(self.settings_path, {})
            write_json(self.settings_path, {**saved, 'topic': topic, 'jump': bool(jump), 'watches': clean, 'link': link[:200]})
        return self.settings()

    def send(self, title, message, tags='fuelpump', priority='default'):
        topic = self.settings()['topic']
        if not topic:
            return False
        headers = {'Title': title.encode('utf-8'), 'Tags': tags, 'Priority': priority}
        link = self.settings()['link']
        if link:
            headers['Click'] = link
        try:
            requests.post(NTFY + topic, data=message.encode('utf-8'), headers=headers, timeout=(5, 10)).raise_for_status()
            return True
        except requests.RequestException:
            return False

    def check(self, dashboard):
        """Compare this collection with the last one and send what newly became true."""
        settings = self.settings()
        if not settings['topic']:
            return []
        with self.lock:
            state = read_json(self.state_path, {})
            sent = []
            signal = dashboard.get('restoration') or {}
            level = signal.get('level')
            before = state.get('jump_level', 'quiet')
            if settings['jump'] and level and LEVEL_RANK.get(level, 0) > LEVEL_RANK.get(before, 0):
                share = round((signal.get('restored_share') or 0) * 100)
                if self.send(f'Fuel price jump {level}', f'{share}% of stations are already 20 c/L or more above the median. '
                             f'{signal.get("message", "")} Filling up sooner is likely cheaper.', tags='warning,fuelpump', priority='high'):
                    sent.append(f'jump:{level}')
            if level:
                state['jump_level'] = level
            prices = {str(s['site_id']): s for s in dashboard.get('stations', [])}
            fired = state.get('fired', {})
            for watch in settings['watches']:
                station = prices.get(watch['site_id'])
                if not station or station.get('flag'):
                    continue   # an old or suspect price must not trigger a "cheap" alert
                price, site = station['price_cpl'], watch['site_id']
                if price <= watch['below'] and not fired.get(site):
                    if self.send(f'{station.get("name") or watch["name"]}: {price:.1f} c/L',
                                 f'At or below your target of {watch["below"]:.1f} c/L.', tags='moneybag,fuelpump'):
                        fired[site] = price
                        sent.append(f'watch:{site}')
                elif price > watch['below']:
                    fired.pop(site, None)   # re-arm once it climbs back above the target
            state['fired'] = fired
            write_json(self.state_path, state)
            return sent
