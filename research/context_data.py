"""Acquire official upstream history without changing the dashboard's database.

Current historical files are not real-time vintages. Availability lags used in
experiments are explicit assumptions, not evidence of historical availability.
Run from the repository: python research/context_data.py
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import config

OUT = ROOT / '.local' / 'research-2026-11-04'


def acquire():
    OUT.mkdir(parents=True, exist_ok=True)
    manifest, series = [], []

    def record(name, url, content, frame, lag):
        frame = frame.rename(columns={'date': 'observed_date'})
        frame['observed_date'] = pd.to_datetime(frame.observed_date).dt.strftime('%Y-%m-%d')
        frame['source'] = name
        frame['assumed_available_date'] = (pd.to_datetime(frame.observed_date)
                                          + pd.Timedelta(days=lag)).dt.strftime('%Y-%m-%d')
        frame = frame.loc[frame.observed_date.between('2023-01-01', '2026-11-03')]
        series.append(frame)
        manifest.append({'source': name, 'url': url, 'sha256': hashlib.sha256(content).hexdigest(),
                         'rows': len(frame), 'first': frame.observed_date.min(),
                         'last': frame.observed_date.max(), 'assumed_lag_calendar_days': lag,
                         'retrieved_at_utc': datetime.now(timezone.utc).isoformat(),
                         'vintage_status': 'current historical file; not archived real-time vintage'})

    def get(url, **kwargs):
        response = requests.get(url, timeout=45, **kwargs)
        response.raise_for_status()
        return response

    for name, code in [('brent', 'RBRTE'), ('gasoline_gulf', 'EER_EPMRU_PF4_RGC_DPG')]:
        try:
            url = 'https://api.eia.gov/v2/petroleum/pri/spt/data/'
            response = get(url, params={'api_key': config.EIA_API_KEY, 'frequency': 'daily',
                'data[0]': 'value', 'facets[series][]': code, 'start': '2023-01-01',
                'sort[0][column]': 'period', 'sort[0][direction]': 'asc', 'length': 5000})
            rows = response.json()['response']['data']
            frame = pd.DataFrame([{'date': r['period'], 'value': float(r['value'])}
                                  for r in rows if r.get('value') is not None])
            record(name, url + ' (series ' + code + '; credential omitted)', response.content, frame, 4)
        except Exception as error:
            manifest.append({'source': name, 'error_type': type(error).__name__})

    try:
        url = 'https://www.rba.gov.au/statistics/tables/csv/f11.1-data.csv'
        response = get(url)
        lines = response.text.splitlines()
        start = next(i for i, line in enumerate(lines) if line.startswith('Series ID,'))
        raw = pd.read_csv(io.StringIO('\n'.join(lines[start:])))
        frame = pd.DataFrame({'date': pd.to_datetime(raw.iloc[:, 0], format='%d-%b-%Y', errors='coerce'),
                              'value': pd.to_numeric(raw['FXRUSD'], errors='coerce')}).dropna()
        record('audusd', url, response.content, frame, 1)
    except Exception as error:
        manifest.append({'source': 'audusd', 'error_type': type(error).__name__})

    try:
        page = 'https://aip.com.au/resources/historical-ulp-and-diesel-tgp-data'
        html = get(page).text
        links = re.findall(r'href=[\"\']([^\"\']+\.xlsx)[\"\']', html)
        daily = [urljoin(page, x) for x in links if 'AIP_TGP_Data_' in x]
        url = daily[-1]
        response = get(url)
        book = pd.ExcelFile(io.BytesIO(response.content))
        # Select by the actual header and sheet title, never by column position alone.
        found = False
        for sheet in book.sheet_names:
            if 'ULP' not in sheet.upper() and 'PETROL' not in sheet.upper():
                continue
            raw = pd.read_excel(book, sheet_name=sheet, header=None)
            for i in range(min(20, len(raw))):
                values = raw.iloc[i].astype(str).str.strip()
                if values.eq('Brisbane').any():
                    col = int(values.eq('Brisbane').to_numpy().nonzero()[0][0])
                    frame = pd.DataFrame({'date': pd.to_datetime(raw.iloc[i+1:, 0], errors='coerce'),
                        'value': pd.to_numeric(raw.iloc[i+1:, col], errors='coerce')}).dropna()
                    if len(frame) > 100:
                        record('tgp', url, response.content, frame, 1)
                        found = True
                        break
            if found:
                break
        if not found:
            raise ValueError('No daily ULP Brisbane table verified')
    except Exception as error:
        manifest.append({'source': 'tgp', 'error_type': type(error).__name__})

    result = pd.concat(series, ignore_index=True) if series else pd.DataFrame()
    result.to_csv(OUT / 'upstream_history.csv', index=False)
    (OUT / 'upstream_manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(json.dumps(manifest, indent=2))
    return result


if __name__ == '__main__':
    acquire()
