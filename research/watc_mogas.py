"""Extract auditable Mogas95 quotes from an explicit, bounded WATC PDF list.

Private research feasibility only: no API, discovery crawler, missing-quote
imputation or production integration. Raw quotes remain in the ignored cache.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlparse

import pandas as pd
from pypdf import PdfReader
import requests


def parse_quote(text,filename):
    normal=' '.join(text.replace('\u2212','-').split())
    headings=re.findall(r'(?:Market\s+)?Daily\s+(?:Report|Update)\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})',normal,re.I)
    dates={datetime.strptime(x.title(),'%d %B %Y').date().isoformat() for x in headings}
    if len(dates)!=1: raise ValueError('Daily report date missing or contradictory')
    report_date=next(iter(dates))
    match=re.fullmatch(r'(\d{6}|\d{8})_daily-report[.]pdf',filename,re.I)
    if not match: raise ValueError('Unrecognised daily PDF filename')
    filename_date=datetime.strptime(match[1],'%d%m%Y' if len(match[1])==8 else '%d%m%y').date().isoformat()
    if filename_date!=report_date: raise ValueError('Filename and document report dates disagree')
    if not re.search(r'Commodities\s+US\s*\$',normal,re.I):
        raise ValueError('USD commodity header not verified')
    if not re.search(r'\*?\s*Mogas95\s+is\s+the\s+Singapore\s+benchmark',normal,re.I):
        raise ValueError('Singapore benchmark identification not verified')
    matches=re.findall(r'\bMogas95\s*\*\s*(\d+(?:[.]\d+)?)\s+(-?\$\s*\d+(?:[.]\d+)?)',normal)
    if len(matches)!=1: raise ValueError('Expected exactly one numeric Mogas95 row')
    value,change=matches[0]
    quote=float(value);delta=float(change.replace('$','').replace(' ',''))
    if not 0<quote<1000 or not -1000<delta<1000: raise ValueError('Implausible quote or displayed change')
    notes=re.findall(r'Changes\s+(?:since|are\s+from|from)[^.]*[.]?',normal,re.I)
    return {'report_date':report_date,'mogas95_quote':quote,'displayed_change':delta,
            'currency':'USD','daily_table_denominator':'not_stated',
            'contextual_unit':'USD/barrel','unit_evidence':'WATC weekly 2026-03-13 page2 Mogas chart; daily unit not separately specified',
            'quote_observed_date':'','quote_observed_at':'','quote_vendor_ticker':'',
            'change_interpretation':'publisher displayed change; not assumed one-day return',
            'change_notes':' | '.join(notes),
            'historical_publication_at':'','verified_point_in_time_ready':False}


def extract(manifest_path,output,download=False):
    manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
    if not isinstance(manifest,list) or not 1<=len(manifest)<=40:
        raise ValueError('An explicit list of 1–40 sample reports is required')
    if output.resolve().parent!=manifest_path.resolve().parent:
        raise ValueError('Keep extracted sample beside its private research manifest')
    rows=[];audit=[]
    for item in manifest:
        url=item['url'];parsed=urlparse(url)
        if parsed.scheme!='https' or parsed.netloc!='www.watc.wa.gov.au' or not parsed.path.startswith('/media/'):
            raise ValueError('Expected an observed official WATC media URL')
        filename=Path(parsed.path).name;local=manifest_path.parent/filename
        evidence={'url':url,'local_file':filename,'discovered_via':item.get('discovered_via',''),
                  'archive_capture_at_utc':item.get('archive_capture_at_utc','')}
        try:
            if not local.exists():
                if not download: raise FileNotFoundError('Sample PDF not cached; explicit --download required')
                response=requests.get(url,timeout=45);response.raise_for_status()
                if not response.content.startswith(b'%PDF'): raise ValueError('Response is not a PDF')
                local.write_bytes(response.content)
                http={'url':url,'fetched_at_utc':datetime.now(timezone.utc).isoformat(),
                      'status':response.status_code,'headers':dict(response.headers)}
                local.with_suffix('.http.json').write_text(json.dumps(http,indent=2),encoding='utf-8')
            content=local.read_bytes();digest=hashlib.sha256(content).hexdigest()
            if item.get('sha256') and item['sha256']!=digest: raise ValueError('Cached PDF differs from pinned checksum')
            reader=PdfReader(local);text='\n'.join(page.extract_text() for page in reader.pages)
            quote=parse_quote(text,filename)
            evidence.update(sha256=digest,bytes=len(content),pages=len(reader.pages),
                            pdf_metadata={str(k):str(v) for k,v in (reader.metadata or {}).items()},status='extracted')
            quote.update(source_url=url,pdf_sha256=digest,source_page=1,
                         pdf_creation_date=str((reader.metadata or {}).get('/CreationDate','')),
                         recorded_at_utc=datetime.now(timezone.utc).isoformat(),
                         archive_capture_at_utc=item.get('archive_capture_at_utc',''))
            rows.append(quote)
            print(f'EXTRACTED {quote["report_date"]} {filename}',flush=True)
        except (ValueError,FileNotFoundError,requests.RequestException) as exc:
            evidence.update(status='failed',failure_category=type(exc).__name__,reason=str(exc))
            print(f'FAILED {filename}: {type(exc).__name__}',flush=True)
        audit.append(evidence)
    frame=pd.DataFrame(rows).sort_values('report_date') if rows else pd.DataFrame()
    if len(frame) and frame.report_date.duplicated().any():
        raise ValueError('Multiple sample PDFs claim the same report date; review revisions explicitly')
    frame.to_csv(output,index=False)
    summary={'reports_requested':len(manifest),'quotes_extracted':len(rows),'failed':len(audit)-len(rows),
             'missing_quotes_imputed':0,'permission_scope':'bounded private research feasibility; public redistribution not licensed',
             'unit_caveat':'Daily header states USD only; per-barrel context comes from same publisher weekly Mogas chart.',
             'known_at_caveat':'Report/PDF creation dates are not verified original publication or quote timestamps.',
             'output_sha256':hashlib.sha256(output.read_bytes()).hexdigest(),'sources':audit}
    output.with_suffix('.audit.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in summary.items() if k!='sources'},indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--download',action='store_true')
    args=parser.parse_args()
    extract(args.manifest,args.output,args.download)
