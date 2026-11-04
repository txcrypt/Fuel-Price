"""Keep small research scoreboards reviewable alongside the reports.

Raw station panels and individual forecast rows remain in the ignored local
cache. This copies only aggregate research evidence, never credentials.
"""
from pathlib import Path
import argparse
import json
import hashlib

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / '.local' / 'research-2026-11-04'
DEST = ROOT / 'docs' / 'research-assets'


def preserve(model_dirs):
    DEST.mkdir(parents=True, exist_ok=True)
    paths = [CACHE / name for name in ('audit.json', 'expiry_sensitivity.json',
              'upstream_manifest.json', 'upstream_lag_correlations.csv',
              'upstream_lag_notes.json', 'forward_scoreboard.csv', 'research_versions.json',
              'excise_metrics.csv', 'excise_schedule.json')]
    for directory in model_dirs:
        paths += [directory / name for name in ('metrics.csv', 'regime_metrics.csv',
                    'development_selection.csv', 'risk_metrics.csv', 'manifest.json')]
    records = []
    for source in paths:
        if not source.is_file():
            raise FileNotFoundError(source)
        label = source.name if source.parent == CACHE else source.parent.name + '_' + source.name
        target = DEST / label
        data = source.read_bytes()
        target.write_bytes(data)
        records.append({'file': label, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)})
    (DEST / 'evidence_manifest.json').write_text(json.dumps(records, indent=2), encoding='utf-8')
    print(json.dumps(records, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--models', nargs='+', type=Path, required=True)
    preserve(parser.parse_args().models)
