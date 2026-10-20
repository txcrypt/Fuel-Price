"""Everything the dashboard already holds, summarised for one place: fuel prices, road events,
cameras and bushfires inside a bounding box. Observed data only; nothing here is forecast."""
from collections import Counter
from statistics import mean, median
from traffic import positions

MIN_SPAN_DEG = .05   # A suburb's box can be a few streets wide; pad to roughly 5 km so it holds stations.


def padded(south, west, north, east):
    pad_lat = max(0, (MIN_SPAN_DEG - (north - south)) / 2)
    pad_lng = max(0, (MIN_SPAN_DEG - (east - west)) / 2)
    return {'lat_min': south - pad_lat, 'lat_max': north + pad_lat, 'lng_min': west - pad_lng, 'lng_max': east + pad_lng}


def inside(b, lat, lng):
    try:
        return b['lat_min'] <= float(lat) <= b['lat_max'] and b['lng_min'] <= float(lng) <= b['lng_max']
    except (TypeError, ValueError):
        return False


def touches(b, geometry):
    """True when any position of the geometry falls in the box. ponytail: a long road whose
    points all lie outside a small box is missed; bbox-intersection would catch it if that matters."""
    return any(inside(b, lat, lng) for lng, lat in positions(geometry))


def fuel(b, stations):
    """U91 over stations in the box. Flagged prices (stale or suspect) are counted, never ranked."""
    here = [s for s in stations if inside(b, s.get('latitude'), s.get('longitude')) and s.get('price_cpl') is not None]
    usable = [s for s in here if not s.get('flag')]
    if not usable:
        return {'stations': len(here), 'flagged': len(here)}
    prices = [s['price_cpl'] for s in usable]
    low, high = min(usable, key=lambda s: s['price_cpl']), max(usable, key=lambda s: s['price_cpl'])
    below = sum(p <= min(prices) + 5 for p in prices)
    return {'stations': len(here), 'flagged': len(here) - len(usable), 'mean': round(mean(prices), 1),
            'median': round(median(prices), 1), 'spread': round(high['price_cpl'] - low['price_cpl'], 1),
            'lowest': {'name': low.get('name'), 'price': low['price_cpl'], 'site_id': str(low.get('site_id'))},
            'highest': {'name': high.get('name'), 'price': high['price_cpl'], 'site_id': str(high.get('site_id'))},
            'within_5c_of_lowest': below}


def roads(b, features, limit=6):
    """Current road events in the box; scheduled and archived ones are left out of the headline count."""
    here = [f for f in features if touches(b, f.get('geometry'))]
    current = [f for f in here if f['properties'].get('_timing') in ('date_window', 'unknown')
               and str(f['properties'].get('status', '')).lower() != 'archived']
    listed = [{'type': p.get('event_type'), 'road': (p.get('road_summary') or {}).get('road_name') or p.get('description'),
               'locality': (p.get('road_summary') or {}).get('locality'), 'impact': (p.get('impact') or {}).get('impact_type'),
               'delay': (p.get('impact') or {}).get('delay')}
              for p in (f['properties'] for f in current)]
    # Closures and major impacts first: those are the ones that change a trip.
    listed.sort(key=lambda e: 0 if 'clos' in str(e['impact']).lower() else 1)
    return {'current': len(current), 'scheduled': sum(f['properties'].get('_timing') == 'scheduled' for f in here),
            'by_type': dict(Counter(e['type'] or 'Other' for e in listed).most_common()), 'events': listed[:limit]}


def brief(b, stations, features, cameras, fires):
    return {'bounds': b, 'fuel': fuel(b, stations), 'roads': roads(b, features),
            'cameras': [c for c in cameras if inside(b, c['lat'], c['lng'])],
            'bushfires': dict(Counter(f['properties'].get('level') or 'Unknown'
                                      for f in fires if touches(b, f.get('geometry'))))}
