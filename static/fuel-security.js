/* Fuel security: Australia's stocks against the legal minimum, month-end cover, imports, refining,
   Queensland sales and Port of Brisbane fuel imports. Published statistics, each with its own date. */
(() => {
  'use strict';
  const byId = id => document.getElementById(id);
  const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
  const num = (v, d = 0) => v == null || !Number.isFinite(Number(v)) ? '—' : Number(v).toLocaleString('en-AU', {maximumFractionDigits: d, minimumFractionDigits: d});
  const month = ym => ym ? new Date(ym + '-15').toLocaleDateString('en-AU', {month: 'short', year: 'numeric'}) : '—';
  const day = iso => iso ? new Date(iso + 'T12:00:00').toLocaleDateString('en-AU', {day: 'numeric', month: 'short', year: 'numeric'}) : '—';

  // Minimal trend line; the last point is the value shown beside it.
  function spark(values, {lo, hi, colour = '#58dac7', marker} = {}) {
    const v = values.map(Number).filter(Number.isFinite);
    if (v.length < 2) return '';
    const min = lo ?? Math.min(...v), max = hi ?? Math.max(...v), W = 120, H = 30, span = max - min || 1;
    const pts = v.map((x, i) => `${(i / (v.length - 1) * W).toFixed(1)},${(H - (x - min) / span * H).toFixed(1)}`).join(' ');
    const line = marker == null ? '' : `<line x1="0" x2="${W}" y1="${(H - (marker - min) / span * H).toFixed(1)}" y2="${(H - (marker - min) / span * H).toFixed(1)}" stroke="#f67e83" stroke-dasharray="3 3"/>`;
    return `<svg class="fs-spark" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" aria-hidden="true">${line}<polyline points="${pts}" fill="none" stroke="${colour}" stroke-width="2"/></svg>`;
  }
  const card = (label, value, note, extra = '', tone = '') => `<div class="fs-card ${tone}"><small>${label}</small><b>${value}</b>${extra}<span>${note}</span></div>`;

  const panel = document.createElement('section');
  panel.className = 'panel'; panel.id = 'fuel-security';
  panel.innerHTML = `<div class="panel-head"><h2>Fuel security · Australia &amp; Queensland</h2><span class="index">PUBLISHED STATISTICS</span></div>
    <div class="panel-body fs-body"><p class="muted">Loading fuel stock statistics…</p></div>`;
  (byId('station-map') || byId('field')).after(panel);
  const nav = document.createElement('a');
  nav.href = '#fuel-security'; nav.textContent = 'Fuel security';
  if (byId('clock')) byId('clock').before(nav); else document.querySelector('nav.nav')?.append(nav);
  const body = panel.querySelector('.fs-body');

  function render(d) {
    const out = [];
    // 1. This week: stock held under the Minimum Stockholding Obligation.
    const mso = d.mso?.products;
    if (mso) {
      const names = {gasoline: 'PETROL', diesel: 'DIESEL', kerosene: 'JET FUEL'};
      const cards = Object.entries(names).filter(([k]) => mso[k]?.length).map(([k, label]) => {
        const rows = mso[k], last = rows.at(-1), req = last.effective_required_ml, held = last.held_ml;
        const pct = Math.min(100, held && req ? (req / held) * 100 : 0);
        const tone = last.pct_above_effective < 10 ? 'warn' : '';
        return card(`${label} · HELD THIS WEEK`, `${num(last.days)} <small>days</small>`,
          `${num(held)} ML held · minimum ${num(req)} ML${req !== last.required_ml ? ` (cut from ${num(last.required_ml)})` : ''} · <b>${last.pct_above_effective > 0 ? '+' : ''}${num(last.pct_above_effective, 1)}%</b> above it`,
          `<div class="fs-bar" title="Required minimum as a share of stock held"><i style="width:${pct}%"></i></div>${spark(rows.map(r => r.days))}`, tone);
      });
      out.push(`<p class="kicker">STOCKS THIS WEEK VS THE LEGAL MINIMUM · ${esc(day(mso.gasoline?.at(-1)?.date))}</p><div class="fs-grid">${cards.join('')}</div>
        <p class="quiet">Minimum Stockholding Obligation: importers and refiners must hold these volumes in Australia. Days = stock held over recent daily use. The trend line is weekly days of cover since February; the bar shows how much of the stock the minimum takes up.</p>`);
    }
    // 2. Month-end cover and the international measure.
    const aps = d.aps;
    if (aps && !aps.error) {
      const cover = aps.series.consumption_cover_days.values, h = aps.headline;
      const series = (label, col, key) => card(label, `${num(h.days_cover[key])} <small>days</small>`,
        `${num(h.stocks_ml[key])} ML at month end`, spark(cover[col] || []));
      const iea = aps.series.iea_net_import_cover?.values['IEA days of net import coverage'] || [];
      out.push(`<p class="kicker">MONTH-END COVER · ${esc(month(aps.data_month))} · AUSTRALIAN PETROLEUM STATISTICS</p><div class="fs-grid">
        ${series('CRUDE OIL', 'Crude oil and refinery feedstocks (days)', 'crude')}
        ${series('PETROL', 'Automotive gasoline (days)', 'petrol')}
        ${series('DIESEL', 'Diesel oil (days)', 'diesel')}
        ${series('JET FUEL', 'Aviation turbine fuel (days)', 'jet')}
        ${card('IEA NET-IMPORT COVER', `${num(h.iea.days)} <small>/ 90 days</small>`,
          `${num(h.iea_incl_on_the_way?.days_total)} days counting stock at sea and overseas. Australia's treaty obligation is 90.`,
          spark(iea, {marker: 90, lo: Math.min(...iea.filter(Number.isFinite), 40), hi: 95}), h.iea.days < 90 ? 'warn' : '')}
      </div>`);
      // 3. Flows: imports, sources, refining, sales.
      const imp = h.imports_ml, ref = h.refinery_ml, sales = h.sales_ml;
      const top = Object.entries(aps.imports_by_country_ml.trailing_12m.total || {}).slice(0, 6);
      const max = top[0]?.[1] || 1;
      out.push(`<p class="kicker">FLOWS · ${esc(month(aps.data_month))}</p><div class="fs-grid">
        ${card('IMPORTS', `${num(imp.total)} <small>ML</small>`, `crude ${num(imp.crude)} · diesel ${num(imp.diesel)} · petrol ${num(imp.petrol)} · jet ${num(imp.jet)} ML`)}
        ${card('REFINING (LYTTON + GEELONG)', `${num(ref.crude_input)} <small>ML crude in</small>`, `${num(ref.domestic_crude_share_pct, 1)}% Australian crude · out: diesel ${num(ref.diesel)}, petrol ${num(ref.petrol)}, jet ${num(ref.jet)} ML`)}
        ${card('NATIONAL SALES', `${num(sales.total)} <small>ML</small>`, `diesel ${num(sales.diesel)} · petrol ${num(sales.petrol)} · jet ${num(sales.jet)} ML in the month`)}
        <div class="fs-card wide"><small>WHERE OUR FUEL COMES FROM · 12 MONTHS TO ${esc(month(aps.imports_by_country_ml.trailing_12m_range?.[1]))}</small>
          <div class="fs-countries">${top.map(([c, v]) => `<div><span>${esc(c)}</span><i style="width:${(v / max) * 100}%"></i><b>${num(v / 1000, 1)} GL</b></div>`).join('')}</div>
          <span>Total oil imports by source country (1 GL = 1,000 ML). A single-country disruption shows up here first.</span></div>
      </div>`);
    } else if (aps?.error) out.push(`<p class="muted">Australian Petroleum Statistics could not be read (${esc(aps.error)}).</p>`);
    // 4. Queensland.
    const q = d.qld, port = d.pbpl;
    const qCards = [];
    if (q) {
      qCards.push(card('QLD DIESEL SALES', `${num(q.diesel_ml.at(-1))} <small>ML</small>`, `${num(q.diesel_share_pct, 1)}% of Australia's diesel · ${esc(month(q.months.at(-1)))}`, spark(q.diesel_ml)));
      qCards.push(card('QLD PETROL SALES', `${num(q.petrol_ml.at(-1))} <small>ML</small>`, `${num(q.petrol_share_pct, 1)}% of Australia's petrol · ${esc(month(q.months.at(-1)))}`, spark(q.petrol_ml)));
    }
    if (port && !port.error) {
      const t = port.tonnes_per_ml, last = i => port[i].at(-1);
      qCards.push(card('PORT OF BRISBANE · CRUDE IN', `${num(last('crude_import_t') / 1000)} <small>kt</small>`, `≈${num(last('crude_import_t') / t)} ML for Lytton refinery · ${esc(month(port.months.at(-1)))}`, spark(port.crude_import_t.slice(-12))));
      qCards.push(card('PORT OF BRISBANE · FUEL IN / OUT', `${num(last('refined_import_t') / 1000)} <small>kt in</small>`, `≈${num(last('refined_import_t') / t)} ML imported, ${num(last('refined_export_t') / 1000)} kt shipped out · ${esc(month(port.months.at(-1)))}`, spark(port.refined_import_t.slice(-12))));
    }
    if (qCards.length) out.push(`<p class="kicker">QUEENSLAND</p><div class="fs-grid">${qCards.join('')}</div>
      <p class="quiet">${esc(d.notes.qld_stocks)} Port tonnes to megalitres uses ~0.8 t per kL, an approximation.</p>`);
    out.push('<div id="tankers"></div>');
    const errors = Object.entries(d.state || {}).filter(([, s]) => s.error).map(([k, s]) => `${k.toUpperCase()}: ${s.error}`);
    out.push(`<p class="quiet">${esc(d.notes.measures)} ${errors.length ? esc(errors.join(' ')) : ''}<br>${esc(d.notes.attribution)}</p>`);
    body.innerHTML = out.join('');
    tankersPanel(byId('tankers'));
  }

  // Ships: scheduled tanker arrivals and tankers at berth (Maritime Safety Queensland), live tankers (AIS).
  const when = iso => iso ? new Date(iso).toLocaleString('en-AU', {weekday: 'short', day: 'numeric', month: 'short', hour: 'numeric', minute: '2-digit', timeZone: 'Australia/Brisbane'}) : '—';
  async function tankersPanel(el) {
    if (!el) return;
    const [ship, live] = await Promise.all([fetch('/api/shipping').then(r => r.json()).catch(() => null),
                                            fetch('/api/ships').then(r => r.json()).catch(() => null)]);
    const parts = [];
    if (ship?.arrivals) {
      const ports = Object.entries(ship.counts.arrivals_by_port || {}).map(([p, n]) => `${esc(p)} ${n}`).join(' · ');
      parts.push(`<p class="kicker">TANKERS DUE · NEXT 7 DAYS · ${ship.arrivals.length} ARRIVALS${ports ? ' · ' + ports : ''}</p>
        ${ship.arrivals.length ? `<div class="table-scroll"><table class="fs-table"><thead><tr><th>Due (Brisbane time)</th><th>Port</th><th>Tanker</th><th>Berth</th><th>From</th></tr></thead><tbody>
        ${ship.arrivals.map(a => `<tr><td>${esc(when(a.start))}</td><td>${esc(a.port)}</td><td>${esc(a.vessel)}<small>${a.length_m ? Math.round(a.length_m) + ' m' : ''}</small></td><td>${esc(a.to)}</td><td>${esc(a.last_port || '—')}</td></tr>`).join('')}
        </tbody></table></div>` : '<p class="muted">No tanker arrivals scheduled in the next 7 days.</p>'}`);
      if (ship.at_berth?.length) parts.push(`<p class="kicker">TANKERS AT BERTH NOW</p><ul class="fs-list">${ship.at_berth.map(b =>
        `<li><b>${esc(b.vessel)}</b> · ${esc(b.port)} · ${esc(b.berth)}<small>arrived ${esc(b.arrived)}${b.departs ? ' · departs ' + esc(b.departs) : ''}</small></li>`).join('')}</ul>`);
    } else parts.push(`<p class="muted">Port schedules are loading or unavailable${ship?.error ? ': ' + esc(ship.error) : ''}.</p>`);
    if (live) {
      const tankers = (live.ships || []).filter(s => s.kind === 'tanker').sort((a, b) => (b.sog || 0) - (a.sog || 0));
      parts.push(`<p class="kicker">TANKERS ON THE WATER NOW · LIVE AIS</p>${tankers.length
        ? `<ul class="fs-list">${tankers.map(s => `<li><button type="button" class="text-button" data-ship="${s.lat},${s.lon}">${esc(s.name || 'MMSI ' + s.mmsi)}</button>
            ${s.type === 84 ? ' (gas carrier)' : ''} · ${s.sog != null ? s.sog.toFixed(1) + ' kn' : '—'}${s.status ? ' · ' + esc(s.status) : ''}
            <small>${s.destination ? 'to ' + esc(s.destination) : 'no destination broadcast'}${s.draught_m ? ' · draught ' + s.draught_m + ' m' : ''} · seen ${Math.round(s.minutes_ago)} min ago</small></li>`).join('')}</ul>`
        : `<p class="muted">${esc(live.error || 'No tankers identified yet. Ship types arrive every few minutes after the feed connects.')}</p>`}`);
    }
    parts.push(`<p class="quiet">Schedules: ${esc(ship?.source || 'Maritime Safety Queensland Qships')}. Live positions: aisstream.io AIS, coverage patchy offshore. Draught near the maximum usually means loaded.</p>`);
    el.innerHTML = parts.join('');
  }
  panel.addEventListener('click', e => {
    const b = e.target.closest('[data-ship]');
    if (!b || !window.FuelMap?.map) return;
    const [lat, lon] = b.dataset.ship.split(',').map(Number);
    window.FuelMap.map.setView([lat, lon], 13);
    window.FuelMap.map.getContainer().scrollIntoView({behavior: 'smooth', block: 'center'});
  });

  let started = false;
  async function load() {
    try { render(await (await fetch('/api/fuel-security')).json()); }
    catch { body.innerHTML = '<p class="muted">Fuel security statistics are unavailable. Restart the dashboard if it predates this section.</p>'; }
  }
  // Below the map, so it loads when it comes near the screen and refreshes hourly after that.
  new IntersectionObserver((entries, observer) => {
    if (!entries.some(e => e.isIntersecting) || started) return;
    started = true; observer.disconnect(); load(); setInterval(load, 60 * 60 * 1000);
  }, {rootMargin: '600px'}).observe(panel);
})();
