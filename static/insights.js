/* Insights: Queensland city leaderboard, when prices move, station price history and the
   favourites watchlist. Observed history only; a pattern here is not a forecast. */
(() => {
  'use strict';
  const byId = id => document.getElementById(id);
  const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
  const num = (v, d = 1) => v == null || !Number.isFinite(Number(v)) ? '—' : Number(v).toFixed(d);
  const signed = v => v == null ? '—' : `${v > 0 ? '+' : v < 0 ? '−' : '±'}${Math.abs(v).toFixed(1)}`;
  const get = async url => { const r = await fetch(url); if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); };
  const hour = h => `${h % 12 || 12} ${h < 12 ? 'am' : 'pm'}`;

  // A small two-line chart: the station (cyan) against its city's median (grey).
  function sparkline(days) {
    const pts = days.filter(d => d.price != null);
    if (pts.length < 2) return '<p class="muted">Not enough history for a chart yet.</p>';
    const values = pts.flatMap(d => [d.price, d.region_median]).filter(v => v != null);
    const lo = Math.min(...values) - 2, hi = Math.max(...values) + 2, W = 300, H = 90;
    const x = i => (i / (pts.length - 1)) * W, y = v => H - ((v - lo) / (hi - lo)) * H;
    const line = key => pts.map((d, i) => d[key] == null ? null : `${x(i).toFixed(1)},${y(d[key]).toFixed(1)}`).filter(Boolean).join(' ');
    return `<svg class="spark" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" role="img"
      aria-label="Price from ${esc(pts[0].date)} to ${esc(pts.at(-1).date)}: ${num(pts[0].price)} to ${num(pts.at(-1).price)} c/L">
      <polyline points="${line('region_median')}" fill="none" stroke="#71899c" stroke-width="1.5" stroke-dasharray="3 3"/>
      <polyline points="${line('price')}" fill="none" stroke="#58dac7" stroke-width="2"/></svg>
      <div class="spark-axis"><span>${esc(pts[0].date)}</span><span>${num(lo + 2, 0)}–${num(hi - 2, 0)} c/L</span><span>${esc(pts.at(-1).date)}</span></div>`;
  }

  let historyRequest = 0;
  async function history(el, siteId) {
    if (!el || !siteId) return;
    const run = ++historyRequest;
    el.innerHTML = '<p class="muted">Loading price history…</p>';
    try {
      const h = await get(`/api/insights/station/${encodeURIComponent(siteId)}`);
      if (run !== historyRequest) return;
      const share = h.cheap_share == null ? '' : `<p>In the cheapest quarter of ${esc(h.city || 'its area')} stations on <b>${Math.round(h.cheap_share * 100)}%</b> of ${h.cheap_days} days.</p>`;
      el.innerHTML = `<p class="kicker">PRICE HISTORY · LAST ${h.days.length} DAYS</p>${sparkline(h.days)}
        <p class="quiet">Cyan: this station. Dashed: ${esc(h.city || 'area')} median.</p>${share}`;
    } catch { if (run === historyRequest) el.innerHTML = '<p class="muted">Price history unavailable.</p>'; }
  }

  // -- Favourites watchlist (planner only: favourites live in this browser) ----------------------
  async function watchlist() {
    const box = byId('watchlist');
    if (!box) return;
    let ids = [];
    try { ids = JSON.parse(localStorage.getItem('fuel.favourites') || '[]'); } catch { /* none saved */ }
    if (!ids.length) {
      box.innerHTML = '<p class="muted">No favourites yet. Select a station on the map and choose ☆ Save favourite.</p>';
      return;
    }
    try {
      const {stations} = await get(`/api/insights/watch?ids=${encodeURIComponent(ids.join(','))}`);
      box.innerHTML = `<ul class="watch">${stations.map(s => `<li><button type="button" data-station="${esc(s.site_id)}">
        <span>★ ${esc(s.name || 'Station ' + s.site_id)}<small>${esc(s.region || '')}${s.flag ? ' · ' + esc(s.flag === 'stale' ? 'old report' : 'check price') : ''}</small></span>
        <b>${num(s.price)}<small> c/L</small></b>
        <span class="${s.change > 0 ? 'up' : s.change < 0 ? 'down' : ''}">${s.change == null ? '—' : signed(s.change)}<small>since previous day</small></span>
        <span class="${s.vs_median > 0 ? 'up' : s.vs_median < 0 ? 'down' : ''}">${signed(s.vs_median)}<small>vs city median</small></span>
        </button></li>`).join('')}</ul>`;
    } catch { box.innerHTML = '<p class="muted">Watchlist unavailable right now.</p>'; }
  }

  // -- Insights section: city leaderboard and when prices move ----------------------------------
  const panel = document.createElement('section');
  panel.className = 'panel'; panel.id = 'insights';
  panel.innerHTML = `<div class="panel-head"><h2>Insights · Queensland</h2><span class="index">OBSERVED HISTORY</span></div>
    <div class="insight-grid"><div><p class="kicker">CITIES TODAY · U91 MEDIAN, CHEAPEST FIRST</p><div class="table-scroll insight-cities"><p class="muted">Loading…</p></div></div>
    <div><p class="kicker">WHEN PRICES MOVE</p><label class="insight-city">City <select aria-label="City for timing patterns"></select></label>
    <div class="insight-timing"><p class="muted">Loading…</p></div></div></div>`;
  (byId('news') || byId('field') || byId('station-map')).after(panel);
  const nav = document.createElement('a');
  nav.href = '#insights'; nav.textContent = byId('clock') ? '09 Insights' : '10 Insights';
  if (byId('clock')) byId('clock').before(nav); else document.querySelector('nav.nav')?.append(nav);

  async function cities() {
    const {cities} = await get('/api/insights/cities');
    panel.querySelector('.insight-cities').innerHTML = `<table><thead><tr><th>City</th><th>Median</th><th>Cheapest</th><th>Spread</th><th>Diesel</th><th>Sites</th></tr></thead>
      <tbody>${cities.map(c => `<tr${c.city === 'Brisbane' ? ' class="chosen"' : ''}><td>${esc(c.city)}</td><td>${num(c.median)}</td>
        <td>${num(c.lowest.price)}<small>${esc(c.lowest.name)}</small></td><td>${num(c.spread)}</td><td>${num(c.diesel_median)}</td><td>${c.stations}</td></tr>`).join('')}</tbody></table>`;
    const select = panel.querySelector('.insight-city select');
    select.innerHTML = cities.map(c => c.city).sort().map(c => `<option${c === 'Brisbane' ? ' selected' : ''}>${esc(c)}</option>`).join('');
    select.onchange = () => timing(select.value);
  }

  async function timing(city) {
    const box = panel.querySelector('.insight-timing');
    box.innerHTML = '<p class="muted">Loading…</p>';
    const t = await get(`/api/insights/timing?city=${encodeURIComponent(city)}`);
    const peak = t.hours.length ? Math.max(...t.hours.map(h => h.hikes), 1) : 1;
    const busiest = [...t.hours].sort((a, b) => b.hikes - a.hikes).slice(0, 3).map(h => h.hour).sort((a, b) => a - b);
    const weekdays = t.pattern
      ? `<div class="bars">${t.weekdays.map(d => `<div><span>${d.day}</span><i style="width:${Math.min(100, Math.abs(d.relative || 0) * 20)}%" class="${d.relative < 0 ? 'down' : 'up'}"></i><b>${signed(d.relative)}</b></div>`).join('')}</div>
         <p class="quiet">c/L against the rest of the same week. Negative is cheaper.</p>`
      : `<p>No reliable cheapest weekday in ${esc(city)}: across ${t.weeks} full weeks, only ${t.clear_weeks} had one clearly cheapest day${t.weeks < 8 ? ', and history here is still short' : ''}. Timing a fill-up by weekday would not save money; watch the price-jump signal instead.</p>`;
    const hours = t.hours.some(h => h.hikes)
      ? `<p class="kicker">HOUR OF PRICE RISES (≥8 c/L) · ${esc(city)}</p><div class="hours">${t.hours.map(h => `<i title="${hour(h.hour)}: ${h.hikes} rises, ${h.cuts} cuts" style="height:${(h.hikes / peak) * 100}%"></i>`).join('')}</div>
         <div class="hours-axis"><span>12 am</span><span>6 am</span><span>12 pm</span><span>6 pm</span><span>11 pm</span></div>
         <p>Stations here most often raise prices around <b>${busiest.map(hour).join(', ')}</b>. Filling up before mid-morning on a day the jump signal is building avoids most same-day rises.</p>`
      : '<p class="quiet">No station-level price history for this city yet; the hour pattern needs the official report files, which cover the Brisbane area.</p>';
    box.innerHTML = weekdays + hours + `<p class="quiet">${esc(t.basis || '')}</p>`;
  }

  // Draw when the section is near the screen; it sits below the fold and is not needed at load.
  let started = false;
  new IntersectionObserver((entries, observer) => {
    if (!entries.some(e => e.isIntersecting) || started) return;
    started = true; observer.disconnect();
    cities().then(() => timing('Brisbane')).catch(() => { panel.querySelector('.insight-cities').innerHTML = '<p class="muted">Insights unavailable. Restart the dashboard if it predates this panel.</p>'; });
  }, {rootMargin: '600px'}).observe(panel);

  // Watchlist card on the planner, under the nearby list.
  const nearby = byId('nearby');
  if (nearby) {
    const card = document.createElement('section');
    card.className = 'panel'; card.id = 'watch';
    card.innerHTML = '<div class="panel-head"><h2>Watchlist · your favourites</h2><span class="index">SINCE PREVIOUS DAY</span></div><div class="panel-body" id="watchlist"></div>';
    nearby.after(card);
    card.addEventListener('click', e => { const b = e.target.closest('[data-station]'); if (b) { window.FuelDecisions?.select(b.dataset.station); byId('station-map').scrollIntoView({behavior: 'smooth'}); } });
    document.addEventListener('fuel:favourites', watchlist);
    watchlist(); setInterval(watchlist, 5 * 60 * 1000);
  }
  window.FuelInsights = {history};
})();
