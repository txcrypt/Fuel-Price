/* Month replay (observatory map): play a calendar month of Brisbane prices one day at a time.
   Each dot is a station coloured by its price that day on one scale for the whole month (teal cheap,
   red dear); a white ring marks stations that put their price up that day. Jumps are marked on the chart. */
(() => {
  'use strict';
  const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
  const panel = document.querySelector('.map-panel');
  if (!panel || typeof markers === 'undefined') return;
  const RAISED = 5;   // c/L up on the day before counts as "put their price up today"
  const STOPS = [[63, 191, 174], [234, 180, 102], [246, 89, 95]];   // teal, amber, red
  const MS_PER_DAY = 700;
  const colour = t => {
    const x = Math.min(1, Math.max(0, t)) * 2, i = Math.min(1, Math.floor(x)), f = x - i;
    return `rgb(${STOPS[i].map((c, k) => Math.round(c + (STOPS[i + 1][k] - c) * f)).join(',')})`;
  };
  const brisbaneMonth = () => new Date().toLocaleDateString('en-CA', {timeZone: 'Australia/Brisbane'}).slice(0, 7);
  const shift = (month, by) => { const d = new Date(month + '-15'); d.setMonth(d.getMonth() + by); return d.toISOString().slice(0, 7); };
  const longDate = (iso, opts) => new Date(iso + 'T12:00:00+10:00').toLocaleDateString('en-AU', opts);

  const box = document.createElement('details');
  box.className = 'replay';
  box.innerHTML = `<summary>Month replay · play a month of Brisbane prices day by day</summary>
    <div class="replay-body">
      <div class="replay-nav"><button type="button" data-step="-1" aria-label="Previous month">‹</button>
        <b data-month>…</b><button type="button" data-step="1" aria-label="Next month">›</button>
        <button type="button" data-play>❚❚ Pause</button></div>
      <input type="range" min="0" max="0" value="0" aria-label="Day of the month">
      <p class="quiet">Each dot is a station, coloured by its price that day: teal is cheap, red is dear, on one scale for the whole month.
      A white ring means that station put its price up that day. Brisbane prices drift down for weeks, then most stations
      jump together within a day or two (dashed lines on the chart). Official Queensland price reports for the Brisbane area.</p>
    </div>`;
  panel.querySelector('.map-footer').after(box);
  const slider = box.querySelector('input[type=range]'), play = box.querySelector('[data-play]'), label = box.querySelector('[data-month]');
  const [back, forward] = box.querySelectorAll('[data-step]');
  // known[i]: each station's latest price up to day i. Coverage has gaps (the collector was off, or the
  // history import ends), so a station keeps its last price rather than vanishing or looking unchanged.
  let month = null, data = null, days = [], known = [], before = null, most = 1, scale = [0, 1], timer = null, hud = null, request = 0;

  function showHud() {
    if (hud || typeof map === 'undefined') return;
    hud = L.control({position: 'bottomleft'});
    hud.onAdd = () => { const el = L.DomUtil.create('div', 'replay-hud'); L.DomEvent.disableClickPropagation(el); return el; };
    hud.addTo(map);
    map.getContainer().classList.add('replaying');
  }
  function hideHud() {
    hud?.remove(); hud = null;
    if (typeof map !== 'undefined') map.getContainer().classList.remove('replaying');
  }

  function chart(index) {
    const total = new Date(month.slice(0, 4), month.slice(5), 0).getDate(), W = 230, H = 54;
    const values = days.map(f => f.median), lo = Math.min(...values) - 2, hi = Math.max(...values) + 2;
    const x = iso => (Number(iso.slice(8)) - 1) / (total - 1) * W, y = v => H - (v - lo) / (hi - lo) * H;
    const line = list => list.map(f => `${x(f.date).toFixed(1)},${y(f.median).toFixed(1)}`).join(' ');
    const jumps = data.jumps.map(j => `<line x1="${x(j.date)}" x2="${x(j.date)}" y1="0" y2="${H}" stroke="#f6595f" stroke-dasharray="3 3"/>
      <text x="${Math.min(x(j.date) + 3, W - 52)}" y="9" fill="#f6595f" font-size="9">jump +${Math.round(j.rise)}</text>`).join('');
    const now = days[index];
    return `<svg viewBox="0 0 ${W} ${H + 12}" class="replay-chart" aria-hidden="true">${jumps}
      <polyline points="${line(days)}" fill="none" stroke="#4d6070" stroke-width="1.5"/>
      <polyline points="${line(days.slice(0, index + 1))}" fill="none" stroke="#dce6ed" stroke-width="2.5"/>
      <circle cx="${x(now.date)}" cy="${y(now.median)}" r="4" fill="#dce6ed"/>
      <text x="0" y="${H + 11}" fill="#92a5b7" font-size="9">city median, c/L · 1st</text>
      <text x="${W}" y="${H + 11}" fill="#92a5b7" font-size="9" text-anchor="end">${total}th</text></svg>`;
  }

  function apply() {
    if (!data || !box.open || !days.length) return;
    const i = Number(slider.value), f = days[i], prev = i ? days[i - 1] : before, last = i ? known[i - 1] : before?.prices || {};
    let raised = 0;
    for (const [id, marker] of markers) {
      const price = known[i][id], fresh = id in f.prices;
      if (price == null) {
        marker.setStyle({radius: 2, color: '#4b5d6c', fillColor: '#4b5d6c', fillOpacity: .4, opacity: .4, weight: 0});
        continue;
      }
      const up = fresh && last[id] != null && price - last[id] >= RAISED;
      if (up) raised++;
      // No new report today: drawn faded at its last known price.
      marker.setStyle({radius: up ? 7 : 4.5, color: up ? '#ffffff' : '#0b141d', weight: up ? 2 : 1, opacity: fresh ? 1 : .35,
                       fillColor: colour((price - scale[0]) / (scale[1] - scale[0])), fillOpacity: fresh ? .95 : .3});
      if (up) marker.bringToFront?.();
    }
    const reporting = Object.keys(f.prices).length, thin = reporting < most * .3;
    const change = prev ? f.median - prev.median : null, jump = data.jumps.some(j => j.date === f.date);
    const total = new Date(month.slice(0, 4), month.slice(5), 0).getDate();
    showHud();
    hud.getContainer().innerHTML = `<div class="replay-date">${esc(longDate(f.date, {weekday: 'short', day: 'numeric', month: 'short', year: 'numeric'}))}</div>
      <div class="replay-where">day ${Number(f.date.slice(8))} of ${total}${jump ? ' · <b class="jump">PRICE JUMP</b>' : ''}</div>
      <div class="replay-share"><b>${f.median.toFixed(1)}</b> c/L city median
        ${change == null ? '' : `<span class="${change >= 0.05 ? 'up' : change <= -0.05 ? 'down' : ''}">${change >= 0.05 ? '▲' : change <= -0.05 ? '▼' : '•'} ${Math.abs(change).toFixed(1)} on yesterday</span>`}</div>
      <div class="replay-where">${raised ? `<b>${raised}</b> station${raised === 1 ? '' : 's'} put their price up today` : 'No station put its price up today'}</div>
      <div class="replay-where">${reporting} stations reported${thin ? ' · <b class="thin">thin data, median less reliable</b>' : ''}</div>
      ${chart(i)}
      <div class="replay-scale"><span>${scale[0].toFixed(0)}</span><i></i><span>${scale[1].toFixed(0)} c/L</span></div>
      <div class="replay-key"><span><i class="ring"></i>raised today</span><span>teal cheap · red dear</span></div>`;
  }

  function stop() { clearInterval(timer); timer = null; play.textContent = '▶ Play'; }
  function start() {
    stop();
    if (Number(slider.value) >= Number(slider.max)) slider.value = 0;
    play.textContent = '❚❚ Pause';
    timer = setInterval(() => {
      if (Number(slider.value) >= Number(slider.max)) return stop();
      slider.value = Number(slider.value) + 1; apply();
    }, MS_PER_DAY);
  }

  async function load(next, fallback = true) {
    stop();
    const mine = ++request;
    label.textContent = longDate(next + '-15', {month: 'long', year: 'numeric'}) + '…';
    try {
      const r = await fetch(`/api/insights/month?month=${next}`);
      if (!r.ok) throw new Error(r.status);
      const got = await r.json();
      if (mine !== request) return;
      // Nothing yet for this month (prices arrive a day late): open the latest month that has data.
      if (!got.frames.some(f => f.date.startsWith(next)) && fallback && got.months[1] && got.months[1] !== next) return load(got.months[1], false);
      month = next; data = got;
      days = got.frames.filter(f => f.date.startsWith(month));
      before = got.frames.find(f => !f.date.startsWith(month)) || null;
      let running = {...before?.prices};
      known = days.map(f => (running = {...running, ...f.prices}));
      most = Math.max(1, ...days.map(f => Object.keys(f.prices).length));
      const all = days.flatMap(f => Object.values(f.prices)).sort((a, b) => a - b);
      scale = all.length ? [all[Math.floor(all.length * .05)], all[Math.floor(all.length * .95)]] : [0, 1];
      if (scale[1] - scale[0] < 1) scale[1] = scale[0] + 1;
      const toDate = month === brisbaneMonth();
      label.textContent = longDate(month + '-15', {month: 'long', year: 'numeric'}) + (toDate ? ' · to date' : '');
      back.disabled = !got.months[0] || month <= got.months[0];
      forward.disabled = !got.months[1] || month >= got.months[1];
      slider.max = Math.max(0, days.length - 1); slider.value = 0;
      if (!days.length) { hideHud(); label.textContent += ' · no data'; return; }
      apply(); start();   // plays straight away: the story is in the motion
    } catch {
      if (mine === request) { hideHud(); label.textContent = 'Replay unavailable'; }
    }
  }

  box.addEventListener('toggle', () => {
    if (!box.open) { stop(); hideHud(); if (typeof drawMap === 'function') drawMap(); return; }   // normal map back
    if (typeof map !== 'undefined') map.setView([-27.47, 153.03], 10);
    load(month || brisbaneMonth());
  });
  back.onclick = () => month && load(shift(month, -1), false);
  forward.onclick = () => month && load(shift(month, 1), false);
  slider.oninput = () => { stop(); apply(); };
  play.onclick = () => (timer ? stop() : start());
  // The page redraws markers when new prices arrive; keep the replay's colours while it is open.
  window.FuelReplay = {reapply: apply};
})();
