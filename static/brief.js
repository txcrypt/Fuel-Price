/* Today at a glance: pump, wholesale, the gap between them, crude, currency and the jump signal.
   Observed values only, each with its own date; nothing here is a forecast. */
(() => {
  'use strict';
  const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
  const num = (v, d = 1) => v == null || !Number.isFinite(Number(v)) ? '—' : Number(v).toFixed(d);
  const signed = (v, d = 1) => v == null || !Number.isFinite(Number(v)) ? '' : `${v > 0 ? '+' : v < 0 ? '−' : '±'}${Math.abs(v).toFixed(d)}`;
  const day = iso => iso ? new Date(String(iso).slice(0, 10) + 'T12:00:00+10:00').toLocaleDateString('en-AU', {day: 'numeric', month: 'short'}) : '';
  const box = document.createElement('section');
  box.className = 'brief'; box.setAttribute('aria-label', 'Today at a glance');
  (document.querySelector('.metrics') || document.querySelector('main')).after(box);
  let context = null, dashboard = null;

  function cell(label, value, note, tone = '') {
    return `<div class="brief-cell ${tone}"><small>${esc(label)}</small><b>${value}</b><span>${note}</span></div>`;
  }
  function draw() {
    if (!dashboard) return;
    const cells = [];
    const history = dashboard.history || [];
    const [prev, last] = history.slice(-2);
    if (last && prev) {
      const d = last.price - prev.price;
      cells.push(cell('PUMP MEDIAN · CHANGE', `${signed(d)} <small>c/L</small>`, `${num(last.price)} on ${esc(day(last.date))} · ${num(prev.price)} on ${esc(day(prev.date))}`, d > 0 ? 'up' : d < 0 ? 'down' : ''));
    }
    const source = id => context?.sources?.find(s => s.id === id);
    const latest = s => s?.points?.at(-1);
    const tgp = source('aip'), brent = source('eia_brent'), fx = source('rba');
    if (latest(tgp)) cells.push(cell('WHOLESALE · AIP BRISBANE', `${num(latest(tgp).value)} <small>c/L</small>`,
      `${signed(Number(tgp.change))} vs previous · ${esc(day(latest(tgp).date))}${tgp.stale ? ' · STALE' : ''}`, Number(tgp.change) > 0 ? 'up' : Number(tgp.change) < 0 ? 'down' : ''));
    const paired = context?.paired_history || [];
    if (paired.length) {
      const gaps = paired.map(p => p.gap), g = paired.at(-1);
      cells.push(cell('PUMP − WHOLESALE', `${num(g.gap)} <small>c/L</small>`,
        `${esc(day(g.date))} · range ${num(Math.min(...gaps))}–${num(Math.max(...gaps))} over ${paired.length} paired days. Costs + margin, not profit.`));
    }
    if (latest(brent)) cells.push(cell('BRENT CRUDE', `US$${num(latest(brent).value, 2)}`,
      `${signed(Number(brent.change), 2)} · ${esc(day(latest(brent).date))} (EIA runs a few days behind)`, Number(brent.change) > 0 ? 'up' : Number(brent.change) < 0 ? 'down' : ''));
    if (latest(fx)) cells.push(cell('AUD / USD', num(latest(fx).value, 4),
      `${signed(Number(fx.change), 4)} · ${esc(day(latest(fx).date))}. A weaker dollar raises import costs.`, Number(fx.change) < 0 ? 'up' : Number(fx.change) > 0 ? 'down' : ''));
    // Other grades, same Brisbane area as the headline U91 median; LPG and E85 are niche, so the common five.
    const ORDER = ['e10', 'Premium Unleaded 95', 'Premium Unleaded 98', 'Diesel', 'Premium Diesel'];
    const grades = (dashboard.grades || []).filter(g => ORDER.includes(g.fuel)).sort((a, b) => ORDER.indexOf(a.fuel) - ORDER.indexOf(b.fuel));
    if (grades.length) cells.push(cell('OTHER FUELS · MEDIAN', grades.map(g => `<span class="grade">${esc(g.fuel.replace('Premium Unleaded ', 'P').replace('Premium Diesel', 'Prem. diesel').replace('e10', 'E10'))} <b>${num(g.median)}</b></span>`).join(''),
      `c/L, ${esc(dashboard.summary?.area || 'analysis area')} · U91 ${num(dashboard.summary?.median)} for comparison`, 'wide'));
    const r = dashboard.restoration;
    if (r) cells.push(cell('PRICE-JUMP SIGNAL', `${Math.round(r.restored_share * 100)}% <small>${esc(r.level)}</small>`, esc(r.message), r.level === 'quiet' ? '' : 'up'));
    box.innerHTML = cells.length ? `<p class="kicker">TODAY AT A GLANCE · OBSERVED, NOT FORECAST</p><div class="brief-grid">${cells.join('')}</div>` : '';
  }
  async function loadContext() {
    try { const r = await fetch('/api/market-context'); if (r.ok) { context = await r.json(); draw(); } } catch { /* The strip still shows pump data. */ }
  }
  loadContext(); setInterval(loadContext, 30 * 60 * 1000);
  window.FuelBrief = {update(payload) { dashboard = payload; draw(); }};
})();
