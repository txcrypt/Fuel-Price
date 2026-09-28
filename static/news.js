/* Energy news and Queensland warnings. Context only: headlines never change a forecast. */
(() => {
  'use strict';
  const byId = id => document.getElementById(id);
  const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
  const ago = iso => {
    if (!iso) return 'undated';
    const h = (Date.now() - new Date(iso)) / 36e5;
    return h < 1 ? `${Math.max(1, Math.round(h * 60))} min ago` : h < 48 ? `${Math.round(h)} h ago` : `${Math.round(h / 24)} d ago`;
  };
  const panel = document.createElement('section');
  panel.className = 'panel'; panel.id = 'news';
  panel.innerHTML = `<div class="panel-head"><h2>Energy & fuel news</h2><span class="index">08 / CONTEXT ONLY</span></div>
    <div class="news-grid"><div><div class="news-topics" role="group" aria-label="News topic"></div><ol class="news-list" aria-live="polite"><li class="muted">Loading headlines…</li></ol></div>
    <aside><p class="kicker">QUEENSLAND WARNINGS · BOM</p><ul class="news-warnings"><li class="muted">Loading…</li></ul>
    <p class="kicker">BUSHFIRE INCIDENTS · QFD</p><p class="news-fires muted">Loading…</p></aside></div>
    <p class="muted news-note"></p>`;
  // Observatory: after the live field, before the clock. Planner: after the station map, last in the nav.
  (byId('field') || byId('station-map')).after(panel);
  const nav = document.createElement('a');
  nav.href = '#news'; nav.textContent = byId('clock') ? '08 News' : '09 News';
  if (byId('clock')) byId('clock').before(nav); else document.querySelector('nav.nav')?.append(nav);

  let items = [], topic = 'All';
  function draw() {
    const shown = topic === 'All' ? items : items.filter(i => i.topic === topic);
    panel.querySelector('.news-list').innerHTML = shown.map(i => `<li><a href="${esc(i.link)}" target="_blank" rel="noopener noreferrer">${esc(i.title)}</a>
      <small>${esc(i.source)} · ${esc(i.topic)} · ${ago(i.published)}</small></li>`).join('') || '<li class="muted">No headlines available right now.</li>';
    panel.querySelector('.news-topics').innerHTML = ['All', ...new Set(items.map(i => i.topic))].map(t =>
      `<button type="button" aria-pressed="${t === topic}" data-topic="${esc(t)}">${esc(t)}</button>`).join('');
  }
  panel.querySelector('.news-topics').addEventListener('click', e => {
    const b = e.target.closest('[data-topic]'); if (b) { topic = b.dataset.topic; draw(); }
  });
  // A 404 here means the running server predates this panel; say so instead of blaming the network.
  const get = async url => {
    const r = await fetch(url);
    if (r.status === 404) throw new Error('This dashboard server is older than the news panel. Restart it (stop-local.ps1, then start-local.ps1) to load news.');
    if (!r.ok) throw new Error('The news service returned an error. It retries every 15 minutes.');
    return r.json();
  };
  async function load() {
    try {
      const data = await get('/api/news');
      items = data.items; draw();
      panel.querySelector('.news-note').textContent = data.note;
      panel.querySelector('.news-warnings').innerHTML = data.warnings.items.map(w => `<li><a href="${esc(w.link)}" target="_blank" rel="noopener noreferrer">${esc(w.title)}</a></li>`).join('')
        || '<li class="muted">No current Queensland weather warnings.</li>';
    } catch (error) {
      const why = error instanceof TypeError ? 'Cannot reach the local dashboard server.' : error.message;
      panel.querySelector('.news-list').innerHTML = `<li class="muted">${esc(why)}</li>`;
      panel.querySelector('.news-warnings').innerHTML = '<li class="muted">Unavailable.</li>';
    }
    try {
      const fires = (await get('/api/bushfires')).features.map(f => f.properties.level);
      const count = level => fires.filter(l => l === level).length;
      panel.querySelector('.news-fires').textContent = fires.length
        ? `${fires.length} current incidents: ${['Emergency Warning', 'Watch and Act', 'Advice', 'Information'].map(l => `${count(l)} ${l}`).join(' · ')}. Shown on the map (🔥 toggle).`
        : 'No current bushfire incidents.';
    } catch { panel.querySelector('.news-fires').textContent = 'Bushfire feed unavailable.'; }
  }
  load(); setInterval(load, 15 * 60 * 1000);
})();
