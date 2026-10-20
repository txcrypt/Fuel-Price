/* Phone alerts panel (planner): ntfy topic, price-jump alert and target prices for favourites. */
(() => {
  'use strict';
  const byId = id => document.getElementById(id);
  const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));
  const anchor = byId('watch') || byId('nearby');
  if (!anchor) return;
  const panel = document.createElement('section');
  panel.className = 'panel'; panel.id = 'alerts';
  panel.innerHTML = `<div class="panel-head"><h2>Phone alerts</h2><span class="index">FREE · VIA NTFY</span></div>
    <div class="panel-body alerts-body"><p class="muted">Loading…</p></div>`;
  anchor.after(panel);
  const body = panel.querySelector('.alerts-body');
  let settings = null;

  async function render() {
    try { settings = await (await fetch('/api/alerts')).json(); }
    catch { body.innerHTML = '<p class="muted">Alerts are unavailable. Restart the dashboard if it predates this panel.</p>'; return; }
    let ids = [];
    try { ids = JSON.parse(localStorage.getItem('fuel.favourites') || '[]'); } catch { /* none */ }
    let favourites = [];
    if (ids.length) try { favourites = (await (await fetch(`/api/insights/watch?ids=${encodeURIComponent(ids.join(','))}`)).json()).stations; } catch { /* listed without prices */ }
    const target = id => settings.watches.find(w => w.site_id === id)?.below;
    const topic = settings.topic || settings.suggested_topic;
    body.innerHTML = `
      <ol class="alert-steps"><li>Install <b>ntfy</b> from the App Store (free, no account).</li>
        <li>In ntfy tap <b>+</b> and subscribe to this topic: <code class="alert-topic">${esc(topic)}</code> <button type="button" class="text-button" data-copy>copy</button></li>
        <li>Choose what to hear about below, then <b>Save</b> and <b>Send test</b>.</li></ol>
      <label class="alert-row"><input type="checkbox" data-jump ${settings.jump ? 'checked' : ''}> Tell me when a citywide price jump starts building, and again when it is imminent</label>
      <p class="kicker">TARGET PRICES · YOUR FAVOURITES</p>
      ${favourites.length ? `<div class="alert-watch">${favourites.map(f => `<label><span>★ ${esc(f.name || f.site_id)}<small>now ${f.price == null ? '—' : f.price.toFixed(1)} c/L</small></span>
        <input type="number" inputmode="decimal" step="0.1" min="80" max="350" placeholder="off" aria-label="Alert at or below, c/L"
          data-site="${esc(f.site_id)}" data-name="${esc(f.name || '')}" value="${target(f.site_id) ?? ''}"></label>`).join('')}</div>
        <p class="quiet">Leave blank for no alert. A target fires once and re-arms when the price goes back above it.</p>`
        : '<p class="muted">Save favourite stations on the map to set target prices for them.</p>'}
      <details><summary>Advanced</summary><label class="alert-row">Topic <input type="text" data-topic value="${esc(topic)}" autocomplete="off" spellcheck="false"></label>
        <label class="alert-row">Open when tapped <input type="url" data-link placeholder="${esc(location.origin)}/planner" value="${esc(settings.link)}"></label></details>
      <div class="alert-actions"><button type="button" data-save>Save</button><button type="button" class="secondary" data-test ${settings.topic ? '' : 'disabled'}>Send test</button>
        <span class="alert-status" role="status"></span></div>
      <p class="quiet">Anyone who knows the topic name can read its alerts, which contain station names and prices. Keep the random name private.</p>`;
  }

  body.addEventListener('click', async e => {
    const status = body.querySelector('.alert-status');
    if (e.target.closest('[data-copy]')) {
      try { await navigator.clipboard.writeText(body.querySelector('[data-topic]').value); status.textContent = 'Topic copied.'; }
      catch { status.textContent = 'Copy failed; select the topic and copy it.'; }
    }
    if (e.target.closest('[data-save]')) {
      const watches = [...body.querySelectorAll('[data-site]')].filter(i => i.value !== '')
        .map(i => ({site_id: i.dataset.site, name: i.dataset.name, below: Number(i.value)}));
      const payload = {topic: body.querySelector('[data-topic]').value.trim(), jump: body.querySelector('[data-jump]').checked,
                       watches, link: body.querySelector('[data-link]').value.trim()};
      try {
        const r = await fetch('/api/alerts', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
        const result = await r.json();
        if (!r.ok) throw new Error(typeof result.detail === 'string' ? result.detail : 'Check the values.');
        await render();
        body.querySelector('.alert-status').textContent = `Saved: ${watches.length} target${watches.length === 1 ? '' : 's'}${payload.jump ? ' + price-jump alert' : ''}.`;
      } catch (error) { status.textContent = `Not saved: ${error.message}`; }
    }
    if (e.target.closest('[data-test]')) {
      try {
        const r = await fetch('/api/alerts/test', {method: 'POST'});
        status.textContent = r.ok ? 'Test sent. It should appear in ntfy within seconds.' : `Test failed: ${(await r.json()).detail}`;
      } catch { status.textContent = 'Test failed: the dashboard could not be reached.'; }
    }
  });
  document.addEventListener('fuel:favourites', render);
  render();
})();
