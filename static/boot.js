/* Start-up screen: a terminal boot log over faint falling code. Shown before anything else paints and
   kept up until the first price data is on screen. The log lines are the page's real requests
   (watched through fetch), not a fake timer. Loaded without defer as the first thing in <body>. */
(() => {
  'use strict';
  const LABELS = [
    ['/api/dashboard', 'price.telemetry', 'qld fuel price reports'],
    ['/api/observatory', 'evidence.ledger', 'price changes, provenance'],
    ['/api/traffic', 'road.network', 'qldtraffic events'],
    ['/api/cameras', 'camera.grid', 'qldtraffic webcams'],
    ['/api/ships', 'maritime.ais', 'vessels, qld coast'],
    ['/api/shipping', 'port.movements', 'maritime safety qld'],
    ['/api/fuel-security', 'fuel.reserves', 'national stockholding'],
    ['/api/news', 'news.wire', 'oil and energy'],
    ['/api/market-context', 'market.context', 'crude, wholesale, aud'],
    ['/api/bushfires', 'hazard.feed', 'qfd bushfire incidents'],
  ];
  const DONE = '/api/dashboard';
  const CAP_MS = 30000;            // never trap anyone behind the screen
  const GLYPHS = '0123456789ABCDEFアイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワ';
  let seen = false;
  try { seen = sessionStorage.getItem('fuel.booted') === '1'; } catch { /* private mode */ }
  const reduced = matchMedia('(prefers-reduced-motion: reduce)').matches;
  const MIN_MS = seen || reduced ? 0 : 1200;   // first visit: long enough to read; afterwards instant
  const began = performance.now();
  const stamp = () => `[${((performance.now() - began) / 1000).toFixed(3).padStart(7)}]`;
  const now = new Date().toLocaleTimeString('en-AU', {timeZone: 'Australia/Brisbane', hour12: false});

  const el = document.createElement('div');
  el.className = 'boot';
  el.setAttribute('role', 'status');
  el.setAttribute('aria-label', 'Loading live data');
  el.innerHTML = `<canvas aria-hidden="true"></canvas>
    <div class="boot-term">
      <div class="boot-head">FUEL OBSERVATORY <span>// node BNE -27.4698 153.0251 // ${now} AEST</span></div>
      <ol class="boot-log"></ol>
      <div class="boot-progress"><span data-bar></span> <span data-pct>0%</span></div>
    </div>
    <button type="button" class="boot-skip">[esc] skip</button>`;
  document.body.prepend(el);
  document.documentElement.classList.add('booting');
  const log = el.querySelector('.boot-log'), barText = el.querySelector('[data-bar]'), pct = el.querySelector('[data-pct]');

  // Falling code behind the log. Kept dim so it reads as texture, not decoration.
  const canvas = el.querySelector('canvas'), ctx = canvas.getContext('2d');
  const COL = 16;
  let drops = [], rain = 0, last = 0;
  function size() {
    canvas.width = innerWidth; canvas.height = innerHeight;
    drops = Array.from({length: Math.ceil(innerWidth / COL)}, () => Math.random() * -innerHeight / COL);
    ctx.font = '14px ui-monospace, Consolas, monospace';
  }
  function fall(t) {
    rain = requestAnimationFrame(fall);
    if (t - last < 55) return;
    last = t;
    ctx.fillStyle = 'rgba(4,7,10,.16)';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    drops.forEach((y, i) => {
      const ch = GLYPHS[Math.random() * GLYPHS.length | 0];
      ctx.fillStyle = Math.random() < .04 ? '#d9fff7' : '#58dac7';
      ctx.fillText(ch, i * COL, y * COL);
      drops[i] = y * COL > canvas.height && Math.random() > .975 ? 0 : y + 1;
    });
  }
  if (!reduced) { size(); addEventListener('resize', size); rain = requestAnimationFrame(fall); }

  // Each new line decodes from random glyphs into its text, left to right.
  function decode(node, text) {
    if (reduced) { node.textContent = text; return; }
    const t0 = performance.now(), span = 260;
    const step = t => {
      const done = Math.floor((t - t0) / span * text.length);
      node.textContent = text.slice(0, done) + [...text.slice(done)].map(c => c === ' ' ? ' ' : GLYPHS[Math.random() * 16 | 0]).join('');
      if (done < text.length) requestAnimationFrame(step); else node.textContent = text;
    };
    requestAnimationFrame(step);
    setTimeout(() => { node.textContent = text; }, span + 100);   // frames pause in background tabs
  }
  function write(label, detail, state = '') {
    const li = document.createElement('li');
    li.className = 'boot-line ' + state;
    li.innerHTML = `<time>${stamp()}</time> <b></b><small></small><span>${state === 'ok' ? 'ok' : '…'}</span>`;
    decode(li.querySelector('b'), label);
    li.querySelector('small').textContent = detail;
    log.querySelector('.cursor')?.classList.remove('cursor');
    li.classList.add('cursor');
    log.append(li);
    while (log.children.length > 12) log.firstElementChild.remove();
    return li;
  }
  write('init session', 'brisbane metro · u91', 'ok');

  let started = 0, finished = 0, closed = false, finishing = false;
  const lines = new Set();
  function progress(share = started ? finished / started : 0) {
    const n = Math.round(share * 28);
    barText.textContent = `[${'#'.repeat(n)}${'.'.repeat(28 - n)}]`;
    pct.textContent = `${Math.round(share * 100)}%`;
  }
  progress();

  const real = window.fetch;
  window.fetch = function (input, ...rest) {
    const path = (typeof input === 'string' ? input : input?.url || '').replace(location.origin, '');
    const known = closed ? null : LABELS.find(([p]) => path.startsWith(p));
    const request = real.call(this, input, ...rest);
    if (!known || lines.has(known[0])) return request;
    lines.add(known[0]);
    const li = write(`link ${known[1]}`, known[2]);
    started++; progress();
    const t0 = performance.now();
    request.then(r => {
      li.querySelector('span').textContent = r.ok ? `ok ${Math.round(performance.now() - t0)}ms` : `err ${r.status}`;
      li.classList.add(r.ok ? 'ok' : 'bad');
      if (r.ok && path.startsWith(DONE)) setTimeout(finish, 4000);   // fallback if a page never signals
    }, () => {
      li.querySelector('span').textContent = 'no signal, retrying';
      li.classList.add('bad');
    }).finally(() => { finished++; progress(); });
    return request;
  };

  function close() {
    if (closed) return;
    closed = true;
    try { sessionStorage.setItem('fuel.booted', '1'); } catch { /* private mode */ }
    document.documentElement.classList.remove('booting');
    removeEventListener('keydown', esc);
    el.classList.add('boot-out');
    // Leaflet measured the map while it was covered; let it re-measure now it can be seen.
    window.dispatchEvent(new Event('resize'));
    setTimeout(() => { cancelAnimationFrame(rain); removeEventListener('resize', size); el.remove(); }, reduced ? 0 : 400);
  }
  function finish() {
    if (closed || finishing) return;
    finishing = true;
    progress(1);
    write('field live', 'handing over', 'ok');
    // Two frames so the first prices have painted underneath before the screen lifts.
    requestAnimationFrame(() => requestAnimationFrame(() => setTimeout(close, Math.max(0, MIN_MS - (performance.now() - began)))));
  }
  const esc = e => { if (e.key === 'Escape') close(); };
  addEventListener('keydown', esc);
  addEventListener('fuel:ready', finish);   // dispatched by observatory.js / app.js after the first render
  el.querySelector('.boot-skip').onclick = close;
  setTimeout(close, CAP_MS);
})();
