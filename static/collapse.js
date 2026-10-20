/* Every section below the map can be collapsed to its heading; the choice is remembered in this
   browser. Sections added later by other scripts (news, insights, fuel security...) are picked up too. */
(() => {
  'use strict';
  const KEY = 'fuel.collapsed';
  const anchor = document.getElementById('station-map') || document.getElementById('field');
  const main = document.querySelector('main');
  if (!anchor || !main) return;
  let collapsed;
  try { collapsed = new Set(JSON.parse(localStorage.getItem(KEY) || '[]')); } catch { collapsed = new Set(); }
  const remember = () => { try { localStorage.setItem(KEY, JSON.stringify([...collapsed])); } catch { /* private mode */ } };

  function enhance(panel) {
    if (panel.dataset.collapsible || !(anchor.compareDocumentPosition(panel) & Node.DOCUMENT_POSITION_FOLLOWING)) return;
    const head = panel.querySelector(':scope > .panel-head');
    if (!head) return;
    // Sections without an id get one from their heading, so the remembered state has a stable key.
    panel.id ||= 'section-' + (head.querySelector('h2')?.textContent || 'panel').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
    panel.dataset.collapsible = '1';
    const button = document.createElement('button');
    button.type = 'button'; button.className = 'collapse-toggle';
    button.setAttribute('aria-controls', panel.id);
    head.append(button);
    const set = shut => {
      panel.classList.toggle('collapsed', shut);
      button.setAttribute('aria-expanded', String(!shut));
      button.textContent = shut ? 'Show ▾' : 'Hide ▴';
      button.setAttribute('aria-label', `${shut ? 'Show' : 'Hide'} ${head.querySelector('h2')?.textContent || 'section'}`);
    };
    set(collapsed.has(panel.id));
    button.addEventListener('click', () => {
      const shut = !panel.classList.contains('collapsed');
      set(shut);
      if (shut) collapsed.add(panel.id); else collapsed.delete(panel.id);
      remember();
      if (!shut) {
        // Charts drawn while hidden have no width; redraw them now they are visible.
        window.dispatchEvent(new Event('resize'));
        if (typeof window.drawChart === 'function') window.drawChart();
      }
    });
  }
  const scan = () => main.querySelectorAll('section.panel').forEach(enhance);
  scan();
  // Other scripts insert their sections after load; they are direct children of <main>.
  new MutationObserver(scan).observe(main, {childList: true});
})();
