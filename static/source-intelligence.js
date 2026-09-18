/* Additional evidence remains distinguishable from validated forecast inputs. */
(() => {
  const byId = id => document.getElementById(id);
  const escape = value => String(value ?? '—').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const number = (value, digits=1) => value == null ? '—' : Number(value).toLocaleString('en-AU',{minimumFractionDigits:digits,maximumFractionDigits:digits});
  const signed = (value, digits=1) => value == null ? '—' : `${value>0?'+':''}${number(value,digits)}`;
  let sourceData, contextData, busy=false, currentSite;
  const panel = document.createElement('section');
  panel.className = 'panel';
  panel.id = 'sources';
  panel.innerHTML = `
    <div class="panel-head"><h2>Source intelligence / API coverage</h2><span class="index">07 / EVIDENCE BEFORE INFERENCE</span></div>
    <div class="panel-body">
      <div id="source-status" class="muted" role="status">Loading upstream coverage and free public context…</div>
      <div id="context-cards" class="context-cards"></div>
      <p id="context-note" class="muted"></p>
      <div class="table-scroll"><table><thead><tr><th>Matched date</th><th>Retail median c/L</th><th>AIP wholesale c/L</th><th>Gap c/L</th></tr></thead><tbody id="paired-context"></tbody></table></div>
      <hr><div id="api-coverage" class="pipeline"></div>
      <p class="muted">QLD API prices use 9999 for unavailable fuel. Sites with no U91 listing are a separate category. Other grades are retained for comparison; they do not enter the U91 forecast. An old report date alone does not prove a price is wrong.</p>
      <div class="source-columns"><div><h3>Brand comparison / available U91</h3><div class="table-scroll"><table><thead><tr><th>Brand</th><th>Sites</th><th>Median c/L</th><th>Minimum c/L</th></tr></thead><tbody id="brand-comparison"></tbody></table></div><p class="muted">Current snapshot only. Geography and station mix can explain differences; these are not causal brand effects.</p></div>
      <div><h3>U91 availability exceptions</h3><div class="table-scroll"><table><thead><tr><th>Site</th><th>Status</th><th>Reported (UTC)</th></tr></thead><tbody id="availability-exceptions"></tbody></table></div></div></div>
      <details><summary>Inspect every returned API field and metadata freshness</summary><div id="reference-status" class="audit"></div><div class="table-scroll"><table><thead><tr><th>Endpoint</th><th>Returned field</th><th>Populated / rows</th></tr></thead><tbody id="api-fields"></tbody></table></div><p class="muted">Every returned field is retained in the local JSON capture. Extra fields without a documented meaning are preserved without inferred interpretation. No opening-hours claims are inferred from undocumented or empty fields.</p></details>
      <div class="actions"><a href="/api/source-evidence" target="_blank" rel="noopener">Full metro API evidence ↗</a><a href="/api/export/api" target="_blank" rel="noopener">Download complete QLD capture ↗</a><a href="/api/market-context" target="_blank" rel="noopener">Context data & provenance ↗</a><a href="https://www.fuelpricesqld.com.au/documents/FuelPricesQLDDirectAPI(OUT)v1.6.pdf" target="_blank" rel="noopener">Official API definitions ↗</a></div>
      <hr><h3>Free data acquisition priorities</h3><div class="table-scroll"><table><thead><tr><th>Priority / source</th><th>Useful conclusion to test</th><th>Access / integration status</th></tr></thead><tbody>
        <tr><td><a href="https://www.data.qld.gov.au/dataset/fuel-price-reporting-2026" target="_blank" rel="noopener">1. Queensland monthly history</a></td><td>Price-cycle timing and station leadership across collection gaps</td><td>Free CSV / CC BY 4.0 · Backfill not imported yet</td></tr>
        <tr><td><a href="https://aip.com.au/pricing/terminal-gate-prices" target="_blank" rel="noopener">2. AIP wholesale ULP</a></td><td>Wholesale direction and retail–wholesale gaps</td><td>Integrated public table · See live status above</td></tr>
        <tr><td><a href="https://www.rba.gov.au/statistics/frequency/exchange-rates.html" target="_blank" rel="noopener">3. RBA AUD/USD</a></td><td>Currency contribution to imported fuel cost</td><td>Integrated public table · No key</td></tr>
        <tr><td><a href="https://openrouteservice.org/plans/" target="_blank" rel="noopener">4. openrouteservice</a></td><td>Actual extra road distance and travel time to cheaper stations</td><td>Connected via local server key · On-demand route comparison in the map’s Road-cost lab · Provider quotas apply</td></tr>
        <tr><td><a href="https://qldtraffic.qld.gov.au/more/Developers-and-Data/index.html" target="_blank" rel="noopener">5. QLDTraffic</a></td><td>Roadworks, closures and disruption near a planned fuel stop</td><td>Integrated event overlay · Shared public key / five-minute server cache · See QLDTraffic panel for live availability</td></tr>
        <tr><td><a href="https://www.eia.gov/opendata/documentation.php" target="_blank" rel="noopener">6. US EIA Brent</a></td><td>Broader oil-cost pressure; test lagged effects</td><td>Free API / registration key · Not connected</td></tr>
        <tr><td><a href="https://www.energy.gov.au/energy-data/australian-petroleum-statistics" target="_blank" rel="noopener">7. Australian Petroleum Statistics</a></td><td>Stocks, imports and refinery output over longer periods</td><td>Free monthly data extracts · Not connected</td></tr>
      </tbody></table></div>
      <p class="muted">The relevant refined-petrol benchmark is Singapore Mogas 95; Brent is only a broader proxy. A reliable free daily Mogas API has not been verified. Historical backfills need report-date reconstruction, coverage checks and release dates before model testing. <a href="https://www.accc.gov.au/consumers/petrol-and-fuel/what-affects-fuel-prices" target="_blank" rel="noopener">ACCC: fuel-price drivers ↗</a></p>
    </div>`;
  byId('raw').before(panel);
  const nav = document.createElement('a');
  nav.href = '#sources'; nav.textContent = '07 Sources';
  byId('clock').before(nav);

  function inspect(id) {
    currentSite = id;
    const site = sourceData?.sites.find(s => s.site_id === id);
    byId('station-api')?.remove();
    if (!site || !byId('inspector')) return;
    const box = document.createElement('div');
    box.id = 'station-api';
    box.innerHTML = `<hr><p class="kicker">UPSTREAM STATION EVIDENCE</p><p>${escape(site.brand)} · ${escape(site.suburb)} ${escape(site.postcode)}</p><p class="muted">${escape(site.address)}<br>API capture: ${escape(sourceData.retrieved_at)} AEST</p><div class="table-scroll"><table><thead><tr><th>Grade</th><th>c/L / status</th></tr></thead><tbody>${site.fuels.map(p => `<tr><td>${escape(p.fuel)}</td><td>${p.status==='available'?number(p.price_cpl):escape(p.status.replaceAll('_',' '))}</td></tr>`).join('')}</tbody></table></div><details><summary>Raw metadata & report provenance</summary><p class="muted">Site modification: ${escape(site.metadata_modified_at)} (API value). Report timestamps below are UTC.</p><pre>${escape(JSON.stringify({site:site.raw_site,prices:site.fuels},null,2))}</pre></details>`;
    byId('inspector').append(box);
  }

  function render() {
    const coverage = sourceData.coverage;
    byId('source-status').textContent = sourceData.status==='recorded'
      ? `QLD capture: ${sourceData.retrieved_at} AEST · ${number(coverage.qld_sites,0)} sites / ${number(coverage.qld_price_rows,0)} price rows. Endpoint coverage and cache freshness are shown below.`
      : 'Extended API evidence is waiting for the next successful capture.';
    byId('api-coverage').innerHTML = [['METRO SITES',coverage.metro_sites],['AVAILABLE U91',coverage.available],['UNAVAILABLE U91',coverage.unavailable],['NO U91 LISTING',coverage.not_listed],['ALL-GRADE PRICE ROWS',coverage.metro_price_rows],['DISTINCT EVENTS SAVED',coverage.price_events_saved]].map(([label,value])=>`<div><small>${label}</small><b>${number(value,0)}</b></div>`).join('');
    const brands = new Map();
    sourceData.sites.forEach(s=>{const price=s.fuels.find(p=>p.fuel_id===2 && p.status==='available'); if(price){if(!brands.has(s.brand))brands.set(s.brand,[]);brands.get(s.brand).push(price.price_cpl);}});
    const ranking = [...brands].map(([name,prices])=>{prices.sort((a,b)=>a-b);const i=Math.floor(prices.length/2);return {name,count:prices.length,median:prices.length%2?prices[i]:(prices[i-1]+prices[i])/2,min:prices[0]};}).sort((a,b)=>a.median-b.median);
    byId('brand-comparison').innerHTML = ranking.map(b=>`<tr><td>${escape(b.name)}</td><td>${b.count}</td><td>${number(b.median)}</td><td>${number(b.min)}</td></tr>`).join('');
    byId('availability-exceptions').innerHTML = sourceData.sites.filter(s=>s.u91_status!=='available').map(s=>`<tr><td>${escape(s.name)}<small>${escape(s.suburb)} · ${s.site_id}</small></td><td>${escape(s.u91_status.replaceAll('_',' '))}</td><td>${escape(s.fuels.find(p=>p.fuel_id===2)?.reported_at)}</td></tr>`).join('');
    byId('api-fields').innerHTML = Object.entries(sourceData.fields).flatMap(([name,fields])=>fields.map(f=>`<tr><td>${escape(name)}</td><td>${escape(f.field)}</td><td>${f.populated} / ${f.rows}</td></tr>`)).join('');
    byId('reference-status').innerHTML = Object.entries(sourceData.references).map(([name,r])=>`<span>${escape(name)} · ${escape(r.status)}</span><b>${escape(r.retrieved_at)}${r.error?' · '+escape(r.error):''}</b>`).join('');
    byId('context-cards').innerHTML = contextData.sources.map(s=>{const latest=s.points.at(-1);return `<article><p class="kicker">${escape(s.name)}</p><strong>${number(latest?.value,s.id==='rba'?4:1)}</strong><small>${escape(s.unit)}</small><p>${signed(s.change,s.id==='rba'?4:1)} since previous published observation</p><p class="muted">Source date: ${escape(latest?.date)} · ${escape(s.status)}${s.stale?' / STALE':''}<br>Retrieved: ${escape(s.retrieved_at)} AEST<br>${escape(s.error||s.purpose)}</p><a href="${escape(s.url)}" target="_blank" rel="noopener">${escape(s.kind)} / source ↗</a></article>`;}).join('');
    byId('context-note').textContent = `${contextData.model_use} ${contextData.gap_note} Sources checked every ${contextData.refresh_seconds/3600} hours; failures retain visibly dated data.`;
    byId('paired-context').innerHTML = contextData.paired_history.slice(-20).reverse().map(p=>`<tr><td>${p.date}</td><td>${number(p.retail)}</td><td>${number(p.wholesale)}</td><td>${signed(p.gap)}</td></tr>`).join('') || '<tr><td colspan="4">No matching observation dates yet.</td></tr>';
    if (currentSite) inspect(currentSite);
  }

  async function load() {
    if (busy) return; busy=true;
    try {
      const responses=await Promise.all(['/api/source-evidence','/api/market-context'].map(async url=>{const r=await fetch(url);if(!r.ok)throw new Error(`HTTP ${r.status}`);return r.json();}));
      [sourceData,contextData]=responses;
      render();
    } catch (error) { byId('source-status').textContent=`Source evidence could not refresh (${error.message}). Displayed values may be stale.`; }
    finally { busy=false; }
  }
  window.FuelSources={inspect};
  load();setInterval(load,60000);
})();
