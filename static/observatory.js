'use strict';
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '—').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num = (v, digits = 1) => v == null || !Number.isFinite(Number(v)) ? '—' : Number(v).toLocaleString('en-AU', {minimumFractionDigits:digits, maximumFractionDigits:digits});
const signed = v => v == null ? '—' : `${v > 0 ? '+' : ''}${num(v)}`;
const stamp = value => {
  if (!value) return NaN;
  let s = String(value).replace(' ', 'T');
  if (s.length === 10) s += 'T12:00:00';
  if (!/(Z|[+-]\d\d:\d\d)$/.test(s)) s += '+10:00';
  return new Date(s).getTime();
};
const date = (value, time = false) => Number.isFinite(stamp(value)) ? new Intl.DateTimeFormat('en-AU', {timeZone:'Australia/Brisbane', day:'2-digit', month:'short', year:'numeric', ...(time ? {hour:'2-digit', minute:'2-digit', hour12:false} : {})}).format(stamp(value)) : '—';
const ageHours = s => Math.max(0, (Date.now() - stamp(s.reported_at)) / 3600000);
const ageText = s => {const h = ageHours(s); return !Number.isFinite(h) ? 'Unknown' : h < 24 ? `${num(h)} h` : `${num(h / 24)} d`;};
let data, evidence, selected, series = 'daily', map, layer, rawOffset = 0, rawTotal = 0, stationRequest = 0, rawRequest = 0, loading = false, firstFit = false;
let roadRouter, devicePosition;
let changes = new Map(), markers = new Map();
const metric = (label, value, unit, note, tone='') => `<article class="metric"><div class="metric-label">${label}</div><div class="metric-value ${tone}">${value}<small>${unit}</small></div><p class="metric-note">${esc(note)}</p></article>`;
async function get(url) {const r = await fetch(url); if (!r.ok) throw new Error(`Request failed (${r.status})`); return r.json();}
const quantile = (values, q) => {if (!values.length) return null; const i = (values.length - 1) * q, low = Math.floor(i); return values[low] + (values[Math.ceil(i)] - values[low]) * (i - low);};
function bars(items) {const max = Math.max(1, ...items.map(i => i[1])); return items.map(([label, count, color]) => `<div class="bar-row"><span>${esc(label)}</span><div class="bar"><i style="width:${100*count/max}%;background:${color || 'var(--cyan)'}"></i></div><span>${count}</span></div>`).join('');}
function renderMetrics() {
  const s = data.summary, values = data.stations.map(s => s.price_cpl).sort((a,b)=>a-b);
  $('metrics').innerHTML = metric('MARKET MEDIAN',num(s.median),'c/L','Equal weight per station','cyan') + metric('LOW / HIGH',`${num(s.minimum)} <small>/ ${num(s.maximum)}</small>`,'','Observed metro range') + metric('PRICE SPREAD',num(s.maximum == null ? null : s.maximum-s.minimum),'c/L',`$${num((s.maximum-s.minimum)*.5,2)} range on 50 L`,'amber') + metric('LIVE STATIONS',num(s.station_count,0),'sites',`Matched comparison: ${changes.size}`) + metric('OBSERVED HISTORY',num(data.coverage.days,0),'days',`${data.coverage.missing_days} unobserved days in span`) + metric('SOURCE RECORDS',num(evidence.audit.rows,0),'rows',`${num(evidence.audit.brisbane_rows,0)} Brisbane rows`);
  const low = Math.floor((s.minimum || 80) / 10) * 10, high = Math.ceil((s.maximum || 90) / 10) * 10;
  const bins = []; for (let p = low; p <= high; p += 10) {const count=values.filter(v=>v>=p && v<p+10).length; if (count || p < high) bins.push([`${p}–${p+10}`,count,p+10 <= s.median ? 'var(--cyan)' : 'var(--amber)']);}
  $('distribution').innerHTML = bars(bins);
  $('percentiles').innerHTML = [[.1,'P10'],[.25,'P25'],[.75,'P75'],[.9,'P90']].map(([q,l])=>`<div><span>${l} · c/L</span><b>${num(quantile(values,q))}</b></div>`).join('');
  const deltas = [...changes.values()].map(c=>c.change);
  $('movement').innerHTML = bars([['Fell',deltas.filter(c=>c<0).length,'var(--cyan)'],['Flat',deltas.filter(c=>c===0).length,'#71899c'],['Rose',deltas.filter(c=>c>0).length,'var(--red)']]);
  $('comparison').textContent = `Live vs CSV capture ${date(evidence.previous_capture,true)}. Only ${changes.size} matched stations; this is not a daily change.`;
  $('report-age').innerHTML = bars([['< 24 h',data.stations.filter(s=>ageHours(s)<24).length],['1–7 d',data.stations.filter(s=>ageHours(s)>=24 && ageHours(s)<168).length,'var(--amber)'],['> 7 d',data.stations.filter(s=>ageHours(s)>=168).length,'var(--red)'],['Unknown',data.stations.filter(s=>!Number.isFinite(ageHours(s))).length,'#71899c']]);
}
function filtered() {const q=$('search').value.trim().toLowerCase(); return data.stations.filter(s=>`${s.name} ${s.site_id} ${s.region}`.toLowerCase().includes(q));}
function color(s) {
  const mode=$('map-mode').value;
  if(mode==='change') {const c=changes.get(String(s.site_id))?.change; return c==null?'#71899c':c<0?'#58dac7':c>0?'#f67e83':'#91a2b2';}
  if(mode==='age') {const a=ageHours(s);return !Number.isFinite(a)?'#71899c':a<24?'#58dac7':a<168?'#eab466':'#f67e83';}
  const v=data.stations.map(s=>s.price_cpl).sort((a,b)=>a-b);return s.price_cpl<=quantile(v,.25)?'#58dac7':s.price_cpl>=quantile(v,.75)?'#f67e83':'#eab466';
}
function initMap() {
  if (!window.L) {$('map').textContent='Map library unavailable. Station records remain available in the ledger.';return;}
  map=L.map('map',{preferCanvas:true,scrollWheelZoom:false}).setView([-27.47,153.03],10);
  const tiles=L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'}).addTo(map);
  tiles.on('tileerror',()=>{$('map-legend').textContent='Basemap unavailable · station coordinates still shown';});
  layer=L.layerGroup().addTo(map);
  new ResizeObserver(()=>map.invalidateSize({pan:false})).observe($('map'));
  window.FuelLocation?.attach(map,{container:document.querySelector('.map-tools'),onPosition:p=>{devicePosition=p;}});
  window.FuelTraffic?.attach(map,{container:document.querySelector('.map-panel')});
  roadRouter=window.FuelRouting?.attach(map,{container:document.querySelector('.map-panel'),getPosition:()=>devicePosition});
}
function drawMap() {
  if(!map)return;
  layer.clearLayers(); markers.clear(); const stations=filtered();
  stations.forEach(s=>{if(!Number.isFinite(s.latitude)||!Number.isFinite(s.longitude))return;
    const marker=L.circleMarker([s.latitude,s.longitude],{radius:String(s.site_id)===selected?8:5,weight:String(s.site_id)===selected?2:1,color:String(s.site_id)===selected?'#fff':color(s),fillColor:color(s),fillOpacity:.85}).addTo(layer);
    marker.bindTooltip(`${esc(s.name || s.site_id)} · ${num(s.price_cpl)} c/L`).on('click',()=>inspect(String(s.site_id)));markers.set(String(s.site_id),marker);
  });
  $('map-count').textContent=`${markers.size} / ${data.stations.length} SITES`;
  $('map-legend').textContent = {price:'CYAN ≤ P25 · AMBER middle · RED ≥ P75',change:'CYAN fell · RED rose · GREY flat / unmatched',age:'CYAN < 24 h · AMBER 1–7 d · RED > 7 d'}[$('map-mode').value];
  if(!firstFit&&markers.size){fitMap();firstFit=true;}
}
function fitMap(){if(map&&markers.size)map.fitBounds(L.latLngBounds([...markers.values()].map(m=>m.getLatLng())),{padding:[24,24],maxZoom:12});}
function renderLedger(){
  const stations=filtered(), mode=$('sort').value;
  stations.sort((a,b)=>mode==='price-desc'?b.price_cpl-a.price_cpl:mode==='change'?Math.abs(changes.get(String(b.site_id))?.change||0)-Math.abs(changes.get(String(a.site_id))?.change||0):mode==='age'?(ageHours(b)||0)-(ageHours(a)||0):mode==='name'?String(a.name).localeCompare(String(b.name)):a.price_cpl-b.price_cpl);
  $('ledger-count').textContent=`${stations.length} VISIBLE / ALL ROWS SCROLLABLE`;
  $('station-rows').innerHTML=stations.map(s=>{const c=changes.get(String(s.site_id))?.change;return `<tr><td><button class="station-select" data-site="${esc(s.site_id)}">${esc(s.name || 'Unnamed station')}<small>${esc(s.site_id)}</small></button></td><td>${esc(s.region)}</td><td class="cyan">${num(s.price_cpl)}</td><td class="${c>0?'red':c<0?'cyan':''}">${signed(c)}</td><td>${signed(s.price_cpl-data.summary.median)}</td><td>${ageText(s)}</td><td>${date(s.reported_at,true)}</td></tr>`;}).join('');
}
async function inspect(id){
  selected=id;const s=data.stations.find(s=>String(s.site_id)===id);if(!s)return;
  const request=++stationRequest, c=changes.get(id)?.change;
  $('inspector').innerHTML=`<div><p class="kicker">SITE ${esc(id)} / ${esc(s.region)}</p><h3 class="station-title">${esc(s.name||'Unnamed station')}</h3><div class="station-price">${num(s.price_cpl)} <small>c/L</small></div></div><dl class="station-facts"><dt>Vs metro median</dt><dd>${signed(s.price_cpl-data.summary.median)} c/L</dd><dt>Since prior capture</dt><dd>${signed(c)} c/L</dd><dt>Price report age</dt><dd>${ageText(s)}</dd><dt>Coordinates</dt><dd>${num(s.latitude,4)}, ${num(s.longitude,4)}</dd></dl><div class="mini-chart" id="station-chart"></div><p class="muted" id="station-evidence">Loading capture history…</p><div class="actions"><button id="inspect-raw">Inspect source rows</button><a href="/planner#buying-plan">Buying lab ↗</a><a target="_blank" rel="noopener" href="https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(s.latitude+','+s.longitude)}">Directions ↗</a></div>`;
  $('inspect-raw').onclick=()=>{$('raw-source').value='live';$('raw-state').value='QLD';$('raw-site').value=id;rawOffset=0;loadRows();$('raw').scrollIntoView({behavior:'smooth'});};
  $('inspector').querySelector('a[href="/planner#buying-plan"]').href=`/planner?station=${encodeURIComponent(id)}#buying-plan`;
  window.FuelSources?.inspect(id);
  roadRouter?.selectStation(s);
  drawMap();markers.get(id)?.openTooltip();
  try {const history=await get(`/api/observatory/station/${encodeURIComponent(id)}`);if(request!==stationRequest)return;
    plot($('station-chart'),history.observations,{mini:true});
    $('station-evidence').textContent=`${history.rows||0} source rows · ${history.observations.length} captures · ${history.reports.length} distinct reports. Mini-chart shows collected prices, with gaps left open.`;
  }catch(e){if(request===stationRequest)$('station-evidence').textContent=e.message;}
}
function plot(element, points, {mini=false, forecast=[], events=false}={}) {
  const valid=points.filter(p=>Number.isFinite(stamp(p.date))&&Number.isFinite(p.price));
  if(!valid.length){element.innerHTML='<p class="muted">No observations in this window.</p>';return;}
  const all=[...valid,...forecast], width=Math.max(260,element.clientWidth),height=mini?115:element.clientHeight,left=mini?30:43,right=18,top=18,bottom=mini?22:33;
  let start=Math.min(...all.map(p=>stamp(p.date))),end=Math.max(...all.map(p=>stamp(p.date)));
  if(start===end){start-=43200000;end+=43200000;}
  const prices=all.flatMap(p=>[p.price,p.low,p.high].filter(v=>v!=null&&Number.isFinite(v)));
  const step=Math.max(5,Math.ceil((Math.max(...prices)-Math.min(...prices))/(mini?3:5)/5)*5);
  const low=Math.floor((Math.min(...prices)-2)/step)*step,high=Math.ceil((Math.max(...prices)+2)/step)*step;
  const x=t=>left+(stamp(t)-start)/(end-start)*(width-left-right),y=p=>top+(high-p)/(high-low)*(height-top-bottom);
  let svg=`<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${events?'Station report events':'Observed prices'} in cents per litre over time">`;
  for(let v=low;v<=high;v+=step)svg+=`<line x1="${left}" x2="${width-right}" y1="${y(v)}" y2="${y(v)}" stroke="#24333f"/><text x="${left-7}" y="${y(v)+3}" text-anchor="end">${v}</text>`;
  const ticks=mini?2:Math.max(2,Math.min(7,Math.floor(width/135)));
  for(let i=0;i<ticks;i++){const t=start+(end-start)*i/(ticks-1),label=new Intl.DateTimeFormat('en-AU',{timeZone:'Australia/Brisbane',day:'numeric',month:'short',...(end-start>180*86400000?{year:'2-digit'}:{})}).format(t);svg+=`<text x="${left+(width-left-right)*i/(ticks-1)}" y="${height-6}" text-anchor="${i===0?'start':i===ticks-1?'end':'middle'}">${esc(label)}</text>`;}
  const segments=[];let segment=[];
  valid.forEach((p,i)=>{if(i&&stamp(p.date)-stamp(valid[i-1].date)>1.5*86400000){segments.push(segment);segment=[];}segment.push(p);});segments.push(segment);
  if(!events)segments.forEach(group=>{svg+=`<polyline points="${group.map(p=>`${x(p.date)},${y(p.price)}`).join(' ')}" fill="none" stroke="#58dac7" stroke-width="${mini?1.5:2}"/>`;});
  valid.forEach(p=>{const label=`${date(p.date,series==='captures')} · ${num(p.price)} c/L${p.stations!=null?' · '+p.stations+' stations':''}`;svg+=`<circle cx="${x(p.date)}" cy="${y(p.price)}" r="${mini?2:3.5}" fill="${events?'#eab466':'#58dac7'}" ${mini?'':`tabindex="0" data-point="${esc(label)}" aria-label="${esc(label)}"`}><title>${esc(label)}</title></circle>`;});
  if(forecast.length){const joined=[valid[valid.length-1],...forecast];svg+=`<polyline points="${joined.map(p=>`${x(p.date)},${y(p.price)}`).join(' ')}" fill="none" stroke="#eab466" stroke-width="2" stroke-dasharray="5 4"/>`;forecast.forEach(p=>{if(p.low!=null&&p.high!=null)svg+=`<line x1="${x(p.date)}" x2="${x(p.date)}" y1="${y(p.low)}" y2="${y(p.high)}" stroke="#eab466" stroke-opacity=".5" stroke-width="6"/>`;svg+=`<circle cx="${x(p.date)}" cy="${y(p.price)}" r="3" fill="#eab466"><title>${date(p.date)} · ${num(p.price)} c/L · ${esc(data.forecast.model)}${data.forecast.status!=='ready'?' (unvalidated baseline)':''}</title></circle>`;});}
  element.innerHTML=svg+'</svg>';
  if(!mini)element.querySelectorAll('[data-point]').forEach(p=>{const show=()=>{$('chart-readout').textContent=p.dataset.point;};p.addEventListener('pointerenter',show);p.addEventListener('focus',show);});
}
function drawHistory(){
  if(!data||!evidence)return;
  let points=series==='daily'?data.history:evidence[series];
  const range=$('range').value, latest=stamp(data.latest), cutoff=latest-Number(range)*86400000;
  if(range!=='all')points=points.filter(p=>stamp(p.date)>=cutoff);
  const desc={daily:'COLLECTED DAILY MEDIAN / CSV + database. Last observation per station per day; each station has equal weight. Lines break across missing days.',captures:'CAPTURE MEDIAN / live collection CSV only. One point per capture; coverage can vary. Missing days are not interpolated.',events:'RETROSPECTIVE REPORT EVENTS / live collection CSV. Median of each station’s last report per report date. Only reporting stations count. This is not a complete historical market snapshot.',archive_events:'LEGACY REPORT ARCHIVE / brisbane_fuel_history_clean.csv. Report-date medians, with varying station coverage and no collection timestamps. Excluded from forecast training.'};
  const show=$('baseline').checked && series==='daily';$('baseline').disabled=series!=='daily';
  $('chart-description').textContent=desc[series]+(show?` Amber: ${data.forecast.status==='ready'?'selected forecast with historical error bars':'UNVALIDATED persistence baseline, not evidence of a flat future'}.`:'')+` ${points.length} points in view.`;
  $('chart-readout').textContent='Point at a mark to inspect its date, price and station count.';
  plot($('history-chart'),points,{events:series.includes('events'),forecast:show?data.forecast.points:[]});
}
function renderCalendar(){
  const history=data.history, days=new Map(history.map(p=>[p.date,p]));
  if(!history.length){$('calendar').innerHTML='';return;}
  const end=stamp(history.at(-1).date);let html='';
  for(let t=stamp(history[0].date);t<=end;t+=86400000){const d=new Date(t+10*3600000).toISOString().slice(0,10),p=days.get(d);html+=`<span class="${p?'observed':''}" title="${d}: ${p?p.stations+' stations · '+num(p.price)+' c/L':'No collection'}"></span>`;}
  $('calendar').innerHTML=html;
  $('coverage-note').textContent=`${date(data.coverage.start)} — ${date(data.coverage.end)} · ${data.coverage.days} observed days / ${data.coverage.missing_days} missing days. Source CSV: ${evidence.audit.captures||0} capture batches. Empty days are unknown, not unchanged prices.`;
}
function renderEvidence(){
  const h=data.health,a=evidence.audit,f=data.forecast;
  $('pipeline').innerHTML=[['QLD API',h.api],['CSV APPEND',h.csv],['SQLITE',h.database]].map(([k,v])=>`<div><small>${k}</small><b class="${['connected','recording','up_to_date'].includes(v)?'cyan':'amber'}">${esc(v)}</b></div>`).join('');
  const audit=[['Active source',a.source||'No collection file'],['All source rows',num(a.rows,0)],['States',Object.entries(a.states||{}).map(([s,n])=>`${s} ${num(n,0)}`).join(' / ')],['Brisbane rows / excluded',`${num(a.brisbane_rows,0)} / ${num(a.excluded_rows,0)}`],['Distinct station report events',num(a.unique_reports,0)],['CSV capture days / batches',`${num(a.capture_days,0)} / ${num(a.captures,0)}`],['Invalid capture / report dates',`${num(a.invalid_capture_dates,0)} / ${num(a.invalid_report_dates,0)}`],['Legacy report archive rows',num(a.archive_rows,0)],['CSV file size',`${num((a.bytes||0)/1048576,2)} MB`],['Last CSV append',date(h.last_csv_write,true)],['Last API success',date(h.last_api_success,true)],['Next API attempt',date(h.next_refresh,true)],['API / CSV interval',`${h.refresh_seconds/60} / ${h.csv_interval_seconds/60} minutes`],['Database observations',num(data.files.database_rows,0)]];
  $('audit').innerHTML=audit.map(([k,v])=>`<span>${esc(k)}</span><b>${esc(v)}</b>`).join('');
  $('model-details').innerHTML=`<div class="model-heading"><b>${esc(f.model||'No model')}</b><span class="tag ${f.status==='ready'?'':'warn'}">${esc(f.status).toUpperCase()}</span></div><p>${esc(f.reason)}</p><div class="audit"><span>Continuous recent history</span><b>${f.recent_consecutive_days||0} days</b><span>Selection / held-out origins</span><b>${f.selection_origins||0} / ${f.validation_origins||0}</b><span>Forward predictions saved / scored</span><b>${data.prediction_log.saved} / ${data.prediction_log.scored}</b><span>Forward MAE</span><b>${num(data.prediction_log.mae,2)} c/L</b></div><p class="muted">Training uses observed daily prices only. Repeated CSV rows and retrospective report dates do not create missing market observations. A flat persistence baseline is a reference, not evidence that prices will stay flat.</p><div class="table-scroll model-scores"><table><thead><tr><th>Candidate</th><th>Selection MAE</th><th>Held-out MAE</th></tr></thead><tbody>${(f.scores||[]).map(s=>`<tr><td>${esc(s.name)}${s.selected?' *':''}</td><td>${num(s.selection_mae,2)}</td><td>${num(s.mae,2)}</td></tr>`).join('')}</tbody></table></div><p class="muted">MAE in c/L · unavailable scores remain blank (—). ${esc(f.band_label||'')}</p><div class="actions"><a href="/api/forecast" target="_blank" rel="noopener">Forecast JSON ↗</a><a href="/planner#buying-plan">Stress-test a purchase ↗</a></div>`;
}
async function loadRows(){
  const req=++rawRequest;const params=new URLSearchParams({source:$('raw-source').value,site_id:$('raw-site').value.trim(),state:$('raw-state').value,offset:rawOffset,limit:50});
  try{const result=await get('/api/observatory/rows?'+params);if(req!==rawRequest)return;rawTotal=result.total;
    $('raw-head').innerHTML='<tr>'+result.columns.map(c=>`<th>${esc(c)}</th>`).join('')+'</tr>';
    $('raw-rows').innerHTML=result.rows.map(row=>'<tr>'+result.columns.map(c=>`<td>${esc(row[c])}</td>`).join('')+'</tr>').join('');
    $('raw-count').textContent=`${rawTotal?rawOffset+1:0}–${Math.min(rawOffset+50,rawTotal)} of ${num(rawTotal,0)} rows`;$('raw-prev').disabled=rawOffset===0;$('raw-next').disabled=rawOffset+50>=rawTotal;
  }catch(e){if(req===rawRequest)$('raw-count').textContent=e.message;}
}
async function load(){
  if(loading)return;loading=true;
  try {const next=await get('/api/dashboard');const changed=!data||next.latest!==data.latest||next.files.csv.bytes!==data.files.csv.bytes;
    if(changed||!evidence)evidence=await get('/api/observatory');data=next;
    changes=new Map(evidence.changes.map(c=>[c.site_id,c]));
    $('connection').textContent=data.health.fresh&&data.health.api==='connected'?'● API CONNECTED':'● CHECK TELEMETRY';$('connection').className='tag'+(data.health.fresh&&data.health.api==='connected'?'':' warn');
    $('snapshot-time').textContent=`SNAPSHOT ${date(data.latest,true)} AEST · AGE ${num(data.health.snapshot_age_minutes)} MIN`;
    $('notice').textContent=data.health.error?`COLLECTOR ERROR / ${data.health.error}`:data.health.collecting?'COLLECTOR RUNNING / saved observations remain visible':!data.health.fresh?'STALE SNAPSHOT / prices are saved observations; inspect collector health below.':`COVERAGE / ${data.coverage.days} observed days. ${data.coverage.missing_days} missing days in the historical span. ${data.forecast.status==='ready'?'Model validation available.':'Forecast validation insufficient; baseline hidden by default.'}`;
    $('refresh').disabled=data.health.collecting;
    if(changed){renderMetrics();drawMap();renderLedger();drawHistory();renderCalendar();if(selected)inspect(selected);else if(data.stations.length)inspect(String(data.stations[0].site_id));}
    renderEvidence();
  }catch(e){$('connection').textContent='● CONNECTION ERROR';$('connection').className='tag warn';$('notice').textContent=`Could not update the console: ${e.message}. Previously displayed values may be stale.`;}
  finally{loading=false;}
}
$('search').addEventListener('input',()=>{if(data){drawMap();renderLedger();}});$('map-mode').onchange=()=>data&&drawMap();$('fit').onclick=fitMap;$('sort').onchange=()=>data&&renderLedger();
$('station-rows').onclick=e=>{const b=e.target.closest('[data-site]');if(b){inspect(b.dataset.site);$('field').scrollIntoView({behavior:'smooth'});}};
$('series-tabs').onclick=e=>{const b=e.target.closest('[data-series]');if(!b)return;series=b.dataset.series;document.querySelectorAll('[data-series]').forEach(t=>t.classList.toggle('active',t===b));drawHistory();};
$('range').onchange=drawHistory;$('baseline').onchange=drawHistory;
$('raw-form').onsubmit=e=>{e.preventDefault();rawOffset=0;loadRows();};$('raw-prev').onclick=()=>{rawOffset=Math.max(0,rawOffset-50);loadRows();};$('raw-next').onclick=()=>{rawOffset+=50;loadRows();};
$('refresh').onclick=async()=>{const b=$('refresh');b.disabled=true;$('notice').textContent='Requesting a new API capture…';try{const r=await fetch('/api/refresh',{method:'POST'});if(!r.ok)throw new Error(`HTTP ${r.status}`);const result=await r.json();$('notice').textContent=result.message;setTimeout(load,1500);}catch(e){$('notice').textContent=e.message;}finally{b.disabled=false;}};
let resizeTimer;window.addEventListener('resize',()=>{clearTimeout(resizeTimer);resizeTimer=setTimeout(()=>{map?.invalidateSize();drawHistory();},200);});
if('serviceWorker'in navigator)navigator.serviceWorker.getRegistrations().then(rs=>rs.forEach(r=>r.unregister())).catch(()=>{});
initMap();load();loadRows();setInterval(load,15000);setInterval(()=>{$('clock').textContent=new Intl.DateTimeFormat('en-AU',{timeZone:'Australia/Brisbane',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}).format(new Date())+' AEST';},1000);
