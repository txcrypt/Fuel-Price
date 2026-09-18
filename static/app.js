'use strict';
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num = value => value == null ? '—' : Number(value).toFixed(1);
const count = value => Number(value || 0).toLocaleString('en-AU');
const stamp = value => value ? new Date(value.replace(' ', 'T') + (/[Z+]/.test(value) ? '' : '+10:00')).toLocaleString('en-AU', {timeZone:'Australia/Brisbane', day:'numeric',month:'short',hour:'2-digit',minute:'2-digit'}) : 'Not yet';
const dayLabel = value => new Date(value + 'T12:00:00+10:00').toLocaleDateString('en-AU',{timeZone:'Australia/Brisbane',day:'numeric',month:'short'});
let data = null, range = 90, limit = 10, loading = false;
function text(id, value) { $(id).textContent = value; }
function badge(id, value, style='') { text(id,value); $(id).className = 'pill ' + style; }
function dot(id, good, bad=false) { $(id).className = 'status-dot ' + (good ? 'good' : bad ? 'bad' : ''); }

function drawChart() {
  if (!data) return;
  const cutoff = range === 'all' ? '' : new Date(Date.now()-Number(range)*86400000).toISOString().slice(0,10);
  const history = data.history.filter(d => d.date >= cutoff);
  const forecast = data.forecast.status === 'ready' ? data.forecast.points : [];
  const all = [...history, ...forecast];
  if (!all.length) { $('chart').innerHTML = '<p class="quiet">No observations in this range. Choose All to see older records.</p>'; return; }
  const W=Math.max(320,$('chart').clientWidth),H=230,L=36,R=20,T=18,B=32;
  const ts = value => Date.parse(value+'T00:00:00Z');
  const first=ts(all[0].date), last=Math.max(ts(all.at(-1).date), first+86400000);
  const values=all.flatMap(d=>[d.price,d.low,d.high]).filter(v=>v!=null);
  const low=Math.floor((Math.min(...values)-5)/10)*10, high=Math.ceil((Math.max(...values)+5)/10)*10;
  const x=d=>L+(ts(d)-first)/(last-first)*(W-L-R), y=v=>T+(high-v)/(high-low)*(H-T-B);
  let svg=`<svg viewBox="0 0 ${W} ${H}" aria-hidden="true"><text x="${L}" y="10" fill="#81907e" font-size="9">c/L</text>`;
  for(let i=0;i<=4;i++){const value=low+(high-low)*i/4;svg+=`<line x1="${L}" x2="${W-R}" y1="${y(value)}" y2="${y(value)}" stroke="#e9ede3"/><text x="${L-9}" y="${y(value)+3}" text-anchor="end" fill="#87917f" font-size="9">${value.toFixed(0)}</text>`;}
  for(let i=0;i<=4;i++){const d=new Date(first+(last-first)*i/4).toISOString().slice(0,10);svg+=`<text x="${x(d)}" y="${H-7}" text-anchor="middle" fill="#87917f" font-size="9">${dayLabel(d)}</text>`;}
  let segments=[],segment=[];
  history.forEach((d,i)=>{if(i && ts(d.date)-ts(history[i-1].date)>86400000){segments.push(segment);segment=[];}segment.push(d);});
  if(segment.length)segments.push(segment);
  segments.forEach(s=>{svg+=`<polyline points="${s.map(d=>`${x(d.date)},${y(d.price)}`).join(' ')}" fill="none" stroke="#347359" stroke-width="2.5" stroke-linejoin="round"/>`;});
  history.forEach(d=>{svg+=`<circle cx="${x(d.date)}" cy="${y(d.price)}" r="3" fill="#347359"><title>${d.date}: ${num(d.price)} c/L · ${d.stations} stations</title></circle>`;});
  if(forecast.length){
    const valid=forecast.filter(d=>d.low!=null);
    if(valid.length)svg+=`<polygon points="${[...valid.map(d=>`${x(d.date)},${y(d.low)}`),...valid.slice().reverse().map(d=>`${x(d.date)},${y(d.high)}`)].join(' ')}" fill="#eee0bf" opacity=".6"/>`;
    const line=history.length ? [history.at(-1),...forecast] : forecast;
    svg+=`<polyline points="${line.map(d=>`${x(d.date)},${y(d.price)}`).join(' ')}" fill="none" stroke="#b18a42" stroke-width="2.5" stroke-dasharray="5 4"/>`;
    forecast.forEach(d=>{svg+=`<circle cx="${x(d.date)}" cy="${y(d.price)}" r="3" fill="#b18a42"><title>${d.date}: forecast ${num(d.price)} c/L</title></circle>`;});
  }
  if(segments.length>1)svg+=`<text x="${W/2}" y="${T+14}" text-anchor="middle" fill="#919987" font-size="10">Collection gaps are left blank</text>`;
  $('chart').innerHTML=svg+'</svg>';
  $('band-legend').hidden=!forecast.some(d=>d.low!=null);
  $('chart').setAttribute('aria-label', `${history.length} recorded days. ${forecast.length ? `Seven-day forecast ${num(forecast.at(-1).price)} cents per litre.` : 'No current forecast.'} Collection gaps are left blank.`);
}
function renderStations() {
  if(!data)return;
  const query=$('search').value.toLowerCase().trim();
  const stations=data.stations.filter(s=>`${s.name||s.site_id} ${s.region}`.toLowerCase().includes(query));
  $('station-rows').innerHTML=stations.slice(0,limit).map(s=>`<tr><td>${esc(s.name||`Station ${s.site_id}`)}</td><td><span class="area-tag">${esc(s.region||'Brisbane')}</span></td><td>${num(s.price_cpl)}</td><td class="quiet">${esc(stamp(s.reported_at ? s.reported_at.replace(/Z$/,'')+'Z' : null))}</td></tr>`).join('') || '<tr><td colspan="4">No matching stations.</td></tr>';
  text('station-result',`Showing ${Math.min(limit,stations.length)} of ${count(stations.length)} stations · lowest price first`);
  $('show-more').hidden=limit>=stations.length;
}
function render() {
  const h=data.health,f=data.forecast,s=data.summary,c=data.coverage;
  badge('connection',h.collecting?'Refreshing prices':h.fresh?'Fresh observations':'Saved observations',h.fresh?'good':'warning');
  $('refresh').disabled=h.collecting;
  const issues=[];
  if(h.error)issues.push(h.error);
  if(!h.fresh)issues.push('Prices are out of date. Showing the last saved observations.');
  if(f.status==='limited')issues.push('Forecast is a baseline: not enough continuous history for reliable model validation yet.');
  $('notice').className='notice'+(h.error?' error':issues.length?'':' hidden');text('notice',issues.join(' '));
  text('median',num(s.median));text('minimum',num(s.minimum));text('stations-count',count(s.station_count));
  text('cheapest-name',data.stations[0]?.name||'No current station data');text('price-age',`${h.fresh?'Observed':'Last observed'} ${stamp(data.latest)}`);
  const end=f.points.at(-1);text('week-price',num(end?.price));text('forecast-label',f.status==='limited'?'BASELINE':'FORECAST');
  text('week-note',end?`${dayLabel(end.date)} · ${f.model}`:'Fresh observations needed');
  badge('model-status',f.status==='ready'?'Backtested':f.status==='limited'?'Limited history':'Awaiting fresh data',f.status==='ready'?'good':'warning');
  text('forecast-reason',f.reason);text('model-name',f.model||'Not available');
  const chosen=f.scores.find(m=>m.selected);text('model-error',chosen?.mae!=null?`${chosen.mae.toFixed(2)} c/L`:'Not enough data');
  text('continuous-days',`${f.recent_consecutive_days||0} ${f.recent_consecutive_days===1?'day':'days'}`);
  $('forecast-days').innerHTML=f.status==='ready'?f.points.map(p=>`<div class="forecast-day"><span>${dayLabel(p.date)}</span><strong>${num(p.price)}</strong></div>`).join(''):'';
  text('chart-caption',f.status==='ready'?f.band_label:'Observed prices only. The unvalidated baseline is withheld from this chart. Inspect all source timelines in the Observatory.');
  const apiGood=h.api==='connected';dot('api-dot',apiGood,h.api==='error');text('api-status',({connected:'Connected',checking:'Fetching prices…',error:'Fetch failed',not_checked:'Not checked yet'})[h.api]||h.api);
  text('api-detail',apiGood?`${count(h.api_rows)} valid Brisbane U91 prices returned`:'Live API status is separate from cached prices');text('api-last',stamp(h.last_api_success));text('api-next',stamp(h.next_refresh));
  const csvGood=['recording','up_to_date'].includes(h.csv);dot('csv-dot',csvGood,h.csv==='error');text('csv-status',h.csv==='recording'?'Recording':h.csv==='up_to_date'?'Up to date':h.csv==='error'?'Write failed':'Not checked this run');
  text('csv-detail',h.csv==='recording'?`${count(h.last_csv_rows)} rows in the last append · hourly cadence`:'Original history retained · includes legacy WA rows');
  text('csv-last',stamp(data.files.csv.last_record));text('csv-rows',`${count(data.files.csv.rows)} rows · ${(data.files.csv.bytes/1048576).toFixed(1)} MB`);
  dot('db-dot',h.database==='recording',h.database==='error');text('db-status',h.database==='recording'?'Recording':h.database==='error'?'Write failed':'Saved data available');
  text('db-last',stamp(data.files.database_last));text('db-rows',count(data.files.database_rows));
  text('coverage',`${c.days} recorded days, ${c.missing_days} missing days between ${c.start||'—'} and ${c.end||'—'}.`);
  $('model-rows').innerHTML=f.scores.map(m=>`<tr class="${m.selected?'chosen':''}"><td>${esc(m.name)}${m.selected?' · current':''}</td><td>${m.mae==null?'Not yet validated':m.mae.toFixed(2)+' c/L'}</td></tr>`).join('');
  text('validation-note',f.validation_origins?`${f.validation_origins} held-out forecast origins (${f.validation_start} to ${f.validation_end}). MAE averages errors across days 1–7; overlapping origins are not independent trials.`:'Validation needs continuous stretches of observations. An untested model is not presented as more accurate.');
  const log=data.prediction_log;text('prediction-log',`${log.saved} forward predictions saved locally. ${log.scored?`${log.scored} scored · ${log.mae} c/L average error.`:'Actual errors will appear after target dates have passed.'}`);
  drawChart();renderStations();
  window.FuelDecisions?.update(data);
}
async function load() {
  if(loading)return;loading=true;
  try {
    const response=await fetch('/api/dashboard',{cache:'no-store',signal:AbortSignal.timeout(45000)});
    if(!response.ok)throw new Error('Dashboard request failed');data=await response.json();render();
  } catch(error) {
    badge('connection','Local server unavailable','bad');$('notice').className='notice error';
    text('notice','Cannot reach the local server. Start it with start-local.ps1. Any prices still visible are saved observations.');
    $('refresh').disabled=false;
  } finally{loading=false;}
}
$('refresh').addEventListener('click',async()=>{
  $('refresh').disabled=true;
  try{const r=await fetch('/api/refresh',{method:'POST'});if(!r.ok)throw new Error('Refresh failed');badge('connection','Refreshing prices','warning');setTimeout(load,1200);setTimeout(load,7000);}
  catch(e){$('notice').className='notice error';text('notice','Refresh could not be started. Check that the local server is running.');$('refresh').disabled=false;}
});
$('search').addEventListener('input',()=>{limit=10;renderStations();});
$('show-more').addEventListener('click',()=>{limit+=20;renderStations();});
document.querySelectorAll('[data-range]').forEach(button=>button.addEventListener('click',()=>{range=button.dataset.range;document.querySelectorAll('[data-range]').forEach(b=>b.classList.toggle('selected',b===button));drawChart();}));
document.querySelectorAll('nav a').forEach(a=>a.addEventListener('click',()=>{document.querySelectorAll('nav a').forEach(b=>b.classList.toggle('active',b===a));}));
// Remove the old offline cache: stale health checks must never look live.
if('serviceWorker' in navigator)navigator.serviceWorker.getRegistrations().then(regs=>regs.forEach(r=>r.unregister())).catch(()=>{});
if('caches' in window)caches.keys().then(keys=>keys.filter(k=>k.startsWith('fuel-ai-')).forEach(k=>caches.delete(k))).catch(()=>{});
load();setInterval(load,15000);
new ResizeObserver(drawChart).observe($('chart'));
