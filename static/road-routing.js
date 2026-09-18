/* Coordinates stay in this page; only an explicit calculation sends them to ORS. */
(() => {
  'use strict';
  const escape = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const number = (v, digits=1) => Number(v).toFixed(digits);
  function attach(map, {container, getPosition=()=>null}) {
    const panel=document.createElement('details'); panel.className='road-routing';
    panel.innerHTML=`<summary>Road-cost lab · openrouteservice</summary><form class="road-form">
      <p class="road-station">Select a station on the map or in the ledger.</p>
      <p class="road-status" role="status">Checking routing connection…</p>
      <div class="road-actions"><button type="button" data-pick="start">Pick start on map</button><button type="button" data-fix>Use current location / chosen point</button><button type="button" data-pick="end">Pick destination on map</button><button type="button" data-return>Return to start</button></div>
      <div class="road-inputs"><label>Start · latitude, longitude<input name="start" placeholder="e.g. -27.4705, 153.0260" required autocomplete="off"></label><label>Destination · blank = return to start<input name="end" placeholder="Optional onward destination" autocomplete="off"></label><label>Purchase (L)<input name="litres" type="number" min="0.1" max="200" step="0.1" value="40" required></label><label>Consumption (L/100 km)<input name="consumption" type="number" min="0.1" max="40" step="0.1" value="8" required></label><label>Value of time ($/hour)<input name="hourly_value" type="number" min="0" max="1000" step="1" value="0" required></label></div>
      <label class="road-tolls"><input name="avoid_tolls" type="checkbox" checked> Avoid toll roads</label>
      <p>Calculate sends the selected coordinates to openrouteservice. Coordinates are not saved to disk. A device fix is copied only when you choose it.</p>
      <button type="submit">Calculate road cost</button><div class="road-result" aria-live="polite"></div>
    </form>`;
    container.append(panel);
    const form=panel.querySelector('form'), field=name=>form.elements.namedItem(name), status=panel.querySelector('.road-status'), result=panel.querySelector('.road-result');
    const routes=L.layerGroup().addTo(map), points=L.layerGroup().addTo(map);
    let station=null, picking=null, generation=0, controller=null;
    const parse=value=>{const parts=value.split(',').map(s=>s.trim()); if(parts.length!==2 || parts.some(s=>s==='')) throw Error('Enter coordinates as latitude, longitude.'); const [latitude,longitude]=parts.map(Number); if(!Number.isFinite(latitude)||!Number.isFinite(longitude)||Math.abs(latitude)>90||Math.abs(longitude)>180) throw Error('Coordinates are outside valid latitude / longitude bounds.'); return {latitude,longitude};};
    function invalidate() {generation++;controller?.abort();controller=null;form.querySelector('[type=submit]').disabled=false;result.textContent='Inputs changed. Calculate to update the route.';routes.clearLayers();}
    function drawPoints() {points.clearLayers();for(const [name,color] of [['start','#50d5ee'],['end','#eab466']]) {try {const p=parse(field(name).value); L.circleMarker([p.latitude,p.longitude],{radius:9,color,fillOpacity:.9}).addTo(points).bindTooltip(name==='start'?'Route start':'Route destination');} catch { /* Incomplete manual entry. */ }}}
    function stopPicking() {picking=null;panel.querySelectorAll('[data-pick]').forEach(b=>b.setAttribute('aria-pressed','false'));map.getContainer().style.cursor='';}
    panel.querySelectorAll('[data-pick]').forEach(button=>button.onclick=()=>{const next=picking===button.dataset.pick?null:button.dataset.pick;stopPicking();picking=next;if(next){button.setAttribute('aria-pressed','true');map.getContainer().style.cursor='crosshair';status.textContent=`Click a road on the map to set ${next==='start'?'the start':'the destination'}.`;}});
    map.on('click',event=>{if(!picking)return;field(picking).value=`${event.latlng.lat.toFixed(6)}, ${event.latlng.lng.toFixed(6)}`;stopPicking();invalidate();drawPoints();status.textContent='Point selected. Calculate when ready.';});
    panel.querySelector('[data-return]').onclick=()=>{field('end').value='';stopPicking();invalidate();drawPoints();};
    panel.querySelector('[data-fix]').onclick=()=>{const p=getPosition();if(!p || p.stale || (p.timestamp && Date.now()-p.timestamp>120000)){status.textContent='No recent location / chosen point. Enable device location above, or pick a start on the map.';return;}field('start').value=`${p.latitude.toFixed(6)}, ${p.longitude.toFixed(6)}`;stopPicking();invalidate();drawPoints();status.textContent='Starting point copied; it will not follow subsequent device movement.';};
    form.addEventListener('input',()=>{invalidate();drawPoints();});
    form.onsubmit=async event=>{
      event.preventDefault();stopPicking();invalidate();const request=++generation;
      try {
        if(!station)throw Error('Select a station first.');
        const body={station_id:String(station.site_id),origin:parse(field('start').value),destination:field('end').value.trim()?parse(field('end').value):null,litres:Number(field('litres').value),consumption:Number(field('consumption').value),hourly_value:Number(field('hourly_value').value),avoid_tolls:field('avoid_tolls').checked};
        controller=new AbortController();form.querySelector('[type=submit]').disabled=true;result.textContent='Calculating driving routes…';
        const response=await fetch('/api/routing/compare',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body),signal:controller.signal});const r=await response.json();
        if(request!==generation)return;
        if(!response.ok)throw Error(typeof r.detail==='string'?r.detail:'Check the route inputs and try again.');
        if(r.direct)L.geoJSON(r.direct.geometry,{style:{color:'#91a2b2',weight:4,dashArray:'6 7'}}).addTo(routes);
        const route=L.geoJSON(r.via.geometry,{style:{color:'#31bacc',weight:5,opacity:.85}}).addTo(routes);map.fitBounds(route.getBounds(),{padding:[30,30],maxZoom:15});
        const row=(label,value)=>`<div><span>${label}</span><strong>${value}</strong></div>`;
        result.innerHTML=`<h4>${r.mode==='trip_detour'?'Fuel stop along your journey':'Dedicated fuel trip · there and back'}</h4><div class="road-metrics">${row('Pump price',number(r.price_cpl)+' c/L')}${row('Route via station',number(r.via.km)+' km · '+number(r.via.minutes)+' min')}${row('Direct journey',r.direct?number(r.direct.km)+' km · '+number(r.direct.minutes)+' min':'No journey otherwise · 0 km')}${row('Extra driving',number(r.extra_km)+' km · '+number(r.extra_minutes)+' min')}${row('Extra driving fuel','$'+number(r.travel_fuel_cost,2)+' · '+number(r.extra_fuel_l,2)+' L')}${row('Time value','$'+number(r.travel_time_cost,2))}${row('Fuel purchase',number(r.litres)+' L · $'+number(r.purchase_cost,2))}${row('Purchase + extra travel','$'+number(r.cost_including_travel,2))}${row('Effective price',number(r.effective_cpl)+' c/L')}</div><p>${r.fresh_prices?'':'Snapshot is stale; verify the pump price. '}${escape(r.note)}</p><p>Cyan: via station. Grey dashed: direct journey.${r.extra_km<0||r.extra_minutes<0?' Negative differences reflect alternative recommended routes; this is not a guaranteed saving.':''}</p><p>Inspect the QLDTraffic overlay for reported disruptions. These events do not adjust this route or cost estimate.</p><small>${escape(r.attribution)}</small>`;
        status.textContent='Route received · '+(r.via.cached?'reused from the five-minute memory cache':'fresh provider response');
      } catch(error) {if(request===generation&&error.name!=='AbortError')result.textContent=error.message;}
      finally {if(request===generation){form.querySelector('[type=submit]').disabled=false;controller=null;}}
    };
    fetch('/api/routing/status').then(r=>r.json()).then(r=>{if(status.textContent==='Checking routing connection…')status.textContent=r.configured?'Server key configured · calculate a route to verify.':'Routing key is not configured.';}).catch(()=>{if(status.textContent==='Checking routing connection…')status.textContent='Routing status unavailable.';});
    return {selectStation(value){const changed=station?.site_id!==value?.site_id||station?.price_cpl!==value?.price_cpl;station=value||null;panel.querySelector('.road-station').textContent=station?`${station.name} · ${number(station.price_cpl)} c/L`:'Select a station on the map or in the ledger.';if(changed)invalidate();}};
  }
  window.FuelRouting={attach};
})();
