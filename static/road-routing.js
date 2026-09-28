/* Coordinates stay in this page; only an explicit calculation sends them to ORS. */
(() => {
  'use strict';
  const escape = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const number = (v, digits=1) => Number(v).toFixed(digits);
  function attach(map, {container, getPosition=()=>null, onAdopt=null, onInvalidate=null, onSelectStation=null}) {
    const panel=document.createElement('details'); panel.className='road-routing';
    panel.innerHTML=`<style>.road-adopt{margin:.6rem 0;padding:.6rem;border:1px solid currentColor;border-radius:6px;opacity:.95}.road-rank-table{width:100%;border-collapse:collapse;font-size:.85em;margin:.5rem 0}.road-rank-table th,.road-rank-table td{border-bottom:1px solid currentColor;padding:.25rem .4rem;text-align:left}.road-rank-table tbody tr:first-child{font-weight:700}.road-disruptions{margin:.6rem 0;padding:.6rem;border:1px dashed currentColor;border-radius:6px;font-size:.9em}.road-disruptions ul{margin:.3rem 0 .6rem;padding-left:1.1rem}.road-disruptions li{margin:.35rem 0}.road-disruptions h5,.road-disruptions h6{margin:.5rem 0 .2rem}.road-disruptions .road-caveats{opacity:.85;font-size:.92em}</style><summary>Road-cost lab · openrouteservice</summary><form class="road-form">
      <p class="road-station">Select a station on the map or in the ledger.</p>
      <p class="road-status" role="status">Checking routing connection…</p>
      <div class="road-actions"><button type="button" data-pick="start">Pick start on map</button><button type="button" data-fix>Use current location / chosen point</button><button type="button" data-pick="end">Pick destination on map</button><button type="button" data-return>Return to start</button></div>
      <div class="road-inputs"><label>Start · latitude, longitude<input name="start" placeholder="e.g. -27.4705, 153.0260" required autocomplete="off"></label><label>Destination · blank = return to start<input name="end" placeholder="Optional onward destination" autocomplete="off"></label><label>Purchase (L)<input name="litres" type="number" min="0.1" max="200" step="0.1" value="40" required></label><label>Consumption (L/100 km)<input name="consumption" type="number" min="0.1" max="40" step="0.1" value="8" required></label><label>Value of time ($/hour)<input name="hourly_value" type="number" min="0" max="1000" step="1" value="0" required></label></div>
      <div class="road-inputs"><label>Which trip is this?<select name="leg"><option value="later_refill">Only a later refill makes this trip</option><option value="both_purchases">Both options make this trip</option></select></label></div>
      <label class="road-tolls"><input name="avoid_tolls" type="checkbox" checked> Avoid toll roads</label>
      <p>Calculate sends the selected coordinates to openrouteservice. Coordinates are not saved to disk. A device fix is copied only when you choose it.</p>
      <button type="submit">Calculate road cost</button><div class="road-result" aria-live="polite"></div>
      <div class="road-inputs"><label>Shortlist size<input name="limit" type="number" min="2" max="8" step="1" value="5"></label><label>Shortlist radius (km)<input name="radius_km" type="number" min="1" max="50" step="1" value="8"></label></div>
      <button type="button" data-rank>Rank cheapest total trip</button>
      <p>Ranking routes one journey per shortlisted station, plus one shared direct journey. It never routes every station in the city.</p>
      <div class="road-rank" aria-live="polite"></div>
    </form>`;
    container.append(panel);
    const form=panel.querySelector('form'), field=name=>form.elements.namedItem(name), status=panel.querySelector('.road-status'), result=panel.querySelector('.road-result'), ranking=panel.querySelector('.road-rank');
    const routes=L.layerGroup().addTo(map), points=L.layerGroup().addTo(map);
    let station=null, picking=null, generation=0, controller=null;
    const parse=value=>{const parts=value.split(',').map(s=>s.trim()); if(parts.length!==2 || parts.some(s=>s==='')) throw Error('Enter coordinates as latitude, longitude.'); const [latitude,longitude]=parts.map(Number); if(!Number.isFinite(latitude)||!Number.isFinite(longitude)||Math.abs(latitude)>90||Math.abs(longitude)>180) throw Error('Coordinates are outside valid latitude / longitude bounds.'); return {latitude,longitude};};
    // Any change to the station, points, volume, price or trip choice means the last
    // route priced a different journey; an adopted plan cost must not outlive it.
    function invalidate() {generation++;controller?.abort();controller=null;form.querySelector('[type=submit]').disabled=false;result.textContent='Inputs changed. Calculate to update the route.';ranking.textContent='';routes.clearLayers();onInvalidate?.();}
    // Only kilometres and minutes cross into the plan. The planner values that fuel and
    // time from its own consumption and time value, so no travel cost is charged twice.
    function adoptBlock(a) {
      if(!onAdopt||!a)return '';
      const quantities=a.extra_km||a.extra_minutes?`<p>Adds <strong>${number(a.extra_km)} km</strong> and <strong>${number(a.extra_minutes,0)} min</strong> of extra travel to the deferred plan. No dollar figure is carried across: the planner re-prices that distance and time itself.</p>`:'<p>Adds no extra travel to the comparison.</p>';
      return `<div class="road-adopt"><h5>Use this route in the buying plan</h5><p>${escape(a.trip)}</p><p>${escape(a.applies_to)}</p>${quantities}${a.notes.map(n=>`<p>${escape(n)}</p>`).join('')}${a.adoptable?'<button type="button" data-adopt>Use this route in the plan</button>':`<p><strong>This route cannot be used in the plan.</strong> ${a.blocked.map(escape).join(' ')}</p>`}</div>`;
    }
    // QLDTraffic events whose geometry touches the corridor. Reporting only: the kilometres,
    // minutes and dollars above are the provider's route figures and are never adjusted here.
    const GROUPS={on_corridor:'On the route line',area_alert:'Area alerts covering the route',nearby:'Nearby, not established to be on your route'};
    function disruptionList(events) {
      return Object.entries(GROUPS).map(([match,heading])=>{
        const list=events.filter(e=>e.match===match);
        if(!list.length)return '';
        return `<h6>${heading} · ${list.length}</h6><ul>${list.map(e=>`<li><strong>${escape(e.event_type)}${e.event_subtype?' · '+escape(e.event_subtype):''}</strong> — ${escape(e.road_name||e.locality||'road not named')}${match==='on_corridor'?'':` · ${e.metres} m from the route line`}<br>${escape(e.impact_type||'Impact not stated')}${e.impact_subtype?' · '+escape(e.impact_subtype):''} · ${escape(e.direction||'direction not stated')}${e.delay_text?`<br>Provider wording: “${escape(e.delay_text)}” — text as supplied, not a measured delay.`:''}<br>${escape(e.schedule_note)}</li>`).join('')}</ul>`;
      }).join('');
    }
    function renderDisruptions(box,d) {
      if(!d.usable) {box.innerHTML='<p>No usable route line to check against QLDTraffic.</p>';return;}
      const feed=d.feed.status==='unavailable'?'No QLDTraffic feed has been retrieved, so nothing was checked.'
        :`QLDTraffic feed ${d.feed.stale?'retained and STALE':'current'}, fetched ${new Date(d.feed.retrieved_at).toLocaleString('en-AU',{timeZone:'Australia/Brisbane',dateStyle:'short',timeStyle:'short'})} · ${d.feed.metro_events} Brisbane-area events searched.`;
      box.innerHTML=`<h5>Reported disruptions near this route</h5><p>${escape(d.headline)} Corridor tolerance ${d.corridor_m} m; anything out to ${d.nearby_m} m is listed as nearby.</p>
        <p>${escape(feed)}</p>${disruptionList(d.events)}
        ${d.counts.expired?`<p>${d.counts.expired} matched event(s) are past their stated end and are not listed.</p>`:''}
        <div class="road-caveats">${d.notes.map(n=>`<p>${escape(n)}</p>`).join('')}</div>`;
    }
    async function checkCorridor(geometry,at) {
      const box=result.querySelector('.road-disruptions');
      if(!box)return;
      box.textContent='Checking QLDTraffic for reported events along this corridor…';
      try {
        const response=await fetch('/api/traffic/route',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(geometry)});
        const d=await response.json();
        if(at!==generation)return;
        if(!response.ok)throw Error('The corridor check could not be completed.');
        renderDisruptions(box,d);
      } catch { if(at===generation)box.textContent='The QLDTraffic corridor check is unavailable, so no reported event was checked against this route. That is not evidence the road is clear.'; }
    }
    function drawPoints() {points.clearLayers();for(const [name,color] of [['start','#50d5ee'],['end','#eab466']]) {try {const p=parse(field(name).value); L.circleMarker([p.latitude,p.longitude],{radius:9,color,fillOpacity:.9}).addTo(points).bindTooltip(name==='start'?'Route start':'Route destination');} catch { /* Incomplete manual entry. */ }}}
    function stopPicking() {picking=null;panel.querySelectorAll('[data-pick]').forEach(b=>b.setAttribute('aria-pressed','false'));map.getContainer().style.cursor='';}
    panel.querySelectorAll('[data-pick]').forEach(button=>button.onclick=()=>{const next=picking===button.dataset.pick?null:button.dataset.pick;stopPicking();picking=next;if(next){button.setAttribute('aria-pressed','true');map.getContainer().style.cursor='crosshair';status.textContent=`Click a road on the map to set ${next==='start'?'the start':'the destination'}.`;}});
    map.on('click',event=>{if(!picking)return;field(picking).value=`${event.latlng.lat.toFixed(6)}, ${event.latlng.lng.toFixed(6)}`;stopPicking();invalidate();drawPoints();status.textContent='Point selected. Calculate when ready.';});
    panel.querySelector('[data-return]').onclick=()=>{field('end').value='';stopPicking();invalidate();drawPoints();};
    panel.querySelector('[data-fix]').onclick=()=>{const p=getPosition();if(!p || p.stale || (p.timestamp && Date.now()-p.timestamp>120000)){status.textContent='No recent location / chosen point. Enable device location above, or pick a start on the map.';return;}field('start').value=`${p.latitude.toFixed(6)}, ${p.longitude.toFixed(6)}`;stopPicking();invalidate();drawPoints();status.textContent='Starting point copied; it will not follow subsequent device movement.';};
    form.addEventListener('input',event=>{if(['limit','radius_km'].includes(event.target.name)){ranking.textContent='Shortlist changed. Rank again to update it.';return;}invalidate();drawPoints();});
    function renderRank(r) {
      const cell=x=>onSelectStation?`<button type="button" data-station="${escape(x.station_id)}">${escape(x.station_name)}</button>`:escape(x.station_name);
      ranking.innerHTML=`<h4>${escape(r.headline)}</h4>
        <p>${escape(r.shortlist_rule)} ${r.mode==='trip_detour'?'Each candidate is a detour from the same direct journey':'Each candidate is a dedicated return trip'}, buying ${number(r.litres)} L at ${number(r.consumption)} L/100 km with time valued at $${number(r.hourly_value,2)}/hour.${r.avoid_tolls?' Toll roads avoided.':' Toll roads allowed; toll charges are still not priced.'}</p>
        <p>${r.ranks_disagree?'<strong>Pump-price rank and total-trip rank disagree.</strong> The lowest advertised price is not the cheapest once the driving is included.':'Pump-price rank and total-trip rank agree across this shortlist.'}</p>
        <table class="road-rank-table"><thead><tr><th scope="col">Total rank</th><th scope="col">Station</th><th scope="col">Purchase + extra travel</th><th scope="col">Effective c/L</th><th scope="col">Pump c/L</th><th scope="col">Pump rank</th><th scope="col">Extra travel</th></tr></thead><tbody>${r.results.map(x=>`<tr><th scope="row">${x.total_rank}</th><td>${cell(x)}</td><td>$${number(x.cost_including_travel,2)}</td><td>${number(x.effective_cpl)}</td><td>${number(x.price_cpl)}</td><td>${x.pump_rank}</td><td>${number(x.extra_km)} km · ${number(x.extra_minutes,0)} min</td></tr>`).join('')}</tbody></table>
        ${r.partial?`<p><strong>Partial ranking.</strong> ${r.evaluated} of ${r.requested} candidates were priced.${r.quota_stopped?' Routing stopped early to protect the request allowance: '+escape(r.quota_stopped):''}</p><ul>${r.failures.map(f=>`<li>${escape(f.name||f.station_id)}: ${escape(f.reason)}</li>`).join('')}</ul>`:''}
        ${r.baseline?`<p>Shared direct journey: ${number(r.baseline.km)} km · ${number(r.baseline.minutes)} min.${r.baseline_consistent?'':' Candidates did not share one baseline journey; treat this comparison with caution.'}</p>`:''}
        ${r.fresh_prices?'':'<p>Snapshot is stale; verify the pump prices before acting.</p>'}
        <p>${escape(r.note)}</p><small>${escape(r.attribution)}</small>`;
      ranking.querySelectorAll('[data-station]').forEach(button=>button.onclick=()=>onSelectStation(button.dataset.station));
    }
    panel.querySelector('[data-rank]').onclick=async event=>{
      const button=event.currentTarget;
      try {
        stopPicking();
        const body={origin:parse(field('start').value),destination:field('end').value.trim()?parse(field('end').value):null,
                    litres:Number(field('litres').value),consumption:Number(field('consumption').value),
                    hourly_value:Number(field('hourly_value').value),avoid_tolls:field('avoid_tolls').checked,
                    limit:Number(field('limit').value),radius_km:Number(field('radius_km').value)};
        button.disabled=true;ranking.textContent='Routing the shortlist one station at a time…';
        const response=await fetch('/api/routing/rank',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
        const r=await response.json();
        if(!response.ok)throw Error(typeof r.detail==='string'?r.detail:'The shortlist could not be ranked.');
        renderRank(r);
      } catch(error) {ranking.textContent=error.message;}
      finally {button.disabled=false;}
    };
    form.onsubmit=async event=>{
      event.preventDefault();stopPicking();invalidate();const request=++generation;
      try {
        if(!station)throw Error('Select a station first.');
        const body={station_id:String(station.site_id),origin:parse(field('start').value),destination:field('end').value.trim()?parse(field('end').value):null,litres:Number(field('litres').value),consumption:Number(field('consumption').value),hourly_value:Number(field('hourly_value').value),avoid_tolls:field('avoid_tolls').checked,leg:field('leg').value};
        controller=new AbortController();form.querySelector('[type=submit]').disabled=true;result.textContent='Calculating driving routes…';
        const response=await fetch('/api/routing/compare',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body),signal:controller.signal});const r=await response.json();
        if(request!==generation)return;
        if(!response.ok)throw Error(typeof r.detail==='string'?r.detail:'Check the route inputs and try again.');
        if(r.direct)L.geoJSON(r.direct.geometry,{style:{color:'#91a2b2',weight:4,dashArray:'6 7'}}).addTo(routes);
        const route=L.geoJSON(r.via.geometry,{style:{color:'#31bacc',weight:5,opacity:.85}}).addTo(routes);map.fitBounds(route.getBounds(),{padding:[30,30],maxZoom:15});
        const row=(label,value)=>`<div><span>${label}</span><strong>${value}</strong></div>`;
        result.innerHTML=`<h4>${r.mode==='trip_detour'?'Fuel stop along your journey':'Dedicated fuel trip · there and back'}</h4><div class="road-metrics">${row('Pump price',number(r.price_cpl)+' c/L')}${row('Route via station',number(r.via.km)+' km · '+number(r.via.minutes)+' min')}${row('Direct journey',r.direct?number(r.direct.km)+' km · '+number(r.direct.minutes)+' min':'No journey otherwise · 0 km')}${row('Extra driving',number(r.extra_km)+' km · '+number(r.extra_minutes)+' min')}${row('Extra driving fuel','$'+number(r.travel_fuel_cost,2)+' · '+number(r.extra_fuel_l,2)+' L')}${row('Time value','$'+number(r.travel_time_cost,2))}${row('Fuel purchase',number(r.litres)+' L · $'+number(r.purchase_cost,2))}${row('Purchase + extra travel','$'+number(r.cost_including_travel,2))}${row('Effective price',number(r.effective_cpl)+' c/L')}</div><p>${r.fresh_prices?'':'Snapshot is stale; verify the pump price. '}${escape(r.note)}</p><p>Cyan: via station. Grey dashed: direct journey.${r.extra_km<0||r.extra_minutes<0?' Negative differences reflect alternative recommended routes; this is not a guaranteed saving.':''}</p><div class="road-disruptions" aria-live="polite"></div>${adoptBlock(r.adoption)}<small>${escape(r.attribution)}</small>`;
        checkCorridor(r.via.geometry,request);
        result.querySelector('[data-adopt]')?.addEventListener('click',()=>{onAdopt(r.adoption);status.textContent='Route adopted into the buying plan. Changing the station, points, purchase volume or price clears it.';});
        status.textContent='Route received · '+(r.via.cached?'reused from the five-minute memory cache':'fresh provider response');
      } catch(error) {if(request===generation&&error.name!=='AbortError')result.textContent=error.message;}
      finally {if(request===generation){form.querySelector('[type=submit]').disabled=false;controller=null;}}
    };
    fetch('/api/routing/status').then(r=>r.json()).then(r=>{if(status.textContent==='Checking routing connection…')status.textContent=r.configured?'Server key configured · calculate a route to verify.':'Routing key is not configured.';}).catch(()=>{if(status.textContent==='Checking routing connection…')status.textContent='Routing status unavailable.';});
    return {selectStation(value){const changed=station?.site_id!==value?.site_id||station?.price_cpl!==value?.price_cpl;station=value||null;panel.querySelector('.road-station').textContent=station?`${station.name} · ${number(station.price_cpl)} c/L`:'Select a station on the map or in the ledger.';if(changed)invalidate();}};
  }
  window.FuelRouting={attach};
})();
