/* Public event overlays only. Never sends device coordinates to QLDTraffic. */
(() => {
  'use strict';
  const escape=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const when=v=>{const d=new Date(v);return v && Number.isFinite(d.getTime())?d.toLocaleString('en-AU',{timeZone:'Australia/Brisbane',dateStyle:'short',timeStyle:'short'}):'Not supplied';};
  const timing={date_window:'Within stated dates · check active hours',scheduled:'Scheduled',past_end:'Past stated end',unknown:'Schedule not supplied'};
  function attach(map,{container}) {
    const panel=document.createElement('details');panel.className='traffic-panel';
    panel.innerHTML=`<summary>QLDTraffic · checking feed…</summary><p class="traffic-status" role="status"></p><div class="traffic-controls"><label><input type="checkbox" data-layer checked> Show event overlay</label><label><input type="checkbox" data-view checked> List current map area only</label><label>Event type<select data-type><option value="">All event types</option></select></label><label>Time window<select data-time><option value="current">Within dates / undated</option><option value="scheduled">Scheduled</option><option value="all">All returned events</option></select></label></div><p class="traffic-count"></p><div class="traffic-events"></div><p class="traffic-note">Orange: roadworks / events · pink: crashes, hazards, flooding or closures. Check the event’s dates, active hours and direction. Events do not adjust route travel times or fuel costs.</p><p class="traffic-rights"></p><div class="traffic-links"><a href="https://qldtraffic.qld.gov.au/" target="_blank" rel="noopener">Official QLDTraffic ↗</a><a href="/api/traffic/raw" target="_blank" rel="noopener">Full cached feed & rights ↗</a></div>`;
    container.append(panel);
    const q=selector=>panel.querySelector(selector),layer=L.layerGroup().addTo(map);
    let data,records=[],busy=false;
    const colour=p=>/crash|hazard|flood|clos/i.test(`${p.event_type} ${p.event_subtype} ${p.impact?.impact_type}`)?'#f67e83':'#eab466';
    function details(p) {
      const impact=p.impact||{},duration=p.duration||{},road=p.road_summary||{};
      const recurrence=(duration.recurrences||[]).map(r=>`${r.description||''} · ${r.impact?.impact_type||''} · ${r.impact?.delay||''}`).join('\n');
      return `<strong>${escape(p.event_type)} · ${escape(road.road_name||p.description)}</strong><p>${escape(p.description)}<br>${escape(p.information||'')}<br>${escape(p.advice||'')}</p><p>${escape(impact.impact_type)} · ${escape(impact.direction)} · ${escape(impact.delay)}<br>${escape(timing[p._timing])}<br>${when(duration.start)} → ${when(duration.end)}<br>Days: ${escape((duration.active_days||[]).join(', ')||'Not supplied')}</p>${recurrence?`<p class="traffic-recurrence">${escape(recurrence)}</p>`:''}<p>Updated ${when(p.last_updated)}<br>Provider: ${escape(p.source?.provided_by||p.source?.source_name||'TMR')}</p><details><summary>All event fields</summary><pre>${escape(JSON.stringify(p,null,2))}</pre></details>`;
    }
    function render() {
      if(!data)return;
      layer.clearLayers();
      const type=q('[data-type]').value,mode=q('[data-time]').value;
      const candidates=records.filter(({feature})=>{const p=feature.properties;
        const matchesTime=mode==='all'||(mode==='scheduled'?p._timing==='scheduled':['date_window','unknown'].includes(p._timing));
        return (!type||p.event_type===type) && matchesTime && (mode==='all'||String(p.status).toLowerCase()!=='archived');});
      if(q('[data-layer]').checked)candidates.forEach(r=>r.shape.addTo(layer));
      const visible=candidates.filter(r=>!q('[data-view]').checked||map.getBounds().intersects(r.bounds));
      q('summary').textContent=`QLDTraffic · ${data.status==='unavailable'?'unavailable':`${data.metro_events} metro events${data.stale?' · STALE':''}`}`;
      q('.traffic-status').textContent=`${data.error?data.error+' ':''}Fetched: ${when(data.retrieved_at)}. Feed published: ${when(data.published)}. Next upstream check no earlier than ${when(data.next_attempt_at)}.`;
      q('.traffic-count').textContent=data.status==='unavailable'?'No usable feed yet; traffic conditions are unknown.':`${visible.length} listed / ${candidates.length} match filters / ${data.metro_events} metro events (${data.total_events} across the feed). ${data.unlocated_events} feed records lack usable map geometry. ${data.stale?'Retained events may no longer describe current conditions.':''}`;
      q('.traffic-events').innerHTML=visible.map(r=>{const p=r.feature.properties;return `<article><button type="button" data-event="${r.index}">${escape(p.event_type)} · ${escape(p.road_summary?.road_name||p.description||p.id)}</button><p>${escape(p.road_summary?.locality)} · ${escape(p.impact?.impact_type)} · ${escape(p.impact?.delay)}<br>${escape(timing[p._timing])}</p><details><summary>Impact, schedule & source</summary>${details(p)}</details></article>`;}).join('') || (data.status==='unavailable'?'':'<p>No events match these filters. This does not establish that roads are clear.</p>');
      q('.traffic-rights').textContent=data.attribution;
    }
    panel.addEventListener('change',render);
    panel.addEventListener('click',event=>{const button=event.target.closest('[data-event]');if(!button)return;const record=records[Number(button.dataset.event)];if(record){map.fitBounds(record.bounds,{padding:[35,35],maxZoom:15});if(!map.hasLayer(layer))layer.addTo(map);q('[data-layer]').checked=true;render();record.shape.openPopup();}});
    map.on('moveend',render);
    async function load() {
      if(busy||document.hidden)return;busy=true;
      try {
        const response=await fetch('/api/traffic');if(!response.ok)throw Error('Local traffic endpoint unavailable');data=await response.json();
        const previous=q('[data-type]').value;
        q('[data-type]').innerHTML='<option value="">All event types</option>'+Object.keys(data.counts).sort().map(t=>`<option value="${escape(t)}">${escape(t)} (${data.counts[t]})</option>`).join('');
        if(Object.hasOwn(data.counts,previous))q('[data-type]').value=previous;
        records=[];
        for(const feature of data.features) {
          try {const p=feature.properties,c=colour(p);const shape=L.geoJSON(feature,{style:{color:c,weight:4,opacity:.85,fillOpacity:.12,dashArray:p._timing==='scheduled'?'4 6':null},pointToLayer:(_,latlng)=>L.circleMarker(latlng,{radius:7,color:c,fillColor:c,fillOpacity:.8})}).bindPopup(details(p),{maxWidth:350}).bindTooltip(`${escape(p.event_type)} · ${escape(p.road_summary?.road_name||p.description)}`);const bounds=shape.getBounds();if(bounds.isValid())records.push({feature,shape,bounds,index:records.length});}catch{ /* One unsupported geometry must not break the station map. */ }
        }
        render();
      } catch(error) {if(data){data.stale=true;data.status='stale';data.error='Local traffic refresh failed; retained events may be stale.';render();}q('summary').textContent='QLDTraffic · refresh unavailable';q('.traffic-status').textContent='Traffic refresh failed. Any displayed events are retained and may be stale.';}
      finally {busy=false;}
    }
    load();setInterval(load,60000);document.addEventListener('visibilitychange',()=>{if(!document.hidden)load();});
  }
  window.FuelTraffic={attach};
})();
