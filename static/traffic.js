/* Public event overlays only. Never sends device coordinates to QLDTraffic. */
(() => {
  'use strict';
  const escape=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const when=v=>{const d=new Date(v);return v && Number.isFinite(d.getTime())?d.toLocaleString('en-AU',{timeZone:'Australia/Brisbane',dateStyle:'short',timeStyle:'short'}):'Not supplied';};
  const timing={date_window:'Within stated dates · check active hours',scheduled:'Scheduled',past_end:'Past stated end',unknown:'Schedule not supplied'};
  // Colour is the feed's own impact wording, ranked worst first. Nothing here is our assessment.
  const LEVELS=[{label:'Closure stated',colour:'#e03a4e',weight:8},{label:'Lanes blocked / road restricted',colour:'#f07c2b',weight:7},
                {label:'Lanes affected',colour:'#d7a72b',weight:5},{label:'No blockage stated',colour:'#5d87ab',weight:4}];
  const level=p=>{const t=String(p.impact?.impact_type||'');
    return /no blockage|n\/a/i.test(t)?3:/closure|closed/i.test(t)?0:/block|restrict/i.test(t)?1:/affect/i.test(t)?2:3;};
  // Two-letter type codes: legible at 10px in every font, unlike symbol glyphs.
  const CODE={Crash:'CR',Hazard:'HZ',Flooding:'FL',Roadworks:'RW',Congestion:'CG','Special event':'SE'};
  const code=t=>CODE[t]||String(t||'?').replace(/[^A-Za-z]/g,'').slice(0,2).toUpperCase()||'??';
  const BADGE_ZOOM=11;   // Below this only the coloured lines show; point events always carry a badge.
  // Display only: Douglas-Peucker at ~11 m draws the same road lines from ~7k points instead of the
  // statewide feed's ~62k, which the map otherwise re-projects on every zoom. Popups keep full detail.
  const SIMPLIFY_DEG=.0001;
  const thin=c=>!c.length||!Array.isArray(c[0])?c:typeof c[0][0]==='number'?L.LineUtil.simplify(c.map(p=>L.point(p[0],p[1])),SIMPLIFY_DEG).map(p=>[p.x,p.y]):c.map(thin);
  const thinGeometry=g=>!g?g:g.geometries?{...g,geometries:g.geometries.map(thinGeometry)}:/Line|Polygon/.test(g.type)&&g.coordinates?.length?{...g,coordinates:thin(g.coordinates)}:g;
  function attach(map,{container}) {
    const panel=document.createElement('details');panel.className='traffic-panel';
    panel.innerHTML=`<summary>QLDTraffic · checking feed…</summary><p class="traffic-status" role="status"></p><div class="traffic-controls"><label><input type="checkbox" data-layer checked> Show event overlay</label><label><input type="checkbox" data-view checked> List current map area only</label><label>Event type<select data-type><option value="">All event types</option></select></label><label>Time window<select data-time><option value="current">Within dates / undated</option><option value="scheduled">Scheduled</option><option value="all">All returned events</option></select></label></div><div class="traffic-legend"></div><p class="traffic-count"></p><div class="traffic-bars"></div><div class="traffic-availability"></div><div class="traffic-events"></div><p class="traffic-note">Check the event’s dates, active hours and direction before relying on it. Events do not adjust route travel times or fuel costs, and no reported event does not establish that a road is clear.</p><p class="traffic-rights"></p><div class="traffic-links"><a href="https://qldtraffic.qld.gov.au/" target="_blank" rel="noopener">Official QLDTraffic ↗</a><a href="/api/traffic/raw" target="_blank" rel="noopener">Full cached feed & rights ↗</a></div>`;
    container.append(panel);
    const q=selector=>panel.querySelector(selector),layer=L.layerGroup().addTo(map);
    let data,records=[],shown=[],busy=false,selectedId=null,rendering=false;
    // Stable across reloads: record indices are rebuilt every fetch, event ids are not.
    const key=r=>String(r.feature.properties?.id??r.feature.id??r.index);
    // Composition of the events in the fetch shown above; feed availability over time is drawn separately below.
    const bars=(rows,total)=>rows.map(([label,count,fill])=>`<div class="traffic-bar"><span>${escape(label)}</span><span class="traffic-track"><i style="width:${total&&count?Math.max(1.5,100*count/total):0}%;background:${fill}"></i></span><b>${count}</b></div>`).join('');
    const colour=p=>LEVELS[level(p)].colour;
    const name=p=>p.road_summary?.road_name||p.description||p.event_type||'Event';
    // The badge, not a 7px dot: a coloured pill carrying the type code, and the road name once zoomed in.
    const pin=(p,chosen)=>L.divIcon({className:'',iconSize:[0,0],iconAnchor:[0,0],
      html:`<span class="traffic-pin traffic-pin-l${level(p)}${chosen?' traffic-pin-on':''}${p._timing==='scheduled'?' traffic-pin-later':''}" style="--pin:${colour(p)}"><b>${escape(code(p.event_type))}</b><i>${escape(name(p))}</i></span>`});
    const outcomeColour={ok:'#58dac7',rate_limited:'#eab466',not_configured:'#8aa0ae'};
    function availability() {
      const rows=(data.availability||[]).filter(r=>r&&Number.isFinite(new Date(r.at).getTime()));
      const labels=data.availability_outcomes||{};
      if(!rows.length){q('.traffic-availability').innerHTML='<p class="traffic-bar-head">Feed availability</p><p class="traffic-note">No upstream fetch attempt has been recorded yet.</p>';return;}
      const ok=rows.filter(r=>r.outcome==='ok').length;
      const strip=rows.map(r=>`<i style="background:${outcomeColour[r.outcome]||'#f67e83'}" title="${escape(when(r.at))} · ${escape(labels[r.outcome]||r.outcome)}${r.http?` · HTTP ${escape(r.http)}`:''} · retained data ${r.age_seconds==null?'none':Math.round(r.age_seconds/60)+' min old'}"></i>`).join('');
      q('.traffic-availability').innerHTML=`<p class="traffic-bar-head">Feed availability · ${ok}/${rows.length} recorded attempts answered · ${escape(when(rows[0].at))} → ${escape(when(rows[rows.length-1].at))}</p><div class="traffic-uptime">${strip}</div><div class="traffic-age-chart"></div><p class="traffic-note">${escape(data.availability_note||'')} Dots are recorded attempts; the line is how old the retained feed was at each one, so it climbs during an outage and drops to zero on recovery. Last ${escape(data.availability_limit)} attempts are kept.</p>`;
      const chart=q('.traffic-age-chart'),ages=rows.map(r=>(r.age_seconds||0)/60);
      // A flat series has a zero range, which the shared plotter turns into NaN coordinates.
      // Say it in words rather than inventing a spread that is not in the data.
      const varies=Math.max(...ages)>Math.min(...ages);
      if(window.FuelPlot&&rows.length>1&&varies)window.FuelPlot.plot(chart,rows.map(r=>({date:r.at,price:(r.age_seconds||0)/60})),
        {mini:true,unit:'min',decimals:0,base:5,pad:0,time:true,colour:'#58dac7',aria:'Age of the retained QLDTraffic feed, in minutes, at each recorded fetch attempt'});
      else chart.innerHTML=`<p class="traffic-note">${rows.length<2?'Not enough recorded attempts to plot yet.':!window.FuelPlot?'Chart unavailable on this page.':`Retained-feed age was ${Math.round(ages[0])} min at every one of the ${rows.length} recorded attempts, so there is no variation to plot.`}</p>`;
    }
    function details(p) {
      const impact=p.impact||{},duration=p.duration||{},road=p.road_summary||{};
      const recurrence=(duration.recurrences||[]).map(r=>`${r.description||''} · ${r.impact?.impact_type||''} · ${r.impact?.delay||''}`).join('\n');
      return `<strong>${escape(p.event_type)} · ${escape(road.road_name||p.description)}</strong><p>${escape(p.description)}<br>${escape(p.information||'')}<br>${escape(p.advice||'')}</p><p>${escape(impact.impact_type)} · ${escape(impact.direction)} · ${escape(impact.delay)}<br>${escape(timing[p._timing])}<br>${when(duration.start)} → ${when(duration.end)}<br>Days: ${escape((duration.active_days||[]).join(', ')||'Not supplied')}</p>${recurrence?`<p class="traffic-recurrence">${escape(recurrence)}</p>`:''}<p>Updated ${when(p.last_updated)}<br>Provider: ${escape(p.source?.provided_by||p.source?.source_name||'TMR')}</p><details><summary>All event fields</summary><pre>${escape(JSON.stringify(p,null,2))}</pre></details>`;
    }
    function render() {
      if(!data)return;
      rendering=true;            // Set before clearLayers: that fires popupclose for our own teardown.
      layer.clearLayers();
      const type=q('[data-type]').value,mode=q('[data-time]').value;
      const candidates=records.filter(({feature})=>{const p=feature.properties;
        const matchesTime=mode==='all'||(mode==='scheduled'?p._timing==='scheduled':['date_window','unknown'].includes(p._timing));
        return (!type||p.event_type===type) && matchesTime && (mode==='all'||String(p.status).toLowerCase()!=='archived');});
      shown=candidates;
      const zoomed=map.getZoom()>=BADGE_ZOOM;
      // Badges only for events near the view: ~400 metro badges rebuilt on every pan made both maps lag.
      const near=map.getBounds().pad(.25);
      map.getContainer().classList.toggle('traffic-labels',map.getZoom()>=15);
      if(q('[data-layer]').checked)candidates.forEach(r=>{
        const chosen=key(r)===selectedId;
        // Selection is a thicker, fully opaque line plus a ringed badge; everything else recedes.
        r.shape.setStyle({...r.style,weight:r.style.weight+(chosen?4:0),opacity:chosen?1:.8});
        r.shape.addTo(layer);
        r.badge.setIcon(pin(r.feature.properties,chosen));
        if(chosen||((r.point||zoomed)&&near.intersects(r.bounds)))r.badge.addTo(layer);
      });
      // clearLayers() above destroys any open popup, and moveend/the 60s reload both land here.
      // Re-open the selected event by id so panning, filtering and refreshing cannot close it.
      const chosen=selectedId==null?null:candidates.find(r=>key(r)===selectedId);
      if(chosen&&q('[data-layer]').checked)chosen.shape.openPopup();
      const visible=candidates.filter(r=>!q('[data-view]').checked||map.getBounds().intersects(r.bounds));
      q('summary').textContent=`QLDTraffic · ${data.status==='unavailable'?'unavailable':`${data.metro_events} metro events${data.stale?' · STALE':''}`}`;
      // The list, bars and legend sit inside the collapsed panel: build them only when it is open.
      if(!panel.open){rendering=false;return;}
      q('.traffic-status').textContent=`${data.error?data.error+' ':''}Fetched: ${when(data.retrieved_at)}. Feed published: ${when(data.published)}. Next upstream check no earlier than ${when(data.next_attempt_at)}.`;
      q('.traffic-count').textContent=data.status==='unavailable'?'No usable feed yet; traffic conditions are unknown.':`${visible.length} listed / ${candidates.length} match filters / ${data.metro_events} metro events (${data.total_events} across the feed). ${data.unlocated_events} feed records lack usable map geometry. ${data.stale?'Retained events may no longer describe current conditions.':''}`;
      q('.traffic-events').innerHTML=visible.map(r=>{const p=r.feature.properties;return `<article><button type="button" data-event="${r.index}">${escape(p.event_type)} · ${escape(p.road_summary?.road_name||p.description||p.id)}</button><p>${escape(p.road_summary?.locality)} · ${escape(p.impact?.impact_type)} · ${escape(p.impact?.delay)}<br>${escape(timing[p._timing])}</p><details data-detail="${r.index}"><summary>Impact, schedule & source</summary></details></article>`;}).join('') || (data.status==='unavailable'?'':'<p>No events match these filters. This does not establish that roads are clear.</p>');
      const types=Object.entries(data.counts||{}).sort((a,b)=>b[1]-a[1]);
      const impacts=Object.entries(data.impact_counts||{}).sort((a,b)=>level({impact:{impact_type:a[0]}})-level({impact:{impact_type:b[0]}}));
      q('.traffic-bars').innerHTML=data.status==='unavailable'?'':`${impacts.length?`<p class="traffic-bar-head">Reported impact · map colours · ${data.metro_events} metro events at ${when(data.published)}${data.stale?' · STALE':''}</p>${bars(impacts.map(([t,n])=>[t,n,colour({impact:{impact_type:t}})]),data.metro_events)}`:''}<p class="traffic-bar-head">Event types · map badge letters · ${data.metro_events} metro events at ${when(data.published)}${data.stale?' · STALE':''}</p>${bars(types.map(([t,n])=>[`${code(t)} · ${t}`,n,'#8aa0ae']),data.metro_events)}<p class="traffic-bar-head">Feed coverage · share of ${data.total_events} returned events</p>${bars([['Brisbane area',data.metro_events,'#58dac7'],['No map geometry',data.unlocated_events,'#f67e83'],['All feed events',data.total_events,'#8aa0ae']],data.total_events)}<p class="traffic-note">One dated snapshot, not a time series: these bars count the events in the fetch above. Impact wording is the feed's own; it is not an assessment of how bad the road is. Brisbane-area and unlocated records are both subsets of the feed total.</p>`;
      q('.traffic-legend').innerHTML=`${LEVELS.map(l=>`<span><i style="background:${l.colour}"></i>${escape(l.label)}</span>`).join('')}<span><i class="traffic-legend-dash"></i>Dashed · starts later</span><span class="traffic-legend-note">Colour is the impact wording the feed supplies, not our assessment. Badge letters are the event type (${Object.entries(CODE).map(([t,c])=>`${c} ${t}`).join(' · ')}). Badges appear from zoom ${BADGE_ZOOM}; road names are added from zoom 15.${zoomed?'':' Zoom in to label the events.'}</span>`;
      q('.traffic-rights').textContent=data.attribution;
      rendering=false;
    }
    // A pan changes which badges are near the view, never the shapes: re-adding ~450 shapes
    // (~50k road points) on every pan was most of the map's lag. The full render is for data,
    // filter and selection changes, or when the open panel's in-view list needs rebuilding.
    function onMove() {
      if(!data)return;
      if(panel.open||!q('[data-layer]').checked){render();return;}
      const zoomed=map.getZoom()>=BADGE_ZOOM,near=map.getBounds().pad(.25);
      map.getContainer().classList.toggle('traffic-labels',map.getZoom()>=15);
      for(const r of shown){
        const want=key(r)===selectedId||((r.point||zoomed)&&near.intersects(r.bounds));
        if(want&&!layer.hasLayer(r.badge))r.badge.addTo(layer);else if(!want&&layer.hasLayer(r.badge))layer.removeLayer(r.badge);
      }
    }
    panel.addEventListener('change',render);
    panel.addEventListener('toggle',()=>{if(panel.open)render();});
    panel.addEventListener('click',event=>{const button=event.target.closest('[data-event]');if(!button)return;const record=records[Number(button.dataset.event)];if(record){selectedId=key(record);map.fitBounds(record.bounds,{padding:[35,35],maxZoom:15});if(!map.hasLayer(layer))layer.addTo(map);q('[data-layer]').checked=true;render();}});
    // A close during render is our own clearLayers, not the reader dismissing the popup.
    map.on('popupclose',()=>{if(!rendering)selectedId=null;});
    map.on('moveend',onMove);
    // Event details (with every raw field) are built on first open, not for every listed event on every pan.
    q('.traffic-events').addEventListener('toggle',e=>{const box=e.target;if(box.open&&box.dataset.detail&&box.children.length<2)box.insertAdjacentHTML('beforeend',details(records[Number(box.dataset.detail)].feature.properties));},true);
    async function load() {
      if(busy||document.hidden)return;busy=true;
      try {
        const response=await fetch('/api/traffic');if(!response.ok)throw Error('Local traffic endpoint unavailable');data=await response.json();
        const previous=q('[data-type]').value;
        q('[data-type]').innerHTML='<option value="">All event types</option>'+Object.keys(data.counts).sort().map(t=>`<option value="${escape(t)}">${escape(t)} (${data.counts[t]})</option>`).join('');
        if(Object.hasOwn(data.counts,previous))q('[data-type]').value=previous;
        records=[];
        for(const feature of data.features) {
          try {
            const p=feature.properties,c=colour(p);
            // Severity sets colour and line weight; type sets the badge code; scheduled stays dashed.
            const style={color:c,weight:LEVELS[level(p)].weight,fillColor:c,fillOpacity:p.area_alert?.06:.15,dashArray:p._timing==='scheduled'?'5 7':null};
            const shape=L.geoJSON({...feature,geometry:thinGeometry(feature.geometry)},{style,pointToLayer:(_,latlng)=>L.circleMarker(latlng,{radius:4,color:c,fillColor:c,fillOpacity:.9})}).bindPopup(details(p),{maxWidth:350});
            const bounds=shape.getBounds();if(!bounds.isValid())continue;
            const record={feature,shape,style,bounds,index:records.length,point:!/Line|Polygon/.test(feature.geometry?.type||'')};
            record.badge=L.marker(bounds.getCenter(),{icon:pin(p,false),keyboard:false,riseOnHover:true})
              .bindTooltip(`${escape(p.event_type)} · ${escape(name(p))}`,{direction:'top',offset:[0,-13]});
            // Open the popup first: that closes any previous one, and popupclose clears the selection.
            record.badge.on('click',()=>{shape.openPopup();selectedId=key(record);render();});
            records.push(record);
          }catch{ /* One unsupported geometry must not break the station map. */ }
        }
        render();
        availability();   // Depends on the fetch only, so once per fetch rather than on every pan.
      } catch(error) {if(data){data.stale=true;data.status='stale';data.error='Local traffic refresh failed; retained events may be stale.';render();}q('summary').textContent='QLDTraffic · refresh unavailable';q('.traffic-status').textContent='Traffic refresh failed. Any displayed events are retained and may be stale.';}
      finally {busy=false;}
    }
    load();setInterval(load,60000);document.addEventListener('visibilitychange',()=>{if(!document.hidden)load();});
  }
  window.FuelTraffic={attach};
})();
