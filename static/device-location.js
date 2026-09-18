/* Device positions stay in page memory; never persisted or posted to the server. */
(() => {
  'use strict';

  function createTracker({geolocation=navigator.geolocation, onPosition, onState}) {
    let watch=null, generation=0, received=false;
    function stop(message='Location off. Coordinates are not saved.') {
      generation++;
      if (watch!==null && geolocation) geolocation.clearWatch(watch);
      watch=null;
      onPosition(null);
      onState({active:false, message});
    }
    function start() {
      if (watch!==null) return;
      if (!geolocation) {
        onState({active:false,message:'Device location is unavailable in this browser. Try a browser with location support.'});
        return;
      }
      const run=++generation;
      received=false;
      onState({active:true,message:'Waiting for device location. Allow this site’s location request if prompted.'});
      try {
        watch=geolocation.watchPosition(position=>{
          if (generation!==run) return;
          const {latitude,longitude,accuracy}=position.coords;
          if (![latitude,longitude,accuracy].every(Number.isFinite) || Math.abs(latitude)>90 || Math.abs(longitude)>180 || accuracy<0) {
            onState({active:true,message:'The device returned an invalid location; waiting for another fix.'});
            return;
          }
          const age=Math.max(0,(Date.now()-position.timestamp)/1000);
          received=true;
          onPosition({latitude,longitude,accuracy,timestamp:position.timestamp,stale:age>120});
          onState({active:true,message:`${age>120?'Last device fix':'Device location'} · accuracy ±${Math.round(accuracy).toLocaleString()} m · updated ${new Date(position.timestamp).toLocaleTimeString()}`});
        }, error=>{
          if (generation!==run) return;
          if (error.code===1) {
            stop('Location permission was denied or blocked. Allow location in browser/site and Windows privacy settings, then try again.');
          } else if (!received) {
            stop('No device location was returned. Check browser permission and device location services, then select Use my location to retry.');
          } else {
            onState({active:true,message:error.code===3
              ? 'Location request timed out. Still listening; check device location services or stop and retry.'
              : 'Device location is temporarily unavailable. Still listening for a fix.'});
          }
        }, {enableHighAccuracy:true,maximumAge:10000,timeout:20000});
      } catch {
        stop('This browser could not start location tracking. Check its location permissions.');
      }
    }
    return {start,stop};
  }

  function attach(map, {container, onPosition=()=>{}}) {
    const bar=document.createElement('div');
    bar.className='device-location';
    bar.innerHTML='<button type="button" class="locate-device" aria-pressed="false">◎ Use my location</button><button type="button" class="follow-device" aria-pressed="true" disabled>Follow location</button><span role="status">Location off · stays in this page</span>';
    container.append(bar);
    const button=bar.querySelector('.locate-device'),followButton=bar.querySelector('.follow-device'),status=bar.querySelector('[role=status]');
    let active=false,follow=true,marker,circle,lastFix,first=true;
    const tracker=createTracker({
      onState(state) {active=state.active;button.textContent=active?'◉ Stop location':'◎ Use my location';button.setAttribute('aria-pressed',String(active));status.textContent=state.message;},
      onPosition(position) {
        lastFix=position;
        if (!position) {
          if(marker)marker.remove();if(circle)circle.remove();marker=null;circle=null;
          followButton.disabled=true;first=true;onPosition(null);return;
        }
        const center=[position.latitude,position.longitude];
        if(!marker) {
          circle=L.circle(center,{radius:position.accuracy,color:'#66b5ff',weight:1,fillColor:'#66b5ff',fillOpacity:.12,interactive:false}).addTo(map);
          marker=L.circleMarker(center,{radius:8,color:'#fff',weight:2,fillColor:'#429fff',fillOpacity:1}).addTo(map).bindTooltip('Device location');
        } else {marker.setLatLng(center);circle.setLatLng(center);circle.setRadius(position.accuracy);}
        marker.setTooltipContent(`${position.stale?'Last known':'Live'} device location · ±${Math.round(position.accuracy)} m`);
        followButton.disabled=false;
        if(follow) {
          if(first)map.setView(center,Math.max(map.getZoom(),12));else map.panTo(center,{animate:false});
        }
        first=false;
        onPosition(position);
      },
    });
    button.onclick=()=>{if(active)tracker.stop();else{follow=true;followButton.setAttribute('aria-pressed','true');tracker.start();}};
    followButton.onclick=()=>{follow=!follow;followButton.setAttribute('aria-pressed',String(follow));if(follow&&lastFix)map.panTo([lastFix.latitude,lastFix.longitude]);};
    map.on('dragstart',()=>{follow=false;followButton.setAttribute('aria-pressed','false');});
    setInterval(()=>{
      if(active&&lastFix&&Date.now()-lastFix.timestamp>120000) {
        status.textContent=`Location fix is stale · last updated ${new Date(lastFix.timestamp).toLocaleTimeString()}. Waiting for device.`;
        marker?.setTooltipContent('Last known device location · stale fix');
      }
    },15000);
    // Also stop on back/forward-cache navigation; the page timer is suspended by the browser.
    window.addEventListener('pagehide',()=>tracker.stop());
    return tracker;
  }
  window.FuelLocation={createTracker,attach};
})();
