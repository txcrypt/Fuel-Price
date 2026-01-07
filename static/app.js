// --- Configuration & State ---
const API_BASE = '/api';
let currentState = 'QLD';
const STATE_CENTERS = {
    "QLD": [-27.470, 153.020],
    "NSW": [-33.868, 151.209],
    "VIC": [-37.813, 144.963],
    "SA":  [-34.928, 138.600],
    "WA":  [-31.950, 115.860],
    "ACT": [-35.280, 149.130],
    "TAS": [-42.882, 147.327],
    "NT":  [-12.463, 130.844]
};
let map, plannerMap;
let markers = [];
let stationData = []; // Store raw data for filtering
let plannerLayer = null;
let trendChart = null;
let cycleChart = null;
let driveWatchId = null;
let wakeLock = null;

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
    initNavigation();
    initLiveView(); 
    initFindNearMe();
    initMapSearch();
});

function initMapSearch() {
    const input = document.getElementById('map-search');
    if (!input) return;
    
    input.addEventListener('keyup', (e) => {
        const term = e.target.value.toLowerCase();
        filterMap(term);
    });
}

function filterMap(term) {
    if (!map) return;
    
    // Clear current
    markers.forEach(m => map.removeLayer(m));
    markers = [];
    
    // Filter & Re-add
    stationData.forEach(s => {
        const txt = (s.name + " " + s.suburb + " " + s.brand).toLowerCase();
        if (term === "" || txt.includes(term)) {
             if(s.lat && s.lng) {
                const color = s.is_cheap ? '#10b981' : '#ef4444';
                const m = L.circleMarker([s.lat, s.lng], {
                    radius: 6, fillColor: color, color: '#fff', weight: 1, fillOpacity: 0.8
                }).addTo(map);
                
                m.bindPopup(`<b>${s.brand}</b><br>${s.name}<br>${s.suburb}<br><b style="color:${color}">${s.price}c</b>`);
                markers.push(m);
            }
        }
    });
}

// --- Find Near Me ---
function initFindNearMe() {
    const btn = document.getElementById('btn-find-near');
    const container = document.getElementById('near-me-results');
    let watchId = null;
    
    if(!btn) return;
    
    btn.addEventListener('click', () => {
        if (!navigator.geolocation) {
            alert("Geolocation is not supported by your browser.");
            return;
        }
        
        // Toggle Logic
        if (watchId !== null) {
            // Stop watching
            navigator.geolocation.clearWatch(watchId);
            watchId = null;
            btn.innerText = "📍 Find Cheapest Fuel Near Me";
            btn.classList.remove('active-tracking'); // Optional styling hook
            container.style.opacity = '0.5';
            return;
        }
        
        // Start watching
        btn.innerText = "🛑 Stop Tracking";
        btn.classList.add('active-tracking');
        container.style.display = 'block';
        container.style.opacity = '1';
        container.innerHTML = '<p style="text-align:center; color:#94a3b8;">Acquiring precise location...</p>';
        
        watchId = navigator.geolocation.watchPosition(
            async (position) => {
                const { latitude, longitude, accuracy } = position.coords;
                // Optional: Show accuracy radius or debug info
                // console.log(`Location update: ${latitude}, ${longitude} (Acc: ${accuracy}m)`);
                
                try {
                    const res = await fetch(`${API_BASE}/find_cheapest_nearby`, {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({ latitude, longitude })
                    });
                    const data = await res.json();
                    
                    if (!data || data.length === 0) {
                        container.innerHTML = '<p style="text-align:center; color:#ef4444;">No stations found within 15km.</p>';
                    } else {
                        let html = `<div style="text-align:center; color:#94a3b8; font-size:0.8rem; margin-bottom:10px;">
                            Updating live based on your location (±${Math.round(accuracy)}m)
                        </div>`;
                        
                        html += '<div style="display:grid; gap:10px;">';
                        data.forEach(s => {
                            html += `
                                <div style="display:flex; justify-content:space-between; align-items:center; padding:10px; background:rgba(255,255,255,0.05); border-radius:8px; border-left: 3px solid #10b981;">
                                    <div>
                                        <div style="font-weight:bold; color:#fff;">${s.name}</div>
                                        <div style="font-size:0.8rem; color:#94a3b8;">${s.distance.toFixed(2)} km away • ${s.suburb}</div>
                                    </div>
                                    <div style="font-size:1.2rem; font-weight:bold; color:#10b981;">${s.price.toFixed(1)}c</div>
                                </div>
                            `;
                        });
                        html += '</div>';
                        container.innerHTML = html;
                    }
                } catch (e) {
                    console.error(e);
                    // Don't wipe container on transient network error, just log
                }
            },
            (error) => {
                console.error(error);
                let msg = "Unable to retrieve location.";
                if(error.code === 1) msg = "Location permission denied.";
                container.innerHTML = `<p style="text-align:center; color:#ef4444;">${msg}</p>`;
                
                // Stop on fatal error
                if (watchId !== null) {
                    navigator.geolocation.clearWatch(watchId);
                    watchId = null;
                    btn.innerText = "📍 Find Cheapest Fuel Near Me";
                    btn.classList.remove('active-tracking');
                }
            },
            {
                enableHighAccuracy: true, // Request precise GPS
                timeout: 10000,
                maximumAge: 0 // Do not use cached positions
            }
        );
    });
}

// --- Navigation ---
function initNavigation() {
    // State Selector
    const stateSel = document.getElementById('state-selector');
    if (stateSel) {
        stateSel.value = currentState;
        stateSel.addEventListener('change', (e) => {
            currentState = e.target.value;
            
            // Re-center maps
            if (map) map.setView(STATE_CENTERS[currentState], currentState === 'QLD' ? 11 : 9);
            if (plannerMap) plannerMap.setView(STATE_CENTERS[currentState], 9);

            // Refresh current view
            const activeNav = document.querySelector('.nav-item.active');
            if (activeNav) {
                const viewId = activeNav.getAttribute('data-view');
                loadViewData(viewId);
            }
        });
    }

    // Desktop Tabs
    const navItems = document.querySelectorAll('.nav-item');
    // Mobile Bottom Tabs
    const bottomNavItems = document.querySelectorAll('.b-nav-item');
    
    const switchView = (item) => {
        // UI Toggle (Both Navs)
        const viewId = item.getAttribute('data-view');
        
        // Update Desktop Classes
        navItems.forEach(i => {
            if(i.getAttribute('data-view') === viewId) i.classList.add('active');
            else i.classList.remove('active');
        });
        
        // Update Mobile Classes
        bottomNavItems.forEach(i => {
             if(i.getAttribute('data-view') === viewId) i.classList.add('active');
             else i.classList.remove('active');
        });

        // Toggle Views
        document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
        document.getElementById(`view-${viewId}`).classList.add('active');
        
        // Map Resize Fixes
        if (viewId === 'live' && map) setTimeout(() => map.invalidateSize(), 100);
        if (viewId === 'planner' && plannerMap) setTimeout(() => plannerMap.invalidateSize(), 100);

        // Logic Switch
        if (viewId !== 'drive') stopDriveMode();
        loadViewData(viewId);
    };

    navItems.forEach(item => item.addEventListener('click', () => switchView(item)));
    bottomNavItems.forEach(item => item.addEventListener('click', () => switchView(item)));
}

function loadViewData(viewId) {
    try {
        switch(viewId) {
            case 'live': initLiveView(); break;
            case 'sentiment': loadSentiment(); break;
            case 'ratings': loadRatings(); break;
            case 'planner': loadPlanner(); break;
            case 'analytics': loadAnalytics(); break;
            case 'drive': initDriveMode(); break;
        }
    } catch (e) {
        console.error("View Load Error:", e);
    }
}

// --- Tab 1: Live Market ---
async function initLiveView() {
    try {
        // Fetch Market Status and News in parallel (Resilient)
        const results = await Promise.allSettled([
            fetch(`${API_BASE}/market-status?state=${currentState}`),
            fetch(`${API_BASE}/sentiment`)
        ]);

        const statusRes = results[0];
        const newsRes = results[1];

        // Critical: Market Data
        if (statusRes.status === 'rejected' || !statusRes.value.ok) {
            throw new Error("Market Data API Error");
        }
        const data = await statusRes.value.json();
        
        // Optional: News Data
        let newsData = { domestic: [] };
        if (newsRes.status === 'fulfilled' && newsRes.value.ok) {
            try { newsData = await newsRes.value.json(); } catch(e) { console.warn("News Parse Error"); }
        } else {
            console.warn("News API unavailable");
        }
        
        if (!data || !data.ticker) {
            document.getElementById('status-text').innerText = "No Data";
            return;
        }

        // --- Ticker (Market Data + News Headlines) ---
        const tickerEl = document.getElementById('global-ticker');
        
        // Market Data Part
        let tickerHtml = `
            <span>🛢️ BRENT: $${(data.ticker.oil||0).toFixed(2)}</span>
            <span>🏭 TGP: ${(data.ticker.tgp||0).toFixed(1)}c</span>
            <span>⛽ MOGAS 95: $${(data.ticker.mogas||0).toFixed(2)}</span>
            <span>📉 TREND: ${data.status}</span>
        `;

        // News Headlines Part
        if (newsData.domestic && newsData.domestic.length > 0) {
            newsData.domestic.forEach(n => {
                const icon = n.sentiment.includes('Relief') ? '🟢' : (n.sentiment.includes('Pressure') ? '🔴' : '📰');
                tickerHtml += `<span>${icon} ${n.title.toUpperCase()}</span>`;
            });
        }
        
        tickerEl.innerHTML = tickerHtml;

        // KPIs
        document.getElementById('status-text').innerText = data.status || "--";
        document.getElementById('tgp-val').innerText = `${(data.ticker.tgp||0).toFixed(1)}c`;
        
        const badge = document.getElementById('advice-badge');
        badge.innerText = data.advice || "Hold";
        badge.style.background = data.advice_type === 'error' ? '#ef4444' : (data.advice_type === 'success' ? '#10b981' : '#3b82f6');
        
        if (data.last_updated) {
            document.getElementById('last-updated').innerText = `Data updated: ${data.last_updated}`;
        }

        if (data.savings_insight) {
            document.getElementById('savings-insight').innerText = data.savings_insight;
        }

        document.getElementById('hike-prediction').innerText = `Est. Next Hike: ${data.next_hike_est || "?"}`;
        
        if (data.current_avg) {
             document.getElementById('avg-price-display').innerText = `Market Avg: ${data.current_avg.toFixed(1)}c`;
        }

        // Cycle Chart (History + Forecast)
        if (data.history && data.history.dates) {
            const ctx = document.getElementById('cycleChart');
            if (ctx) {
                if (cycleChart) cycleChart.destroy();
                
                // Merge Data
                const histDates = data.history.dates;
                const histPrices = data.history.prices;
                const fcDates = data.forecast ? data.forecast.dates : [];
                const fcPrices = data.forecast ? data.forecast.prices : [];
                
                const allLabels = [...histDates, ...fcDates];
                
                // Pad forecast with nulls for history part
                const paddedFc = new Array(histDates.length).fill(null);
                // Connect lines: start forecast at last history point
                if(histPrices.length > 0) paddedFc[paddedFc.length-1] = histPrices[histPrices.length-1];
                
                const finalFc = [...paddedFc, ...fcPrices];
                
                cycleChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: allLabels,
                        datasets: [
                            {
                                label: 'History',
                                data: [...histPrices, ...new Array(fcPrices.length).fill(null)],
                                borderColor: '#10b981',
                                backgroundColor: 'rgba(16, 185, 129, 0.1)',
                                fill: true,
                                tension: 0.4,
                                pointRadius: 0,
                                pointHitRadius: 10
                            },
                            {
                                label: 'Forecast',
                                data: finalFc,
                                borderColor: '#f59e0b',
                                borderDash: [5, 5],
                                backgroundColor: 'rgba(0,0,0,0)',
                                fill: false,
                                tension: 0.4,
                                pointRadius: 0
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        interaction: { intersect: false, mode: 'index' },
                        plugins: { legend: { display: true, labels: {color:'#94a3b8'} }, tooltip: { callbacks: { label: (c) => `${c.raw.toFixed(1)}c` } } },
                        scales: {
                            x: { grid: { display: false }, ticks: { maxTicksLimit: 6, color: '#94a3b8' } },
                            y: { grid: { color: '#334155' }, ticks: { color: '#94a3b8' } }
                        }
                    }
                });
            }
        }

        // Map
        initMap();
        loadStations();
        
    } catch (e) { 
        console.error(e);
        document.getElementById('status-text').innerText = "Offline"; 
    }
}

function initMap() {
    if (map) return;
    const mapEl = document.getElementById('map');
    if (!mapEl) return;
    
    map = L.map('map').setView(STATE_CENTERS[currentState], currentState === 'QLD' ? 11 : 9);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; CARTO'
    }).addTo(map);
}

async function loadStations() {
    try {
        const res = await fetch(`${API_BASE}/stations?state=${currentState}`);
        const stations = await res.json();
        
        if (!map) return;
        stationData = stations; // Store for search
        
        // Clear old markers
        markers.forEach(m => map.removeLayer(m));
        markers = [];

        if (Array.isArray(stations)) {
            stations.forEach(s => {
                if(s.lat && s.lng) {
                    const color = s.is_cheap ? '#10b981' : '#ef4444';
                    const m = L.circleMarker([s.lat, s.lng], {
                        radius: 6, fillColor: color, color: '#fff', weight: 1, fillOpacity: 0.8
                    }).addTo(map);
                    
                    m.bindPopup(`<b>${s.brand}</b><br>${s.name}<br>${s.suburb}<br><b style="color:${color}">${s.price}c</b>`);
                    markers.push(m);
                }
            });
        }
    } catch(e) { console.warn("Station load failed", e); }
}

// --- DRIVE MODE ---
async function initDriveMode() {
    // 1. Wake Lock
    try {
        if ('wakeLock' in navigator) {
            wakeLock = await navigator.wakeLock.request('screen');
            console.log('Wake Lock active');
        }
    } catch (err) { console.warn("Wake Lock failed:", err); }

    // 2. Clear previous watch
    if (driveWatchId !== null) navigator.geolocation.clearWatch(driveWatchId);

    // 3. Start Geolocation
    const speedEl = document.getElementById('drive-speed');
    const accEl = document.getElementById('drive-acc');
    const cardEl = document.getElementById('drive-main-card');
    const navBtn = document.getElementById('drive-nav-btn');
    
    // Card placeholders
    const cName = cardEl.querySelector('.big-name');
    const cPrice = cardEl.querySelector('.big-price');
    const cMeta = cardEl.querySelector('.big-meta');

    driveWatchId = navigator.geolocation.watchPosition(
        async (position) => {
            const { latitude, longitude, accuracy, speed } = position.coords;
            
            // Speed (m/s to km/h)
            if (speed !== null && speed >= 0) {
                speedEl.innerHTML = `${Math.round(speed * 3.6)} <span style="font-size:0.8rem; font-weight:normal;">km/h</span>`;
            }
            
            // Accuracy
            if(accuracy < 20) {
                 accEl.innerText = "Excellent";
                 accEl.style.color = "#10b981";
            } else if (accuracy < 100) {
                 accEl.innerText = "Good";
                 accEl.style.color = "#f59e0b";
            } else {
                 accEl.innerText = "Poor";
                 accEl.style.color = "#ef4444";
            }

            // Fetch Nearest
            try {
                const res = await fetch(`${API_BASE}/find_cheapest_nearby`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ latitude, longitude })
                });
                const data = await res.json();
                
                if (data && data.length > 0) {
                    const best = data[0]; // Already sorted by Price then Distance
                    
                    cName.innerText = best.name;
                    cPrice.innerText = `${best.price.toFixed(1)}c`;
                    cMeta.innerText = `${best.distance.toFixed(1)} km • ${best.brand}`;
                    
                    // Highlight if cheap (< 165c approx logic)
                    if(best.price < 170) cardEl.classList.add('highlight');
                    else cardEl.classList.remove('highlight');
                    
                    // Activate Nav Button
                    navBtn.style.opacity = '1';
                    navBtn.style.pointerEvents = 'auto';
                    navBtn.href = `https://www.google.com/maps/dir/?api=1&destination=${latitude},${longitude}&destination_place_id=${best.name}&travelmode=driving`; 
                    // Note: Ideally backend sends Lat/Lng of station. 
                    // Backend `find_cheapest_nearby` sends distance but not explicit lat/lng in the response object currently?
                    // Let's assume we might need to patch backend or just use Name search for Google Maps if Coords missing.
                    // Actually, let's fix the Nav link to use the station's lat/lng if we can.
                    // Checking backend: `find_cheapest_nearby` returns {name, price, distance, brand, suburb}. 
                    // It DOES NOT return lat/lng. 
                    // FIX: I will use the current user location + name query for now, OR rely on the fact that standard Google Maps query works well.
                    navBtn.href = `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(best.name + " " + best.suburb + " Fuel Station")}`;
                    
                } else {
                    cName.innerText = "No Stations";
                    cPrice.innerText = "--.-";
                }
            } catch (e) { console.error(e); }
        }, 
        (err) => {
            console.error(err);
            accEl.innerText = "GPS Error";
            accEl.style.color = "#ef4444";
        },
        { enableHighAccuracy: true, timeout: 5000, maximumAge: 0 }
    );
}

async function stopDriveMode() {
    if (driveWatchId !== null) {
        navigator.geolocation.clearWatch(driveWatchId);
        driveWatchId = null;
    }
    if (wakeLock !== null) {
        await wakeLock.release();
        wakeLock = null;
        console.log('Wake Lock released');
    }
}

// --- Tab 2: Sentiment ---
async function loadSentiment() { 
    try {
        const res = await fetch(`${API_BASE}/sentiment`);
        const data = await res.json();
        
        const renderFeed = (items, containerId) => {
            const el = document.getElementById(containerId);
            if (!el) return;
            
            if (items && items.length > 0) {
                el.innerHTML = items.map(a => `
                    <div class="news-item">
                        <div class="news-title"><a href="${a.link}" target="_blank" style="color:#fff;text-decoration:none;">${a.title}</a></div>
                        <div class="news-meta">
                            <span>${a.publisher}</span>
                            <span>${a.published ? a.published.substring(0, 16) : ''}</span>
                            <span style="color:${a.sentiment.includes('High') ? '#ef4444' : '#10b981'}">${a.sentiment}</span>
                        </div>
                    </div>
                `).join('');
            } else {
                el.innerHTML = "<p style='padding:10px; opacity:0.6;'>No news available.</p>";
            }
        };

        renderFeed(data.global, 'feed-global');
        renderFeed(data.domestic, 'feed-domestic');

    } catch(e) {
        console.error(e);
    }
}

// --- Tab 3: Ratings ---
async function loadRatings() {
    try {
        const res = await fetch(`${API_BASE}/stations?state=${currentState}`);
        let stations = await res.json();
        if (!Array.isArray(stations)) stations = [];
        
        const renderTable = (data, container, isBest) => {
            const el = document.getElementById(container);
            if(!el) return;
            if(data.length === 0) { el.innerHTML = "<p style='padding:1rem; opacity:0.5'>No data available.</p>"; return; }
            
            const hintId = isBest ? 'hint-best' : 'hint-worst';
            const toggleId = isBest ? 'toggle-best' : 'toggle-worst';
            
            el.innerHTML = `
                <div style="margin-bottom:10px;">
                    <a href="#" id="${toggleId}" style="font-size:0.8rem; color:var(--accent-blue); text-decoration:none;">❓ What does this score mean?</a>
                    <div id="${hintId}" style="display:none; margin-top:5px; font-size:0.8rem; color:var(--text-secondary); background:rgba(0,0,0,0.2); padding:8px; border-radius:4px;">
                        The Fairness Score compares the station's price to the local average.<br>
                        <b>Negative Score (e.g. -5.0):</b> Station is 5c <i>cheaper</i> than average.<br>
                        <b>Positive Score (e.g. +5.0):</b> Station is 5c <i>more expensive</i> than average.<br>
                        Lower is better!
                    </div>
                </div>
                <table>
                    <tr><th>Station</th><th>Suburb</th><th>Price</th><th>Score</th></tr>
                    ${data.map(s => `
                        <tr>
                            <td>${s.name}</td>
                            <td>${s.suburb !== 'Unknown' ? s.suburb : '-'}</td>
                            <td style="font-weight:bold;">${s.price}c</td>
                            <td style="color:${s.fairness_score < 0 ? '#10b981' : '#ef4444'}">${s.fairness_score.toFixed(1)}</td>
                        </tr>
                    `).join('')}
                </table>
            `;
            
            setTimeout(() => {
                const toggle = document.getElementById(toggleId);
                if(toggle) {
                    toggle.addEventListener('click', (e) => {
                        e.preventDefault();
                        const hint = document.getElementById(hintId);
                        hint.style.display = hint.style.display === 'block' ? 'none' : 'block';
                    });
                }
            }, 0);
        };

        // Best Value: Lowest Score First (Ascending)
        const best = [...stations].sort((a, b) => a.fairness_score - b.fairness_score).slice(0, 10);
        renderTable(best, 'table-best', true);

        // Most Expensive: Highest Score First (Descending)
        const worst = [...stations].sort((a, b) => b.fairness_score - a.fairness_score).slice(0, 10);
        renderTable(worst, 'table-worst', false);
    } catch(e) { console.error(e); }
}

// --- Tab 4: Planner ---
function loadPlanner() {
    if (!plannerMap) {
        const pmEl = document.getElementById('planner-map');
        if (pmEl) {
            plannerMap = L.map('planner-map').setView(STATE_CENTERS[currentState], 9);
            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; CARTO'
            }).addTo(plannerMap);
        }
    }

    const btn = document.querySelector('#view-planner button');
    if (!btn) return;
    
    const newBtn = btn.cloneNode(true);
    btn.parentNode.replaceChild(newBtn, btn);
    
    newBtn.addEventListener('click', async () => {
        const inputs = document.querySelectorAll('#view-planner input');
        const start = inputs[0].value;
        const end = inputs[1].value;
        
        if (!start || !end) return alert("Please enter both locations.");
        
        newBtn.disabled = true;
        newBtn.innerText = "Calculating...";
        
        try {
            const res = await fetch(`${API_BASE}/planner`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({start, end})
            });
            const data = await res.json();
            
            if (plannerLayer) {
                plannerMap.removeLayer(plannerLayer);
                plannerLayer = null;
            }
            plannerMap.closePopup();

            const container = document.querySelector('#view-planner .card');
            const oldRes = document.getElementById('planner-results');
            if (oldRes) oldRes.remove();
            
            const resDiv = document.createElement('div');
            resDiv.id = 'planner-results';

            if (data.error) {
                resDiv.innerHTML = `<p style="color:#ef4444; margin-top:10px;">⚠️ ${data.error}</p>`;
            } else {
                let html = `<div style="margin-top:20px; padding:15px; background:rgba(255,255,255,0.05); border-radius:8px;">
                    <h3>Route: ${data.start.name} ➝ ${data.end.name}</h3>
                    <p>Distance: ${data.distance_km.toFixed(1)} km</p>
                </div>`;
                
                const routeGroup = L.featureGroup();
                
                if (data.route_path) {
                    L.polyline(data.route_path, {color: '#3b82f6', weight: 4}).addTo(routeGroup);
                }

                if (data.stations && data.stations.length > 0) {
                    html += `<table style="margin-top:20px;">
                        <tr><th>Station</th><th>Price</th><th>Save</th></tr>
                        ${data.stations.map(s => `
                            <tr>
                                <td>${s.name}</td>
                                <td style="font-weight:bold; color:${s.price_cpl < data.market_avg ? '#10b981' : '#fff'}">${s.price_cpl}c</td>
                                <td>$${s.net_utility.toFixed(2)}</td>
                            </tr>
                        `).join('')}
                    </table>`;
                    
                    data.stations.forEach(s => {
                         const m = L.circleMarker([s.latitude, s.longitude], {
                            radius: 8, fillColor: '#10b981', color: '#fff', weight: 2, fillOpacity: 0.9
                        }).addTo(routeGroup);
                        m.bindPopup(`<b>${s.name}</b><br>${s.price_cpl}c`);
                    });
                } else {
                    html += `<p style="margin-top:10px; opacity:0.7">No suitable stations found along route.</p>`;
                }
                
                if (plannerMap) {
                    plannerLayer = routeGroup.addTo(plannerMap);
                    plannerMap.fitBounds(routeGroup.getBounds(), {padding: [50, 50]});
                }
                
                resDiv.innerHTML = html;
            }
            container.appendChild(resDiv);
            
        } catch (e) {
            console.error(e);
            alert("Planning failed. Check server logs.");
        } finally {
            newBtn.disabled = false;
            newBtn.innerText = "Find Cheapest Stops";
        }
    });
}

// --- Tab 5: Analytics ---
async function loadAnalytics() {
    try {
        const res = await fetch(`${API_BASE}/analytics?state=${currentState}`);
        const data = await res.json();
        
        // 1. Suburb Table
        const el = document.getElementById('table-suburbs');
        if(el && data.suburb_ranking && data.suburb_ranking.length > 0) {
             el.innerHTML = `<table><tr><th>Suburb</th><th>Avg Price</th></tr>${data.suburb_ranking.map(r=>`<tr><td>${r.suburb !== 'Unknown' ? r.suburb : '-'}</td><td>${r.price_cpl.toFixed(1)}c</td></tr>`).join('')}</table>`;
        } else if (el) {
            el.innerHTML = "<p style='padding:10px; opacity:0.6;'>No live data available to rank suburbs.</p>";
        }

        // 2. Chart
        const ctx = document.getElementById('analyticsChart');
        if (ctx && data.trend && data.trend.history) {
            if (trendChart) trendChart.destroy();
            
            // Fix: Use correct keys (dates/values from backend)
            const historyDates = data.trend.history.dates || [];
            const historyTgp = data.trend.history.values || [];
            
            // Format dates
            const labels = historyDates.map(d => new Date(d).toLocaleDateString(undefined, {month:'short', day:'numeric'}));
            
            // Add Forecast
            const forecastDates = data.trend.sarimax.forecast_dates.map(d => new Date(d).toLocaleDateString(undefined, {month:'short', day:'numeric'}));
            const forecastVals = data.trend.sarimax.forecast_mean;
            
            const allLabels = [...labels, ...forecastDates];
            const allData = [...historyTgp, ...forecastVals];
            
            trendChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: allLabels,
                    datasets: [{
                        label: 'TGP Forecast (cpl)',
                        data: allData,
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        borderWidth: 2,
                        tension: 0.4,
                        pointRadius: 0,
                        pointHitRadius: 10
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { intersect: false, mode: 'index' },
                    scales: {
                        y: { 
                            grid: { color: '#334155' },
                            ticks: { color: '#94a3b8' } 
                        },
                        x: { 
                            grid: { display: false },
                            ticks: { color: '#94a3b8', maxTicksLimit: 8 } 
                        }
                    },
                    plugins: {
                        legend: { display: false }
                    }
                }
            });
        }

    } catch(e) { console.error("Analytics Error", e); }
}