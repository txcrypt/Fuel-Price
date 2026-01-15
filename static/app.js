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
let stationData = [];
let plannerLayer = null;
let trendChart = null;
let cycleChart = null;
let driveWatchId = null;
let wakeLock = null;

// Expose app functions globally for inline HTML clicks
window.app = {
    loadSubView: (view) => loadSubView(view),
    closeSubView: () => closeSubView()
};

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
    initNavigation();
    initLiveView(); 
    initFindNearMe();
    initMapSearch();
    
    // Planner Button Listener
    const pBtn = document.getElementById('planner-btn');
    if(pBtn) pBtn.addEventListener('click', runPlanner);
});

function initMapSearch() {
    const input = document.getElementById('map-search');
    if (!input) return;
    input.addEventListener('keyup', (e) => filterMap(e.target.value.toLowerCase()));
}

function filterMap(term) {
    if (!map) return;
    markers.forEach(m => map.removeLayer(m));
    markers = [];
    
    stationData.forEach(s => {
        const txt = (s.name + " " + s.suburb + " " + s.brand).toLowerCase();
        if (term === "" || txt.includes(term)) {
             if(s.lat && s.lng) {
                const color = s.is_cheap ? '#10b981' : '#ef4444';
                const m = L.circleMarker([s.lat, s.lng], {
                    radius: 6, fillColor: color, color: '#fff', weight: 1, fillOpacity: 0.8
                }).addTo(map);
                m.bindPopup(`
                    <div class="font-sans text-slate-900">
                        <div class="font-bold">${s.brand}</div>
                        <div class="text-xs text-slate-500">${s.name}</div>
                        <div class="text-lg font-black ${s.is_cheap ? 'text-emerald-600' : 'text-red-600'}">${s.price}c</div>
                    </div>
                `);
                markers.push(m);
            }
        }
    });
}

function initFindNearMe() {
    const btn = document.getElementById('btn-find-near');
    const container = document.getElementById('near-me-results');
    let watchId = null;
    if(!btn) return;
    
    btn.addEventListener('click', () => {
        if (!navigator.geolocation) return alert("Geolocation not supported.");
        
        if (watchId !== null) {
            navigator.geolocation.clearWatch(watchId);
            watchId = null;
            btn.innerHTML = `<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 11a3 3 0 11-6 0 3 3 0 016 0z"></path></svg> Find Cheapest`;
            btn.classList.remove('bg-red-600', 'animate-pulse');
            btn.classList.add('bg-primary-600');
            container.classList.add('hidden');
            return;
        }
        
        btn.innerHTML = `🛑 Stop Tracking`;
        btn.classList.remove('bg-primary-600');
        btn.classList.add('bg-red-600', 'animate-pulse');
        container.classList.remove('hidden');
        container.innerHTML = '<p class="text-center text-slate-400 text-sm py-2">Locating satellites...</p>';
        
        watchId = navigator.geolocation.watchPosition(async (pos) => {
            const { latitude, longitude, accuracy } = pos.coords;
            try {
                const res = await fetch(`${API_BASE}/find_cheapest_nearby`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ latitude, longitude })
                });
                const data = await res.json();
                
                if (!data || data.length === 0) {
                    container.innerHTML = '<p class="text-center text-red-400 text-sm">No stations found nearby.</p>';
                } else {
                    let html = `<div class="text-center text-[10px] text-slate-500 mb-2">GPS Accuracy: ±${Math.round(accuracy)}m</div><div class="space-y-2">`;
                    data.slice(0, 3).forEach(s => { // Only top 3
                        html += `
                            <div class="flex justify-between items-center p-3 bg-slate-800/50 rounded-xl border border-slate-700/50">
                                <div>
                                    <div class="font-bold text-white text-sm">${s.name}</div>
                                    <div class="text-xs text-slate-400">${s.distance.toFixed(1)}km • ${s.brand}</div>
                                </div>
                                <div class="text-xl font-black text-emerald-400">${s.price.toFixed(1)}</div>
                            </div>
                        `;
                    });
                    html += '</div>';
                    container.innerHTML = html;
                }
            } catch(e) {}
        }, null, { enableHighAccuracy: true });
    });
}

// --- Navigation Logic ---
function initNavigation() {
    const stateSel = document.getElementById('state-selector');
    if (stateSel) {
        stateSel.value = currentState;
        stateSel.addEventListener('change', (e) => {
            currentState = e.target.value;
            if (map) map.setView(STATE_CENTERS[currentState], currentState === 'QLD' ? 11 : 9);
            initLiveView(); // Refresh data
        });
    }

    const switchTab = (target) => {
        // 1. UI Update
        document.querySelectorAll('.nav-btn, .nav-btn-mobile').forEach(btn => {
            const isTarget = btn.getAttribute('data-target') === target;
            // Desktop styles
            if(btn.classList.contains('nav-btn')) {
                if(isTarget) { btn.classList.remove('text-slate-400','bg-transparent'); btn.classList.add('bg-slate-700','text-white','shadow-sm'); }
                else { btn.classList.add('text-slate-400','bg-transparent'); btn.classList.remove('bg-slate-700','text-white','shadow-sm'); }
            }
            // Mobile styles
            if(btn.classList.contains('nav-btn-mobile')) {
                if(isTarget) { btn.classList.add('text-primary-500'); btn.classList.remove('text-slate-400'); }
                else { btn.classList.add('text-slate-400'); btn.classList.remove('text-primary-500'); }
            }
        });

        // 2. View Switching
        document.querySelectorAll('.view-section').forEach(v => v.classList.add('hidden'));
        document.getElementById(`view-${target}`).classList.remove('hidden');
        
        // 3. Logic Hooks
        if (target === 'live' && map) setTimeout(() => map.invalidateSize(), 100);
        if (target === 'drive') initDriveMode(); else stopDriveMode();
    };

    document.querySelectorAll('[data-target]').forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.getAttribute('data-target')));
    });
}

function loadSubView(toolName) {
    // Hide tool grid, show sub-view container
    document.querySelectorAll('.tool-card').forEach(el => el.parentElement.classList.add('hidden')); // Hide grid
    document.getElementById('sub-view-container').classList.remove('hidden');
    
    // Hide all subs, show requested
    document.querySelectorAll('.sub-view').forEach(el => el.classList.add('hidden'));
    document.getElementById(`sub-${toolName}`).classList.remove('hidden');
    
    // Trigger loads
    if(toolName === 'planner') initPlannerMap();
    if(toolName === 'ratings') loadRatings();
    if(toolName === 'analytics') loadAnalytics();
    if(toolName === 'sentiment') loadSentiment();
}

function closeSubView() {
    document.getElementById('sub-view-container').classList.add('hidden');
    document.querySelectorAll('.tool-card').forEach(el => el.parentElement.classList.remove('hidden')); // Show grid
}

// --- Live View Logic ---
async function initLiveView() {
    try {
        const res = await fetch(`${API_BASE}/market-status?state=${currentState}`);
        const data = await res.json();
        if(!data.ticker) return;
        
        // Update Ticker
        const t = data.ticker;
        const tickerHTML = `
            <span>🛢️ BRENT: $${t.oil.toFixed(2)}</span>
            <span class="text-slate-600">|</span>
            <span>⛽ MOGAS: $${t.mogas.toFixed(2)}</span>
            <span class="text-slate-600">|</span>
            <span>TREND: <span class="${data.status === 'HIKE_STARTED' ? 'text-red-400' : 'text-emerald-400'}">${data.status}</span></span>
        `;
        document.getElementById('global-ticker').innerHTML = tickerHTML;

        // Hero Stats
        document.getElementById('status-text').innerText = data.status;
        document.getElementById('status-text').className = `text-3xl md:text-5xl font-black tracking-tight ${data.status.includes('HIKE') ? 'text-red-500' : 'text-emerald-400'}`;
        
        document.getElementById('advice-badge').innerText = data.advice;
        document.getElementById('advice-badge').className = `px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wide ${data.advice_type === 'success' ? 'bg-emerald-500/20 text-emerald-300' : 'bg-red-500/20 text-red-300'}`;
        
        document.getElementById('tgp-val').innerHTML = `${t.tgp.toFixed(1)}<span class="text-xl text-slate-500">c</span>`;
        document.getElementById('savings-insight').innerText = data.savings_insight;
        document.getElementById('hike-prediction').innerText = `Next Hike: ${data.next_hike_est}`;
        if(data.current_avg) document.getElementById('avg-price-display').innerText = `Avg: ${data.current_avg.toFixed(1)}c`;

        // Cycle Chart
        renderChart('cycleChart', data.history.dates, data.history.prices, data.forecast?.prices);
        
        // Map
        initMap();
        loadStations();
    } catch(e) { console.error(e); }
}

function renderChart(id, hDates, hPrices, fPrices) {
    const ctx = document.getElementById(id);
    if(!ctx) return;
    if(cycleChart) cycleChart.destroy();
    
    // Pad forecast
    const combinedLabels = [...hDates, ...(new Array(fPrices?.length || 0).fill('Fcst'))];
    const paddedFc = new Array(hPrices.length).fill(null);
    if(hPrices.length > 0) paddedFc[paddedFc.length-1] = hPrices[hPrices.length-1]; // Link
    
    cycleChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: combinedLabels,
            datasets: [
                { label: 'History', data: hPrices, borderColor: '#10b981', backgroundColor: 'rgba(16, 185, 129, 0.1)', fill: true, tension: 0.4, pointRadius: 0 },
                { label: 'Forecast', data: [...paddedFc, ...(fPrices||[])], borderColor: '#f59e0b', borderDash: [4,4], tension: 0.4, pointRadius: 0 }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: {display:false} },
            scales: { x: {display:false}, y: {display:false} }
        }
    });
}

function initMap() {
    if(map) return;
    map = L.map('map', {zoomControl: false}).setView(STATE_CENTERS[currentState], 11);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', { attribution: '' }).addTo(map);
}

async function loadStations() {
    const res = await fetch(`${API_BASE}/stations?state=${currentState}`);
    stationData = await res.json();
    filterMap(""); // Draw all
}

// --- Drive Mode ---
function initDriveMode() {
    if ('wakeLock' in navigator) navigator.wakeLock.request('screen').then(w => wakeLock = w).catch(() => {});
    if (driveWatchId) navigator.geolocation.clearWatch(driveWatchId);
    
    const els = { speed: document.getElementById('drive-speed'), acc: document.getElementById('drive-acc'), name: document.querySelector('.big-name'), price: document.querySelector('.big-price'), meta: document.querySelector('.big-meta'), btn: document.getElementById('drive-nav-btn') };
    
    driveWatchId = navigator.geolocation.watchPosition(async (pos) => {
        const { speed, accuracy, latitude, longitude } = pos.coords;
        els.speed.innerHTML = `${Math.round((speed||0)*3.6)} <span class="text-sm font-medium text-slate-500">km/h</span>`;
        els.acc.innerText = accuracy < 20 ? "GPS Strong" : "GPS Weak";
        els.acc.className = `text-sm font-bold ${accuracy < 20 ? 'text-emerald-500' : 'text-amber-500'}`;
        
        try {
            const res = await fetch(`${API_BASE}/find_cheapest_nearby`, { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ latitude, longitude }) });
            const data = await res.json();
            if(data.length > 0) {
                const s = data[0];
                els.name.innerText = s.name;
                els.price.innerText = s.price.toFixed(1);
                els.meta.innerText = `${s.distance.toFixed(1)} km • ${s.brand}`;
                els.btn.style.opacity = '1'; els.btn.style.pointerEvents = 'auto';
                els.btn.href = `https://www.google.com/maps/dir/?api=1&destination=${encodeURIComponent(s.name + " " + s.suburb)}`;
            }
        } catch(e){}
    }, null, {enableHighAccuracy:true});
}

function stopDriveMode() {
    if(driveWatchId) navigator.geolocation.clearWatch(driveWatchId);
    if(wakeLock) wakeLock.release();
}

// --- Tools Logic (Planner, Analytics, etc.) ---
function initPlannerMap() {
    if(!plannerMap) {
        plannerMap = L.map('planner-map', {zoomControl:false}).setView(STATE_CENTERS[currentState], 9);
        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png').addTo(plannerMap);
    }
    setTimeout(() => plannerMap.invalidateSize(), 200);
}

async function runPlanner() {
    const start = document.getElementById('planner-start').value;
    const end = document.getElementById('planner-end').value;
    const btn = document.getElementById('planner-btn');
    const resDiv = document.getElementById('planner-results');
    
    if(!start || !end) return;
    btn.innerText = "Calculating...";
    
    try {
        const res = await fetch(`${API_BASE}/planner`, { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({start, end}) });
        const data = await res.json();
        
        if(data.stations) {
            let html = `<div class="mt-4 space-y-2">`;
            data.stations.forEach(s => {
                html += `<div class="flex justify-between p-3 bg-slate-800 rounded-lg"><span class="text-sm text-slate-300">${s.name}</span><span class="text-emerald-400 font-bold">${s.price_cpl}c</span></div>`;
            });
            html += `</div>`;
            resDiv.innerHTML = html;
        }
    } catch(e) { resDiv.innerHTML = "Error calculating route."; }
    btn.innerText = "Find Fuel Stops";
}

async function loadRatings() {
    const res = await fetch(`${API_BASE}/stations?state=${currentState}`);
    const data = await res.json();
    const render = (items, id) => {
        document.getElementById(id).innerHTML = `<table class="w-full text-sm text-left text-slate-400">
            <thead class="text-xs uppercase bg-slate-700/50 text-slate-300"><tr><th class="px-4 py-2">Station</th><th class="px-4 py-2">Price</th></tr></thead>
            <tbody>${items.map(s => `<tr class="border-b border-slate-700/50"><td class="px-4 py-2 text-white">${s.name}</td><td class="px-4 py-2 text-emerald-400 font-bold">${s.price}c</td></tr>`).join('')}</tbody>
        </table>`;
    };
    render(data.sort((a,b) => a.price - b.price).slice(0, 10), 'table-best');
    render(data.sort((a,b) => b.price - a.price).slice(0, 10), 'table-worst');
}

async function loadAnalytics() {
    // Re-use logic for chart but specific for Analytics ID
    // Simple placeholder for chart init if needed
}

async function loadSentiment() {
    const res = await fetch(`${API_BASE}/sentiment`);
    const data = await res.json();
    const render = (list, id) => {
        document.getElementById(id).innerHTML = list.map(n => `
            <a href="${n.link}" target="_blank" class="block p-3 bg-slate-800 rounded-xl hover:bg-slate-700 transition">
                <div class="text-sm font-bold text-white mb-1">${n.title}</div>
                <div class="flex justify-between text-xs text-slate-500"><span>${n.publisher}</span><span class="${n.sentiment.includes('High')?'text-red-400':'text-emerald-400'}">${n.sentiment}</span></div>
            </a>
        `).join('');
    };
    render(data.global, 'feed-global');
    render(data.domestic, 'feed-domestic');
}