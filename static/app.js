
// --- Configuration & State ---
const API_BASE = '/api';
let map, markers = [];
let trendChart = null;

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
    initNavigation();
    initLiveView(); 
});

// --- Navigation ---
function initNavigation() {
    const navItems = document.querySelectorAll('.nav-item');
    navItems.forEach(item => {
        item.addEventListener('click', () => {
            // UI Toggle
            navItems.forEach(i => i.classList.remove('active'));
            item.classList.add('active');
            
            const viewId = item.getAttribute('data-view');
            document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
            document.getElementById(`view-${viewId}`).classList.add('active');
            
            // Map Resize Fix (Leaflet needs this when tab becomes visible)
            if (viewId === 'live' && map) {
                setTimeout(() => map.invalidateSize(), 100);
            }

            // Data Load with Error Handling
            loadViewData(viewId);
        });
    });
}

function loadViewData(viewId) {
    try {
        switch(viewId) {
            case 'live': initLiveView(); break;
            case 'sentiment': loadSentiment(); break;
            case 'ratings': loadRatings(); break;
            case 'planner': loadPlanner(); break;
            case 'analytics': loadAnalytics(); break;
            case 'data': loadDataStatus(); break;
            case 'sandbox': initSandbox(); break;
        }
    } catch (e) {
        console.error("View Load Error:", e);
    }
}

// --- Tab 1: Live Market ---
async function initLiveView() {
    try {
        const res = await fetch(`${API_BASE}/market-status`);
        if (!res.ok) throw new Error("API Error");
        const data = await res.json();
        
        if (!data || !data.ticker) {
            document.getElementById('status-text').innerText = "No Data";
            return;
        }

        // Ticker
        document.getElementById('global-ticker').innerHTML = `
            <span>🛢️ BRENT: $${(data.ticker.oil||0).toFixed(2)}</span>
            <span>🏭 TGP: ${(data.ticker.tgp||0).toFixed(1)}c</span>
            <span>⛽ MOGAS 95: $${(data.ticker.mogas||0).toFixed(2)}</span>
            <span>🏛️ EXCISE: ${(data.ticker.excise * 100||0).toFixed(1)}c</span>
            <span>📉 TREND: ${data.status}</span>
        `;

        // KPIs
        document.getElementById('status-text').innerText = data.status || "--";
        document.getElementById('tgp-val').innerText = `${(data.ticker.tgp||0).toFixed(1)}c`;
        
        const badge = document.getElementById('advice-badge');
        badge.innerText = data.advice || "Hold";
        badge.style.background = data.advice_type === 'error' ? '#ef4444' : (data.advice_type === 'success' ? '#10b981' : '#3b82f6');
        
        document.getElementById('hike-prediction').innerText = `Est. Next Hike: ${data.next_hike_est || "?"}`;

        // Map
        initMap();
        loadStations();
        
        // Calculator
        setupCalculator(data.ticker.tgp);

    } catch (e) { 
        console.error(e);
        document.getElementById('status-text').innerText = "Offline"; 
    }
}

function initMap() {
    if (map) return;
    const mapEl = document.getElementById('map');
    if (!mapEl) return;
    
    map = L.map('map').setView([-27.47, 153.02], 11);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; CARTO'
    }).addTo(map);
}

async function loadStations() {
    try {
        const res = await fetch(`${API_BASE}/stations`);
        const stations = await res.json();
        
        if (!map) return;
        
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
                    
                    m.bindPopup(`<b>${s.brand}</b><br>${s.name}<br><b style="color:${color}">${s.price}c</b>`);
                    markers.push(m);
                }
            });
        }
    } catch(e) { console.warn("Station load failed", e); }
}

function setupCalculator(tgp) {
    const calc = () => {
        const tank = document.getElementById('calc-tank').value;
        const potentialSave = 25; // Default assumption
        const total = (potentialSave * tank) / 100;
        document.getElementById('calc-result').innerHTML = `Potential savings of <b>$${total.toFixed(2)}</b>`;
    };
    const inp = document.getElementById('calc-tank');
    if(inp) { inp.removeEventListener('change', calc); inp.addEventListener('change', calc); calc(); }
}

// --- Tab 3: Ratings ---
async function loadRatings() {
    try {
        const res = await fetch(`${API_BASE}/stations`);
        let stations = await res.json();
        if (!Array.isArray(stations)) stations = [];
        
        const renderTable = (data, container) => {
            const el = document.getElementById(container);
            if(!el) return;
            if(data.length === 0) { el.innerHTML = "<p style='padding:1rem; opacity:0.5'>No data available.</p>"; return; }
            
            el.innerHTML = `
                <table>
                    <tr><th>Station</th><th>Suburb</th><th>Price</th><th>Rating</th></tr>
                    ${data.map(s => `
                        <tr>
                            <td>${s.name}</td>
                            <td>${s.suburb}</td>
                            <td>${s.price}c</td>
                            <td>${s.rating}</td>
                        </tr>
                    `).join('')}
                </table>
            `;
        };

        const best = [...stations].sort((a, b) => b.fairness_score - a.fairness_score).slice(0, 10);
        renderTable(best, 'table-best');

        const worst = [...stations].sort((a, b) => a.fairness_score - b.fairness_score).slice(0, 10);
        renderTable(worst, 'table-worst');
    } catch(e) { console.error(e); }
}

// --- Tab 4: Planner ---
function loadPlanner() {
    const btn = document.querySelector('#view-planner button');
    if (!btn) return;
    
    // Simple way to avoid duplicate listeners: clone
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
                
                if (data.stations && data.stations.length > 0) {
                    html += `<table style="margin-top:20px;">
                        <tr><th>Station</th><th>Price</th><th>Utility</th></tr>
                        ${data.stations.map(s => `
                            <tr>
                                <td>${s.name}</td>
                                <td style="font-weight:bold; color:${s.price_cpl < data.market_avg ? '#10b981' : '#fff'}">${s.price_cpl}c</td>
                                <td>$${s.net_utility.toFixed(2)}</td>
                            </tr>
                        `).join('')}
                    </table>`;
                } else {
                    html += `<p style="margin-top:10px; opacity:0.7">No suitable stations found along route.</p>`;
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

// --- Other Stubs ---
async function loadSentiment() { 
    // Basic Stub
    const el = document.getElementById('mood-val');
    if(el) el.innerText = "Neutral";
}
async function loadAnalytics() {
    // Basic Stub for robustness
    const el = document.getElementById('table-suburbs');
    if(el) el.innerHTML = "Loading...";
    try {
        const res = await fetch(`${API_BASE}/analytics`);
        const data = await res.json();
        // ... (Chart logic skipped for brevity, but won't crash)
        if(el && data.suburb_ranking) {
             el.innerHTML = `<table><tr><th>Suburb</th><th>Price</th></tr>${data.suburb_ranking.map(r=>`<tr><td>${r.suburb}</td><td>${r.price_cpl.toFixed(1)}</td></tr>`).join('')}</table>`;
        }
    } catch(e) {}
}
async function loadDataStatus() {
    const res = await fetch(`${API_BASE}/collect-status`);
    const data = await res.json();
    document.getElementById('data-status').innerHTML = `<b>File:</b> ${data.file}<br><b>Last Run:</b> ${data.last_run}`;
    
    const btn = document.getElementById('btn-collect');
    const newBtn = btn.cloneNode(true);
    btn.parentNode.replaceChild(newBtn, btn);
    
    newBtn.addEventListener('click', async () => {
        newBtn.disabled = true; 
        newBtn.innerText = "Collecting (Wait 10s)...";
        await fetch(`${API_BASE}/trigger-collect`, { method: 'POST' });
        loadDataStatus();
        newBtn.disabled = false;
        newBtn.innerText = "Trigger Live Snapshot";
    });
}
function initSandbox() {}
