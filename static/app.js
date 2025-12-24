// --- Configuration & State ---
const API_BASE = '/api';
let map, plannerMap;
let markers = [];
let plannerLayer = null;
let trendChart = null;
let cycleChart = null;

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
    initNavigation();
    initLiveView(); 
    initCalculator();
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
            
            // Map Resize Fixes
            if (viewId === 'live' && map) {
                setTimeout(() => map.invalidateSize(), 100);
            }
            if (viewId === 'planner' && plannerMap) {
                setTimeout(() => plannerMap.invalidateSize(), 100);
            }

            // Data Load
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

        // Cycle Chart
        if (data.history && data.history.dates) {
            const ctx = document.getElementById('cycleChart');
            if (ctx) {
                if (cycleChart) cycleChart.destroy();
                cycleChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: data.history.dates,
                        datasets: [{
                            label: 'Avg Price (cpl)',
                            data: data.history.prices,
                            borderColor: '#10b981',
                            backgroundColor: 'rgba(16, 185, 129, 0.1)',
                            fill: true,
                            tension: 0.4,
                            pointRadius: 0,
                            pointHitRadius: 10
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        interaction: { intersect: false, mode: 'index' },
                        plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => `${c.raw.toFixed(1)}c` } } },
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
        
        // Calculator Update
        updateCalculator(data.ticker.tgp);

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
                    
                    m.bindPopup(`<b>${s.brand}</b><br>${s.name}<br>${s.suburb}<br><b style="color:${color}">${s.price}c</b>`);
                    markers.push(m);
                }
            });
        }
    } catch(e) { console.warn("Station load failed", e); }
}

function initCalculator() {
    const toggle = document.getElementById('toggle-calc-details');
    if (toggle) {
        toggle.addEventListener('click', (e) => {
            e.preventDefault();
            const box = document.getElementById('calc-details-box');
            if (box) box.style.display = box.style.display === 'block' ? 'none' : 'block';
        });
    }
    
    const inp = document.getElementById('calc-tank');
    if(inp) inp.addEventListener('change', () => updateCalculator());
}

function updateCalculator(tgp) {
    const inp = document.getElementById('calc-tank');
    const res = document.getElementById('calc-result');
    if(!inp || !res) return;
    
    const tank = inp.value;
    const potentialSave = 25; // Default assumption
    const total = (potentialSave * tank) / 100;
    res.innerHTML = `Potential Savings: $${total.toFixed(2)}`;
}

// --- Tab 2: Sentiment ---
async function loadSentiment() { 
    try {
        const res = await fetch(`${API_BASE}/sentiment`);
        const data = await res.json();
        
        const moodVal = document.getElementById('mood-val');
        if (moodVal) {
            moodVal.innerText = data.mood || "Unknown";
            moodVal.style.color = data.color || "#fff";
        }
        document.getElementById('mood-score').innerText = `Score: ${data.score !== undefined ? data.score : '--'}/10`;
        
        const feed = document.getElementById('news-feed');
        if (data.articles && data.articles.length > 0) {
            feed.innerHTML = data.articles.map(a => `
                <div class="news-item">
                    <div class="news-title"><a href="${a.link}" target="_blank" style="color:${data.color};text-decoration:none;">${a.title}</a></div>
                    <div class="news-meta">
                        <span>${a.publisher}</span>
                        <span>${a.published.substring(0, 16)}</span>
                        <span style="color:${a.sentiment.includes('High') ? '#ef4444' : '#10b981'}">${a.sentiment}</span>
                    </div>
                </div>
            `).join('');
        } else {
            feed.innerHTML = "<p style='padding:10px; opacity:0.6;'>No news available right now.</p>";
        }
    } catch(e) {
        console.error(e);
        document.getElementById('news-feed').innerHTML = "<p>Failed to load news.</p>";
    }
}

// --- Tab 3: Ratings ---
async function loadRatings() {
    try {
        const res = await fetch(`${API_BASE}/stations`);
        let stations = await res.json();
        if (!Array.isArray(stations)) stations = [];
        
        const renderTable = (data, container, isBest) => {
            const el = document.getElementById(container);
            if(!el) return;
            if(data.length === 0) { el.innerHTML = "<p style='padding:1rem; opacity:0.5'>No data available.</p>"; return; }
            
            el.innerHTML = `
                <p style="font-size:0.8rem; color:#94a3b8; margin-bottom:10px;">
                    ${isBest ? '📉 Lower score = Better value' : '📈 Higher score = More expensive'}
                </p>
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
            plannerMap = L.map('planner-map').setView([-27.47, 153.02], 10);
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
        const res = await fetch(`${API_BASE}/analytics`);
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
            
            const historyDates = data.trend.history.date || [];
            const historyTgp = data.trend.history.tgp || [];
            
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