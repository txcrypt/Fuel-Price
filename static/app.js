// --- Configuration & State ---
const API_BASE = '/api';
let map, markers = [];
let trendChart = null;

// --- Init ---
document.addEventListener('DOMContentLoaded', () => {
    initNavigation();
    initLiveView(); // Load default tab
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
            
            // Data Load
            loadViewData(viewId);
        });
    });
}

function loadViewData(viewId) {
    switch(viewId) {
        case 'live': initLiveView(); break;
        case 'sentiment': loadSentiment(); break;
        case 'ratings': loadRatings(); break;
        case 'planner': loadPlanner(); break;
        case 'analytics': loadAnalytics(); break;
        case 'data': loadDataStatus(); break;
        case 'sandbox': initSandbox(); break;
    }
}

// --- Tab 4: Planner ---
function loadPlanner() {
    const btn = document.querySelector('#view-planner button');
    
    // Remove old listeners to avoid dupes
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
            
            if (data.error) {
                alert(data.error);
            } else {
                // Show Results
                let html = `<div style="margin-top:20px; padding:15px; background:rgba(255,255,255,0.05); border-radius:8px;">
                    <h3>Route: ${data.start.name} ➝ ${data.end.name}</h3>
                    <p>Distance: ${data.distance_km.toFixed(1)} km</p>
                </div>`;
                
                if (data.stations.length > 0) {
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
                    html += `<p>No suitable stations found along route.</p>`;
                }
                
                // Append to view
                const container = document.querySelector('#view-planner .card');
                const oldRes = document.getElementById('planner-results');
                if (oldRes) oldRes.remove();
                
                const resDiv = document.createElement('div');
                resDiv.id = 'planner-results';
                resDiv.innerHTML = html;
                container.appendChild(resDiv);
            }
        } catch (e) {
            console.error(e);
            alert("Planning failed. See console.");
        } finally {
            newBtn.disabled = false;
            newBtn.innerText = "Find Cheapest Stops";
        }
    });
}

// --- Tab 5: Analytics ---
async function loadAnalytics() {
    const res = await fetch(`${API_BASE}/analytics`);
    const data = await res.json();

    // Suburb Table
    document.getElementById('table-suburbs').innerHTML = `
        <table>
            <tr><th>Suburb</th><th>Avg Price</th></tr>
            ${data.suburb_ranking.map(r => `
                <tr><td>${r.suburb}</td><td>${r.price_cpl.toFixed(1)}c</td></tr>
            `).join('')}
        </table>
    `;

    // Chart
    const ctx = document.getElementById('analyticsChart').getContext('2d');
    if (trendChart) trendChart.destroy();

    const history = data.trend.history || {date:[], tgp:[]};
    const forecast = data.trend.forecast_tgp; // Single value? Or SARIMAX?
    // Check structure from tgp_forecast.py
    const sx = data.trend.sarimax || {forecast_dates:[], forecast_mean:[]};

    // Combine for continuous line
    const labels = [...history.date.map(d => d.split('T')[0]), ...sx.forecast_dates.map(d => d.split('T')[0])];
    const histData = history.tgp;
    
    // Pad forecast
    const forecastData = new Array(histData.length).fill(null);
    // Link last point
    if (histData.length > 0) {
        forecastData[histData.length-1] = histData[histData.length-1];
    }
    sx.forecast_mean.forEach(v => forecastData.push(v));

    trendChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Historical TGP',
                    data: histData,
                    borderColor: '#3b82f6',
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.3
                },
                {
                    label: 'Forecast',
                    data: forecastData,
                    borderColor: '#f59e0b',
                    borderDash: [5,5],
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.3
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { y: { grid: { color: '#334155' } }, x: { display: false } }
        }
    });
}

// --- Tab 7: Sandbox ---
function initSandbox() {
    const btn = document.querySelector('#view-sandbox button');
    
    // Replace logic
    const newBtn = btn.cloneNode(true);
    btn.parentNode.replaceChild(newBtn, btn);
    newBtn.disabled = false; // Enable for demo
    newBtn.innerText = "Run Backtest (on current data)";
    
    newBtn.addEventListener('click', async () => {
        newBtn.disabled = true;
        newBtn.innerText = "Running Simulation...";
        
        try {
            const res = await fetch(`${API_BASE}/sandbox/backtest`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({threshold: 5.0})
            });
            const data = await res.json();
            
            if (data.metrics) {
                const m = data.metrics;
                const html = `<div style="margin-top:20px; display:grid; grid-template-columns:1fr 1fr; gap:10px;">
                    <div style="background:#1e293b; padding:10px; border-radius:4px;">Accuracy: ${(m.accuracy*100).toFixed(1)}%</div>
                    <div style="background:#1e293b; padding:10px; border-radius:4px;">F1 Score: ${(m.f1_score).toFixed(2)}</div>
                    <div style="background:#1e293b; padding:10px; border-radius:4px; color:#10b981">Correct Hikes: ${m.confusion.tp}</div>
                    <div style="background:#1e293b; padding:10px; border-radius:4px; color:#ef4444">False Alarms: ${m.confusion.fp}</div>
                </div>`;
                
                const container = document.querySelector('#view-sandbox .card');
                let oldRes = document.getElementById('sandbox-results');
                if (oldRes) oldRes.remove();
                
                const resDiv = document.createElement('div');
                resDiv.id = 'sandbox-results';
                resDiv.innerHTML = html;
                container.appendChild(resDiv);
            }
            
        } catch (e) {
            alert("Backtest failed: " + e);
        } finally {
            newBtn.disabled = false;
            newBtn.innerText = "Run Backtest (on current data)";
        }
    });
}

// --- Tab 1: Live Market ---
async function initLiveView() {
    try {
        const res = await fetch(`${API_BASE}/market-status`);
        const data = await res.json();
        
        // Ticker
        const ticker = document.getElementById('global-ticker');
        ticker.innerHTML = `
            <span>🛢️ BRENT: $${data.ticker.oil.toFixed(2)}</span>
            <span>🏭 TGP: ${data.ticker.tgp.toFixed(1)}c</span>
            <span>⛽ MOGAS 95: $${data.ticker.mogas.toFixed(2)}</span>
            <span>🏛️ EXCISE: ${(data.ticker.excise * 100).toFixed(1)}c</span>
            <span>📉 TREND: ${data.status}</span>
        `;

        // KPIs
        document.getElementById('status-text').innerText = data.status;
        document.getElementById('tgp-val').innerText = `${data.ticker.tgp.toFixed(1)}c`;
        
        const badge = document.getElementById('advice-badge');
        badge.innerText = data.advice;
        badge.style.background = data.advice_type === 'error' ? '#ef4444' : (data.advice_type === 'success' ? '#10b981' : '#3b82f6');
        
        document.getElementById('hike-prediction').innerText = `Est. Next Hike: ${data.next_hike_est} (Cycle Day ${data.days_elapsed})`;

        // Map
        initMap();
        loadStations();
        
        // Calculator
        setupCalculator(data.ticker.tgp);

    } catch (e) { console.error(e); }
}

function initMap() {
    if (map) return;
    map = L.map('map').setView([-27.47, 153.02], 11);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; CARTO'
    }).addTo(map);
}

async function loadStations() {
    const res = await fetch(`${API_BASE}/stations`);
    const stations = await res.json();
    
    // Clear old markers
    markers.forEach(m => map.removeLayer(m));
    markers = [];

    stations.forEach(s => {
        const color = s.is_cheap ? '#10b981' : '#ef4444';
        const m = L.circleMarker([s.lat, s.lng], {
            radius: 7, fillColor: color, color: '#fff', weight: 1, fillOpacity: 0.8
        }).addTo(map);
        
        m.bindPopup(`<b>${s.brand}</b><br>${s.name}<br><b style="color:${color}">${s.price}c</b>`);
        markers.push(m);
    });
}

function setupCalculator(tgp) {
    const calc = () => {
        const tank = document.getElementById('calc-tank').value;
        const potentialSave = 40; // Simplified logic
        const total = (potentialSave * tank) / 100;
        document.getElementById('calc-result').innerHTML = `Potential savings of <b>$${total.toFixed(2)}</b> by timing the cycle correctly.`;
    };
    document.getElementById('calc-tank').addEventListener('change', calc);
    calc();
}

// --- Tab 2: Sentiment ---
async function loadSentiment() {
    const res = await fetch(`${API_BASE}/sentiment`);
    const data = await res.json();
    
    const moodEl = document.getElementById('mood-val');
    moodEl.innerText = data.mood;
    moodEl.style.color = data.color || '#fff';
    document.getElementById('mood-score').innerText = `Score: ${data.score}/10`;
    
    const feed = document.getElementById('news-feed');
    feed.innerHTML = data.articles.map(a => `
        <div class="news-item">
            <div class="news-title"><a href="${a.link}" target="_blank" style="color:inherit;text-decoration:none;">${a.title}</a></div>
            <div class="news-meta">
                <span>${a.publisher}</span>
                <span style="color:${a.sentiment === 'Bullish' ? '#ef4444' : '#10b981'}">${a.sentiment}</span>
            </div>
        </div>
    `).join('');
}

// --- Tab 3: Ratings ---
async function loadRatings() {
    const res = await fetch(`${API_BASE}/stations`);
    let stations = await res.json();
    
    const renderTable = (data, container) => {
        document.getElementById(container).innerHTML = `
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

    // Best 10
    const best = [...stations].sort((a, b) => a.fairness_score - b.fairness_score).slice(0, 10);
    renderTable(best, 'table-best');

    // Worst 10
    const worst = [...stations].sort((a, b) => b.fairness_score - a.fairness_score).slice(0, 10);
    renderTable(worst, 'table-worst');
}

// --- Tab 5: Analytics ---
async function loadAnalytics() {
    const res = await fetch(`${API_BASE}/analytics`);
    const data = await res.json();

    // Suburb Table
    document.getElementById('table-suburbs').innerHTML = `
        <table>
            <tr><th>Suburb</th><th>Avg Price</th></tr>
            ${data.suburb_ranking.map(r => `
                <tr><td>${r.suburb}</td><td>${r.price_cpl.toFixed(1)}c</td></tr>
            `).join('')}
        </table>
    `;

    // Chart
    const ctx = document.getElementById('analyticsChart').getContext('2d');
    if (trendChart) trendChart.destroy();

    const history = data.trend.history;
    trendChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: history.date.map(d => d.split('T')[0]),
            datasets: [{
                label: 'TGP History',
                data: history.tgp,
                borderColor: '#3b82f6',
                borderWidth: 2,
                pointRadius: 0,
                tension: 0.3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { y: { grid: { color: '#334155' } }, x: { display: false } }
        }
    });
}

// --- Tab 6: Data ---
async function loadDataStatus() {
    const res = await fetch(`${API_BASE}/collect-status`);
    const data = await res.json();
    document.getElementById('data-status').innerHTML = `
        <b>File:</b> ${data.file}<br>
        <b>Last Run:</b> ${data.last_run}
    `;
    
    document.getElementById('btn-collect').onclick = async () => {
        const btn = document.getElementById('btn-collect');
        btn.disabled = true;
        btn.innerText = "Collecting...";
        await fetch(`${API_BASE}/trigger-collect`, { method: 'POST' });
        loadDataStatus();
        btn.disabled = false;
        btn.innerText = "Trigger Live Snapshot";
    };
}