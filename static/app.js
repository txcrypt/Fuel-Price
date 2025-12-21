// --- Configuration ---
const API_BASE = '/api';

// --- State ---
let marketAvg = 180.0; // Fallback
let currentStatus = "UNKNOWN";

// --- Initialization ---
document.addEventListener('DOMContentLoaded', () => {
    initDashboard();
});

async function initDashboard() {
    updateTimestamp();
    
    // 1. Fetch Market Status
    await fetchMarketStatus();
    
    // 2. Fetch Stations & Render Map
    await fetchStations();
    
    // 3. Fetch Analytics & Render Chart
    await fetchAnalytics();
    
    // 4. Setup Calculator Listeners
    setupCalculator();
}

function updateTimestamp() {
    const now = new Date();
    document.getElementById('last-updated').innerText = `Updated: ${now.toLocaleTimeString()}`;
}

// --- 1. Market Status ---
async function fetchMarketStatus() {
    try {
        const res = await fetch(`${API_BASE}/market-status`);
        const data = await res.json();
        
        // Update UI
        const badge = document.getElementById('cycle-badge');
        const advice = document.getElementById('advice-text');
        
        badge.innerText = data.status;
        badge.className = 'status-badge'; // Reset
        
        // Apply styling based on type
        if (data.advice_type === 'error') badge.classList.add('status-fill');
        else if (data.advice_type === 'warning') badge.classList.add('status-warn');
        else if (data.advice_type === 'success') badge.classList.add('status-buy');
        else badge.classList.add('status-wait');
        
        advice.innerText = data.advice;
        document.getElementById('next-hike').innerText = data.next_hike_est || 'Unknown';
        document.getElementById('cycle-day').innerText = `Day ${data.days_elapsed} of ~${Math.round(data.avg_cycle_length)}`;
        
        currentStatus = data.status;
        
    } catch (e) {
        console.error("Status fetch failed", e);
    }
}

// --- 2. Map & Stations ---
async function fetchStations() {
    try {
        const res = await fetch(`${API_BASE}/stations`);
        const stations = await res.json();
        
        if (!stations || stations.length === 0) return;
        
        // Calculate stats
        const prices = stations.map(s => s.price).filter(p => p > 100); // Filter errors
        if (prices.length > 0) {
            const minPrice = Math.min(...prices);
            const sum = prices.reduce((a, b) => a + b, 0);
            marketAvg = sum / prices.length;
            
            document.getElementById('best-price').innerText = `$${(minPrice/100).toFixed(2)}`;
            document.getElementById('market-avg').innerText = `Market Avg: ${(marketAvg).toFixed(1)}c`;
            
            // Trigger calculator update now that we have data
            calculateSavings(); 
        }

        // Init Map
        const map = L.map('map').setView([-27.47, 153.02], 11); // Brisbane Center
        
        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
            subdomains: 'abcd',
            maxZoom: 19
        }).addTo(map);

        // Add Markers
        stations.forEach(s => {
            const color = s.is_cheap ? '#10b981' : '#ef4444';
            
            const circle = L.circleMarker([s.lat, s.lng], {
                radius: 8,
                fillColor: color,
                color: '#fff',
                weight: 1,
                opacity: 1,
                fillOpacity: 0.8
            }).addTo(map);
            
            circle.bindPopup(`
                <div style="text-align:center;">
                    <strong>${s.brand}</strong><br>
                    ${s.name}<br>
                    <span style="font-size: 1.2rem; font-weight:bold; color:${color}">
                        ${s.price.toFixed(1)}c
                    </span><br>
                    <small>Fairness: ${s.rating}</small>
                </div>
            `);
        });

    } catch (e) {
        console.error("Station fetch failed", e);
        document.getElementById('map').innerHTML = '<p style="text-align:center; padding:50px;">Map Data Unavailable</p>';
    }
}

// --- 3. Analytics Chart ---
async function fetchAnalytics() {
    try {
        const res = await fetch(`${API_BASE}/analytics`);
        const data = await res.json();
        
        const ctx = document.getElementById('trendChart').getContext('2d');
        
        // Prepare Data
        const history = data.history || [];
        const forecast = data.forecast || [];
        
        const labels = [...history.map(d => d.date), ...forecast.map(d => d.date)];
        const histData = history.map(d => d.value);
        
        // Pad forecast with nulls for history part
        const forecastData = new Array(history.length).fill(null);
        // Connect lines: add last history point to forecast
        if (history.length > 0) {
             forecastData[history.length - 1] = history[history.length - 1].value;
        }
        forecast.forEach(d => forecastData.push(d.mean));

        new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Historical TGP',
                        data: histData,
                        borderColor: '#94a3b8',
                        borderWidth: 2,
                        tension: 0.4,
                        pointRadius: 0
                    },
                    {
                        label: 'Forecast TGP',
                        data: forecastData,
                        borderColor: '#3b82f6',
                        borderDash: [5, 5],
                        borderWidth: 2,
                        tension: 0.4,
                        pointRadius: 0
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
                plugins: {
                    legend: {
                        labels: { color: '#cbd5e1' }
                    }
                },
                scales: {
                    y: {
                        grid: { color: '#334155' },
                        ticks: { color: '#cbd5e1' }
                    },
                    x: {
                        grid: { display: false },
                        ticks: { 
                            color: '#cbd5e1',
                            maxTicksLimit: 8 
                        }
                    }
                }
            }
        });

    } catch (e) {
        console.error("Analytics fetch failed", e);
    }
}

// --- 4. Calculator Logic ---
function setupCalculator() {
    const selector = document.getElementById('tank-size');
    selector.addEventListener('change', calculateSavings);
}

function calculateSavings() {
    const tankSize = parseInt(document.getElementById('tank-size').value);
    const resultDiv = document.getElementById('savings-result');
    
    // Logic: 
    // If prices are high (Margin > 20c), potential save is small (waiting for drop).
    // If prices are low (Margin < 5c), potential save is avoiding the hike (saving ~40c/L).
    
    // Use a rough estimate if TGP unknown, else usage marketAvg - 170 (approx TGP)
    const estTGP = 172.5; 
    const currentMargin = marketAvg - estTGP;
    
    let potentialSavePerL = 0;
    let msg = "";
    let color = "#fff";
    
    if (currentMargin < 10) {
        // Low margin -> Hike coming -> Save by filling NOW
        potentialSavePerL = 40.0; // Avg hike jump
        const total = (potentialSavePerL * tankSize) / 100;
        msg = `Prices are LOW. Fill now to save approx <strong>$${total.toFixed(2)}</strong> before the hike!`;
        color = "#10b981"; // Green
    } else if (currentMargin > 25) {
        // High margin -> Prices dropping -> Save by WAITING
        potentialSavePerL = currentMargin - 10; // Drop to reasonable bottom
        const total = (potentialSavePerL * tankSize) / 100;
        msg = `Prices are HIGH. Wait to save approx <strong>$${total.toFixed(2)}</strong>.`;
        color = "#f59e0b"; // Orange
    } else {
        msg = "Market is stable. Small savings only.";
    }
    
    resultDiv.innerHTML = msg;
    resultDiv.style.borderLeft = `4px solid ${color}`;
}
