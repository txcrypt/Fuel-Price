/**
 * Fuel AI - Main Application Logic
 * Modern SPA architecture handling API integrations, UI state, and mapping.
 */

const app = (function() {
    // --- State ---
    const state = {
        currentStateCode: 'QLD',
        activeView: 'live',
        activeIntelTab: 'context',
        marketStatus: null,
        stations: [],
        analytics: null,
        sentiment: null,
        context: null,
        supply: null,
        tankers: null,
        advancedBriefing: null,
        advancedUnlocked: false,
        map: null,
        markers: [],
        plannerMap: null,
        plannerLayer: null,
        tankerMap: null,
        miniChart: null,
        mainChart: null,
        driveWatchId: null,
        driveTargetStation: null
    };

    // --- Configuration ---
    const CONFIG = {
        apiBase: '/api',
        mapCenters: {
            "QLD": [-27.470, 153.020],
            "NSW": [-33.868, 151.209],
            "VIC": [-37.813, 144.963],
            "SA":  [-34.928, 138.600],
            "WA":  [-31.950, 115.860],
            "ACT": [-35.280, 149.130],
            "TAS": [-42.882, 147.327],
            "NT":  [-12.463, 130.844]
        },
        refreshInterval: 60000 // 1 minute
    };

    // --- Utilities ---
    const showToast = (message, type = 'info') => {
        const container = document.getElementById('toast-container');
        if (!container) return;
        
        const toast = document.createElement('div');
        toast.className = `toast border-l-4 ${type === 'error' ? 'border-red-500 text-red-100' : 'border-blue-500 text-white'}`;
        toast.innerHTML = message;
        
        container.appendChild(toast);
        
        // Trigger reflow for animation
        void toast.offsetWidth;
        toast.classList.add('show');
        
        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => toast.remove(), 300);
        }, 5000);
    };

    const formatPrice = (price) => {
        if (price === null || price === undefined || isNaN(price)) return '--.-';
        return Number(price).toFixed(1);
    };

    const escapeHtml = (value) => {
        return String(value ?? '').replace(/[&<>"']/g, (char) => ({
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        }[char]));
    };

    // --- Core API Layer ---
    const fetchApi = async (endpoint, options = {}) => {
        try {
            const res = await fetch(`${CONFIG.apiBase}${endpoint}`, options);
            if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
            return await res.json();
        } catch (error) {
            console.error(`API Error on ${endpoint}:`, error);
            showToast(`Error loading data from ${endpoint}`, 'error');
            return null;
        }
    };

    // --- Navigation & View Management ---
    const initNavigation = () => {
        // Main view navigation
        document.querySelectorAll('.nav-btn, .nav-btn-mobile').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const target = e.currentTarget.dataset.target;
                switchView(target);
                
                // Update active states
                document.querySelectorAll('.nav-btn').forEach(b => {
                    if(b.dataset.target === target) {
                        b.classList.add('bg-slate-700', 'text-white');
                        b.classList.remove('text-slate-400');
                    } else {
                        b.classList.remove('bg-slate-700', 'text-white');
                        b.classList.add('text-slate-400');
                    }
                });
                
                document.querySelectorAll('.nav-btn-mobile').forEach(b => {
                    if(b.dataset.target === target) {
                        b.classList.add('text-primary-500');
                        b.classList.remove('text-slate-500');
                    } else {
                        b.classList.remove('text-primary-500');
                        b.classList.add('text-slate-500');
                    }
                });
            });
        });

        // Intelligence sub-tab navigation
        document.querySelectorAll('.intel-tab-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const target = e.currentTarget.dataset.target;
                switchIntelTab(target);
            });
        });

        // State Selector
        const stateSelect = document.getElementById('state-selector');
        if (stateSelect) {
            stateSelect.addEventListener('change', (e) => {
                state.currentStateCode = e.target.value;
                state.advancedBriefing = null;
                loadAllData();
            });
        }

        initAdvancedControls();
    };

    const switchView = (viewId) => {
        if (state.activeView === viewId) return;
        
        // Hide all
        document.querySelectorAll('.view-section').forEach(el => el.classList.add('hidden'));
        
        // Show target
        const targetEl = document.getElementById(`view-${viewId}`);
        if (targetEl) targetEl.classList.remove('hidden');
        
        state.activeView = viewId;
        
        // View-specific actions
        if (viewId === 'live' && state.map) {
            setTimeout(() => state.map.invalidateSize(), 100);
        } else if (viewId === 'drive') {
            initDriveMode();
        } else if (viewId === 'intel') {
            // Lazy load intelligence components
            if (!state.context) loadMarketContext();
            if (!state.analytics) loadAnalytics();
        }
        
        // Clean up
        if (viewId !== 'drive') {
            stopDriveMode();
        }
    };

    const switchIntelTab = (tabId) => {
        // Update UI
        document.querySelectorAll('.intel-tab-btn').forEach(btn => {
            if(btn.dataset.target === tabId) {
                btn.classList.add('bg-primary-600', 'text-white');
                btn.classList.remove('bg-slate-800', 'text-slate-400');
            } else {
                btn.classList.remove('bg-primary-600', 'text-white');
                btn.classList.add('bg-slate-800', 'text-slate-400');
            }
        });

        // Hide all subviews
        document.querySelectorAll('.intel-subview').forEach(el => {
            el.classList.add('hidden');
            el.classList.remove('block', 'grid');
        });
        
        // Show target
        const targetEl = document.getElementById(`intel-sub-${tabId}`);
        if (targetEl) {
            if(tabId === 'ratings') targetEl.classList.add('grid');
            else targetEl.classList.add('block');
            targetEl.classList.remove('hidden');
        }

        state.activeIntelTab = tabId;

        // Lazy load data based on tab
        if (tabId === 'supply' && !state.supply) {
            loadSupplyData();
            loadTankers();
        } else if (tabId === 'news' && !state.sentiment) {
            loadSentiment();
        } else if (tabId === 'planner' && !state.plannerMap) {
            initPlannerMap();
        } else if (tabId === 'advanced') {
            initAdvancedMode();
        } else if (tabId === 'forecast' && state.mainChart) {
            // Chart.js bug requires explicit resize when unhidden
            setTimeout(() => state.mainChart.resize(), 50);
        }
    };

    // --- Maps ---
    const initMaps = () => {
        // Main Map
        if (document.getElementById('map') && !state.map) {
            state.map = L.map('map', { zoomControl: false }).setView(CONFIG.mapCenters[state.currentStateCode], 11);
            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                attribution: '© OpenStreetMap © CARTO',
                subdomains: 'abcd',
                maxZoom: 19
            }).addTo(state.map);
            L.control.zoom({ position: 'bottomright' }).addTo(state.map);
            
            // Map search
            const searchInput = document.getElementById('map-search');
            if (searchInput) {
                searchInput.addEventListener('keyup', (e) => filterMap(e.target.value.toLowerCase()));
            }
        }
    };

    const initPlannerMap = () => {
        if (document.getElementById('planner-map') && !state.plannerMap) {
            state.plannerMap = L.map('planner-map').setView(CONFIG.mapCenters[state.currentStateCode], 6);
            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
                attribution: '© OpenStreetMap © CARTO'
            }).addTo(state.plannerMap);
        }
    };

    const initTankerMap = () => {
        if (document.getElementById('tanker-map') && !state.tankerMap) {
            state.tankerMap = L.map('tanker-map', { zoomControl: false, attributionControl: false }).setView([-25.0, 135.0], 4);
            L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png').addTo(state.tankerMap);
        }
    };

    const filterMap = (term) => {
        if (!state.map) return;
        state.markers.forEach(m => state.map.removeLayer(m));
        state.markers = [];
        
        state.stations.forEach(s => {
            const txt = (`${s.name} ${s.suburb} ${s.brand}`).toLowerCase();
            if (term === "" || txt.includes(term)) {
                if (s.lat && s.lng) {
                    const color = s.is_cheap === 1 ? '#10b981' : (s.price > state.marketStatus?.current_median ? '#ef4444' : '#f59e0b');
                    const m = L.circleMarker([s.lat, s.lng], {
                        radius: 5, fillColor: color, color: '#0f172a', weight: 1.5, fillOpacity: 0.9
                    }).addTo(state.map);
                    m.bindPopup(`
                        <div class="p-2 min-w-[150px]">
                            <div class="font-bold text-slate-800 text-lg mb-1">${formatPrice(s.price)}<span class="text-xs">c</span></div>
                            <div class="font-semibold text-slate-700">${s.name}</div>
                            <div class="text-xs text-slate-500">${s.brand} • ${s.suburb}</div>
                        </div>
                    `);
                    state.markers.push(m);
                }
            }
        });
    };

    // --- Charts ---
    const renderMiniChart = (history, forecast) => {
        const ctx = document.getElementById('miniCycleChart');
        if (!ctx) return;
        
        if (state.miniChart) state.miniChart.destroy();
        
        // Combine history and forecast for simple rendering
        const labels = [...(history?.dates || []), ...(forecast?.dates || [])];
        const histData = [...(history?.prices || []), ...Array(forecast?.prices?.length || 0).fill(null)];
        const foreData = [...Array(history?.prices?.length || 0).fill(null), ...(forecast?.prices || [])];
        
        // Connect the lines
        if (history?.prices?.length > 0 && forecast?.prices?.length > 0) {
            foreData[history.prices.length - 1] = history.prices[history.prices.length - 1];
        }

        state.miniChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'History',
                        data: histData,
                        borderColor: '#10b981',
                        borderWidth: 2,
                        tension: 0.4,
                        pointRadius: 0
                    },
                    {
                        label: 'Forecast',
                        data: foreData,
                        borderColor: '#f59e0b',
                        borderWidth: 2,
                        borderDash: [5, 5],
                        tension: 0.4,
                        pointRadius: 0
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false }, tooltip: { enabled: false } },
                scales: {
                    x: { display: false },
                    y: { display: false, min: Math.min(...(history?.prices || [150])) - 10 }
                },
                interaction: { intersect: false, mode: 'index' }
            }
        });
    };

    const renderMainChart = (history, forecast) => {
        const ctx = document.getElementById('mainAnalyticsChart');
        if (!ctx) return;
        
        if (state.mainChart) state.mainChart.destroy();
        
        const labels = [...(history?.dates || []), ...(forecast?.forecast_dates || [])];
        const histData = [...(history?.values || []), ...Array(forecast?.forecast_mean?.length || 0).fill(null)];
        const foreData = [...Array(history?.values?.length || 0).fill(null), ...(forecast?.forecast_mean || [])];
        
        // Confidence bands
        const lowData = [...Array(history?.values?.length || 0).fill(null), ...(forecast?.forecast_low || [])];
        const highData = [...Array(history?.values?.length || 0).fill(null), ...(forecast?.forecast_high || [])];

        // Connect the lines
        if (history?.values?.length > 0 && forecast?.forecast_mean?.length > 0) {
            const lastHistIdx = history.values.length - 1;
            const lastHistVal = history.values[lastHistIdx];
            foreData[lastHistIdx] = lastHistVal;
            lowData[lastHistIdx] = lastHistVal;
            highData[lastHistIdx] = lastHistVal;
        }

        state.mainChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'History',
                        data: histData,
                        borderColor: '#10b981',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        borderWidth: 2,
                        tension: 0.2,
                        pointRadius: 0,
                        fill: true
                    },
                    {
                        label: 'Forecast P50',
                        data: foreData,
                        borderColor: '#f59e0b',
                        borderWidth: 2,
                        borderDash: [5, 5],
                        tension: 0.2,
                        pointRadius: 0
                    },
                    {
                        label: 'Forecast Low',
                        data: lowData,
                        borderColor: 'transparent',
                        backgroundColor: 'rgba(245, 158, 11, 0.1)',
                        fill: '+1', // Fill to high band
                        pointRadius: 0,
                        tension: 0.2
                    },
                    {
                        label: 'Forecast High',
                        data: highData,
                        borderColor: 'transparent',
                        pointRadius: 0,
                        tension: 0.2
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { 
                    legend: { display: false },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        backgroundColor: 'rgba(15, 23, 42, 0.9)',
                        titleColor: '#94a3b8',
                        bodyColor: '#fff',
                        borderColor: 'rgba(255,255,255,0.1)',
                        borderWidth: 1,
                        callbacks: {
                            label: function(context) {
                                if(context.dataset.label.includes('Low') || context.dataset.label.includes('High')) return null;
                                return `${context.dataset.label}: ${context.parsed.y.toFixed(1)} cpl`;
                            }
                        }
                    }
                },
                scales: {
                    x: { 
                        grid: { color: 'rgba(255,255,255,0.05)' },
                        ticks: { color: '#64748b', maxTicksLimit: 10 }
                    },
                    y: { 
                        grid: { color: 'rgba(255,255,255,0.05)' },
                        ticks: { 
                            color: '#64748b',
                            callback: function(value) { return value + 'c'; }
                        }
                    }
                },
                interaction: { intersect: false, mode: 'index' }
            }
        });
    };

    // --- Data Loaders ---
    
    const loadMarketStatus = async () => {
        const data = await fetchApi(`/market-status?state=${state.currentStateCode}`);
        if (!data) return;
        state.marketStatus = data;
        
        // Update Hero
        const stText = document.getElementById('status-text');
        if (stText) {
            stText.textContent = data.status.replace('_', ' ');
            stText.classList.remove('pulse-skeleton');
            
            // Color coding
            stText.className = stText.className.replace(/text-\w+-400/g, '');
            if(data.status === 'HIKE_IMMINENT') stText.classList.add('text-red-400');
            else if(data.status === 'WARNING') stText.classList.add('text-amber-400');
            else if(data.status === 'BOTTOM') stText.classList.add('text-emerald-400');
            else if(data.status === 'DROPPING') stText.classList.add('text-blue-400');
            else stText.classList.add('text-white');
        }

        const adBadge = document.getElementById('advice-badge');
        if (adBadge) {
            adBadge.textContent = data.advice;
            adBadge.classList.remove('opacity-0');
            adBadge.className = adBadge.className.replace(/bg-\w+-500\/20 text-\w+-400 border-\w+-500\/50/g, '');
            
            if(data.advice_type === 'success') adBadge.classList.add('bg-emerald-500/20', 'text-emerald-400', 'border-emerald-500/50');
            else if(data.advice_type === 'warning') adBadge.classList.add('bg-amber-500/20', 'text-amber-400', 'border-amber-500/50');
            else adBadge.classList.add('bg-blue-500/20', 'text-blue-400', 'border-blue-500/50');
        }

        const avgDisp = document.getElementById('avg-price-display');
        if(avgDisp) {
            avgDisp.textContent = formatPrice(data.current_avg);
            avgDisp.classList.remove('pulse-skeleton');
        }

        const saveIn = document.getElementById('savings-insight');
        if(saveIn) {
            saveIn.textContent = data.savings_insight;
            saveIn.classList.remove('pulse-skeleton');
        }

        const lu = document.getElementById('last-updated');
        if(lu) lu.textContent = `Updated ${data.last_updated}`;

        // Hike Gauge
        const hikeBar = document.getElementById('hike-gauge-bar');
        const hikeText = document.getElementById('hike-gauge-text');
        if(hikeBar && hikeText) {
            const prob = data.hike_probability || 0;
            hikeBar.style.width = `${prob}%`;
            hikeText.textContent = `${Math.round(prob)}%`;
            
            hikeBar.className = hikeBar.className.replace(/bg-\w+-500/g, '');
            if(prob > 70) hikeBar.classList.add('bg-red-500');
            else if(prob > 40) hikeBar.classList.add('bg-amber-500');
            else hikeBar.classList.add('bg-emerald-500');
        }

        // TGP
        const tgpVal = document.getElementById('tgp-val');
        if (tgpVal && data.ticker && data.ticker.tgp) {
            tgpVal.textContent = formatPrice(data.ticker.tgp);
            tgpVal.classList.remove('pulse-skeleton');
        }

        // Update Ticker
        const ticker = document.getElementById('nav-ticker');
        if (ticker && data.ticker) {
            const t = data.ticker;
            const content = `OIL $${t.oil} | AUD/USD ${t.fx} | TGP ${t.tgp}c | MOGAS ${t.mogas}c | PARITY: ${t.import_parity_lag}`;
            ticker.innerHTML = `<span class="ticker-content mr-8">${content}</span><span class="ticker-content mr-8">${content}</span><span class="ticker-content">${content}</span>`;
        }
        
        // Render Mini Chart
        renderMiniChart(data.history, data.forecast);
    };

    const loadStations = async () => {
        const data = await fetchApi(`/stations?state=${state.currentStateCode}`);
        if (!data) return;
        state.stations = data;
        
        if (state.map) {
            state.map.setView(CONFIG.mapCenters[state.currentStateCode], 10);
            filterMap(""); // Draw all
        }
    };

    const loadMarketContext = async () => {
        const data = await fetchApi(`/market-context?state=${state.currentStateCode}`);
        if (!data || data.error) return;
        state.context = data;

        // Populate Health Badge
        const badge = document.getElementById('mc-health-badge');
        if (badge) {
            badge.textContent = data.market_health.status;
            badge.className = badge.className.replace(/bg-\w+-500\/20 text-\w+-400/g, '');
            if(data.market_health.status === 'HEALTHY') badge.classList.add('bg-emerald-500/20', 'text-emerald-400');
            else if(data.market_health.status === 'VOLATILE') badge.classList.add('bg-red-500/20', 'text-red-400');
            else badge.classList.add('bg-amber-500/20', 'text-amber-400');
        }

        // Populate Narrative
        const narrative = document.getElementById('mc-narrative');
        if (narrative) narrative.textContent = data.narrative;

        // Populate Factors
        const factorsList = document.getElementById('mc-factors-list');
        if (factorsList && data.driving_factors) {
            factorsList.innerHTML = data.driving_factors.map(f => `
                <div class="bg-slate-800/50 rounded-xl p-3 border border-slate-700/50 flex items-start gap-3">
                    <div class="mt-0.5 ${f.direction === 'up' ? 'text-red-400' : (f.direction === 'down' ? 'text-emerald-400' : 'text-slate-400')}">
                        ${f.direction === 'up' ? '↑' : (f.direction === 'down' ? '↓' : '→')}
                    </div>
                    <div>
                        <div class="font-bold text-white text-sm">${f.factor} <span class="text-xs font-normal text-slate-400 ml-2">Impact: ${f.impact_cpl} cpl</span></div>
                        <div class="text-xs text-slate-400 mt-1">${f.explanation}</div>
                    </div>
                </div>
            `).join('');
        }

        // Populate Cycle Vis
        const marker = document.getElementById('mc-cycle-marker');
        const text = document.getElementById('mc-cycle-text');
        if (marker && text && data.cycle_position) {
            const pos = data.cycle_position.visual_position_percent;
            marker.style.left = `${pos}%`;
            text.innerHTML = `<span class="text-white font-bold">${data.cycle_position.phase}</span> - est. ${data.cycle_position.estimated_days_remaining} days remaining`;
        }

        // Populate Breakdown Bar
        const bar = document.getElementById('mc-breakdown-bar');
        const legend = document.getElementById('mc-breakdown-legend');
        if (bar && legend && data.price_breakdown) {
            const bd = data.price_breakdown;
            const total = bd.total_estimated;
            
            const segments = [
                { id: 'crude', val: bd.crude_oil_component, color: 'bg-slate-600', label: 'Crude' },
                { id: 'refining', val: bd.refining_margin, color: 'bg-indigo-500', label: 'Refining' },
                { id: 'shipping', val: bd.shipping, color: 'bg-blue-500', label: 'Shipping' },
                { id: 'excise', val: bd.excise, color: 'bg-amber-500', label: 'Excise' },
                { id: 'gst', val: bd.gst, color: 'bg-orange-500', label: 'GST' },
                { id: 'retail', val: bd.retail_margin, color: bd.retail_margin < 0 ? 'bg-red-500' : 'bg-emerald-500', label: 'Retail Margin' }
            ];
            
            let barHtml = '';
            let legHtml = '';
            
            segments.forEach(seg => {
                const pct = Math.max(0, (seg.val / total) * 100);
                if (pct > 0) {
                    barHtml += `<div class="h-full ${seg.color}" style="width: ${pct}%" title="${seg.label}: ${seg.val.toFixed(1)}c"></div>`;
                }
                legHtml += `<div class="flex items-center gap-1.5"><div class="w-3 h-3 rounded-sm ${seg.color}"></div><span class="text-slate-400">${seg.label}</span> <span class="text-white font-medium ml-auto">${seg.val.toFixed(1)}c</span></div>`;
            });
            
            bar.innerHTML = barHtml;
            legend.innerHTML = legHtml;
        }
    };

    const loadAnalytics = async () => {
        const data = await fetchApi(`/analytics?state=${state.currentStateCode}`);
        if (!data || data.error) return;
        state.analytics = data;

        // Render Main Chart
        if (data.trend) {
            renderMainChart(data.trend.history, data.trend.sarimax);
        }

        // Suburbs
        const tbody = document.getElementById('analytics-suburbs-body');
        if (tbody && data.suburb_ranking) {
            tbody.innerHTML = data.suburb_ranking.map((s, i) => `
                <tr class="border-b border-slate-800/50 hover:bg-slate-800/20 transition-colors">
                    <td class="py-3 pl-2 text-slate-500 font-bold">${i+1}</td>
                    <td class="py-3 text-white font-medium">${s.suburb}</td>
                    <td class="py-3 text-right pr-2 text-emerald-400 font-bold">${formatPrice(s.price)}c</td>
                </tr>
            `).join('');
        }

        // Also populate ratings while we have station data
        populateRatings();
    };

    const populateRatings = () => {
        if (!state.stations.length) return;
        
        const sorted = [...state.stations].sort((a, b) => a.price - b.price);
        const best = sorted.slice(0, 20);
        const worst = sorted.slice(-20).reverse();

        const renderRows = (arr) => arr.map(s => `
            <tr class="border-b border-slate-800/50 hover:bg-slate-800/20 transition-colors">
                <td class="py-2.5 font-medium text-white">${s.name}</td>
                <td class="py-2.5 text-slate-400 text-xs">${s.suburb}</td>
                <td class="py-2.5 text-right font-bold text-white">${formatPrice(s.price)}c</td>
            </tr>
        `).join('');

        const bestBody = document.getElementById('ratings-best-body');
        if (bestBody) bestBody.innerHTML = renderRows(best);
        
        const worstBody = document.getElementById('ratings-worst-body');
        if (worstBody) worstBody.innerHTML = renderRows(worst);
    };

    const loadSupplyData = async () => {
        const data = await fetchApi('/supply/summary');
        if (!data || data.error) return;
        state.supply = data;

        const sumEl = document.getElementById('supply-summary');
        if(sumEl) sumEl.textContent = data.overall_assessment;

        const impDep = document.getElementById('supply-import-dep');
        if(impDep) impDep.textContent = `${Math.round(data.import_dependency * 100)}%`;

        // Gauges
        const container = document.getElementById('supply-gauges-container');
        if (container && data.fuel_types) {
            container.innerHTML = Object.values(data.fuel_types).map(f => {
                const pct = Math.min(100, Math.max(0, (f.days_of_cover / 30) * 100)); // normalized to 30 days
                const color = f.status === 'CRITICAL' ? '#ef4444' : (f.status === 'WARNING' ? '#f59e0b' : '#10b981');
                const stroke = 283; // 2 * pi * 45
                const offset = stroke - (pct / 100) * stroke;
                
                return `
                <div class="glass-panel p-4 rounded-3xl flex flex-col items-center text-center">
                    <h4 class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-4">${f.display_name}</h4>
                    <div class="relative w-24 h-24 mb-3">
                        <svg class="w-full h-full circular-progress" viewBox="0 0 100 100">
                            <circle class="text-slate-700 stroke-current" stroke-width="8" cx="50" cy="50" r="45" fill="transparent"></circle>
                            <circle style="stroke: ${color}; stroke-dasharray: ${stroke}; stroke-dashoffset: ${offset}; transition: stroke-dashoffset 1s ease-in-out;" class="stroke-current" stroke-width="8" stroke-linecap="round" cx="50" cy="50" r="45" fill="transparent"></circle>
                        </svg>
                        <div class="absolute inset-0 flex flex-col items-center justify-center">
                            <span class="text-2xl font-black text-white leading-none">${Math.round(f.days_of_cover)}</span>
                            <span class="text-[10px] text-slate-400 font-bold">DAYS</span>
                        </div>
                    </div>
                    <div class="text-xs font-medium text-slate-300">${f.current_stock_ml.toLocaleString()} ML</div>
                </div>
                `;
            }).join('');
        }

        // Allocation
        const allocContainer = document.getElementById('supply-allocation');
        if (allocContainer && data.allocations) {
            allocContainer.innerHTML = Object.entries(data.allocations).map(([k, v]) => `
                <div class="flex justify-between items-center p-3 bg-slate-800/50 rounded-xl border border-slate-700/50">
                    <span class="text-sm font-medium text-slate-300">${v.label}</span>
                    <span class="text-sm font-bold text-white">${v.formatted_value}</span>
                </div>
            `).join('');
        }
    };

    const loadTankers = async () => {
        const data = await fetchApi('/supply/tankers');
        if (!data || data.error) return;
        state.tankers = data;

        const countBadge = document.getElementById('tanker-count-badge');
        if(countBadge) countBadge.textContent = `${data.tankers?.length || 0} Tracking`;

        initTankerMap();

        const listContainer = document.getElementById('tanker-list');
        if (listContainer && data.tankers) {
            // Draw on map and build list
            let listHtml = '';
            
            // Clear old markers if any (simplified)
            
            data.tankers.forEach(t => {
                if(state.tankerMap && t.position) {
                    L.circleMarker([t.position.lat, t.position.lng], {
                        radius: 4, fillColor: '#06b6d4', color: '#fff', weight: 1, fillOpacity: 1
                    }).addTo(state.tankerMap).bindPopup(`${t.name} -> ${t.destination_port}`);
                }
                
                listHtml += `
                    <div class="bg-slate-800/50 border border-slate-700/50 rounded-xl p-4 mb-3">
                        <div class="flex justify-between items-start mb-2">
                            <div>
                                <div class="font-bold text-white">${t.name}</div>
                                <div class="text-xs text-slate-400">${t.vessel_type} • ${t.flag_country}</div>
                            </div>
                            <div class="text-right">
                                <div class="text-sm font-bold text-cyan-400">${t.eta_hours} hrs</div>
                                <div class="text-[10px] text-slate-500 uppercase tracking-wider">ETA</div>
                            </div>
                        </div>
                        <div class="flex justify-between text-xs font-medium border-t border-slate-700/50 pt-2 mt-2">
                            <span class="text-slate-300">To: ${t.destination_port}</span>
                            <span class="text-slate-400">Cargo: ~${t.cargo_estimate_ml} ML</span>
                        </div>
                    </div>
                `;
            });
            listContainer.innerHTML = listHtml;
        }
    };

    const loadSentiment = async () => {
        const data = await fetchApi('/sentiment');
        if (!data) return;
        state.sentiment = data;

        // Needle Gauge
        const needle = document.getElementById('sentiment-needle');
        if (needle) {
            // -1 (Bullish/Up) to 1 (Bearish/Down) mapped to 0% to 100%
            // But wait, visually: Left is Bearish (Price Down / Green), Right is Bullish (Price Up / Red)
            // So if sentiment is positive (bearish keywords), it should be on the left.
            // Let's assume data.global.overall_sentiment is -1 to 1. 
            // 0 is middle (50%). 1 is 100%. -1 is 0%.
            const sent = data.global?.overall_sentiment || 0;
            // Map -1 to 1 into 100% to 0% (because -1 is price up/right, 1 is price down/left)
            const pos = 50 - (sent * 50); 
            needle.style.left = `${Math.max(0, Math.min(100, pos))}%`;
        }

        const sumText = document.getElementById('news-summary-text');
        if (sumText) sumText.textContent = data.global?.summary || "Analysis complete.";

        const renderFeed = (articles, containerId) => {
            const container = document.getElementById(containerId);
            if (!container) return;
            
            if (!articles || !articles.length) {
                container.innerHTML = '<div class="text-sm text-slate-500 italic">No recent articles found.</div>';
                return;
            }

            container.innerHTML = articles.map(a => {
                let badgeColor = 'bg-slate-700 text-slate-300';
                if(a.sentiment_tag.includes('Pressure')) badgeColor = 'bg-red-500/20 text-red-400 border border-red-500/30';
                else if(a.sentiment_tag.includes('Relief')) badgeColor = 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30';
                
                return `
                    <div class="bg-slate-800/50 rounded-2xl p-4 border border-slate-700/50 hover:border-slate-600 transition-colors">
                        <div class="flex flex-wrap gap-2 mb-2">
                            <span class="text-[10px] uppercase font-bold tracking-wider px-2 py-0.5 rounded-sm ${badgeColor}">${a.sentiment_tag.replace(/[🔴🟢⚪]/g, '')}</span>
                            <span class="text-[10px] uppercase font-bold tracking-wider px-2 py-0.5 rounded-sm bg-indigo-500/20 text-indigo-400 border border-indigo-500/30">${a.impact_vector}</span>
                        </div>
                        <a href="${a.link}" target="_blank" class="font-bold text-white text-sm hover:text-primary-400 transition-colors line-clamp-2 mb-2">${a.title}</a>
                        <p class="text-xs text-slate-400 leading-relaxed mb-3">${a.analysis}</p>
                        <div class="text-[10px] text-slate-500 font-medium flex justify-between">
                            <span>${a.publisher}</span>
                            <span>${new Date(a.published).toLocaleDateString()}</span>
                        </div>
                    </div>
                `;
            }).join('');
        };

        renderFeed(data.global?.articles, 'news-global-feed');
        renderFeed(data.domestic?.articles, 'news-domestic-feed');
    };

    // --- Advanced Mode ---
    const ADVANCED_TOKEN_KEY = 'fuelai_advanced_token';
    const ADVANCED_EXPIRY_KEY = 'fuelai_advanced_expires';

    const getAdvancedToken = () => {
        const token = sessionStorage.getItem(ADVANCED_TOKEN_KEY);
        const expiry = sessionStorage.getItem(ADVANCED_EXPIRY_KEY);
        if (!token || !expiry) return null;
        if (Date.parse(expiry) <= Date.now()) {
            clearAdvancedSession();
            return null;
        }
        return token;
    };

    const clearAdvancedSession = () => {
        sessionStorage.removeItem(ADVANCED_TOKEN_KEY);
        sessionStorage.removeItem(ADVANCED_EXPIRY_KEY);
        state.advancedUnlocked = false;
        state.advancedBriefing = null;
        setAdvancedUnlocked(false);
    };

    const setAdvancedUnlocked = (unlocked) => {
        state.advancedUnlocked = unlocked;
        const locked = document.getElementById('advanced-locked');
        const suite = document.getElementById('advanced-suite');
        if (locked) locked.classList.toggle('hidden', unlocked);
        if (suite) suite.classList.toggle('hidden', !unlocked);
    };

    const advancedFetch = async (endpoint, options = {}) => {
        const token = getAdvancedToken();
        if (!token) {
            clearAdvancedSession();
            return null;
        }

        const headers = {
            ...(options.headers || {}),
            'Authorization': `Bearer ${token}`
        };

        try {
            const res = await fetch(`${CONFIG.apiBase}${endpoint}`, { ...options, headers });
            if (res.status === 401) {
                clearAdvancedSession();
                showToast('Advanced session expired. Unlock again.', 'error');
                return null;
            }
            if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
            return await res.json();
        } catch (error) {
            console.error(`Advanced API Error on ${endpoint}:`, error);
            showToast('Advanced analysis failed', 'error');
            return null;
        }
    };

    const initAdvancedControls = () => {
        const unlockBtn = document.getElementById('advanced-unlock-btn');
        const passwordInput = document.getElementById('advanced-password');
        const refreshBtn = document.getElementById('advanced-refresh-btn');
        const lockBtn = document.getElementById('advanced-lock-btn');
        const askBtn = document.getElementById('advanced-ask-btn');
        const questionInput = document.getElementById('advanced-question');
        const shockBtn = document.getElementById('advanced-shock-btn');

        if (unlockBtn && !unlockBtn.dataset.bound) {
            unlockBtn.dataset.bound = 'true';
            unlockBtn.addEventListener('click', unlockAdvancedMode);
        }

        if (passwordInput && !passwordInput.dataset.bound) {
            passwordInput.dataset.bound = 'true';
            passwordInput.addEventListener('keydown', (event) => {
                if (event.key === 'Enter') unlockAdvancedMode();
            });
        }

        if (refreshBtn && !refreshBtn.dataset.bound) {
            refreshBtn.dataset.bound = 'true';
            refreshBtn.addEventListener('click', () => loadAdvancedBriefing(true));
        }

        if (lockBtn && !lockBtn.dataset.bound) {
            lockBtn.dataset.bound = 'true';
            lockBtn.addEventListener('click', clearAdvancedSession);
        }

        if (askBtn && !askBtn.dataset.bound) {
            askBtn.dataset.bound = 'true';
            askBtn.addEventListener('click', askAdvancedQuestion);
        }

        if (questionInput && !questionInput.dataset.bound) {
            questionInput.dataset.bound = 'true';
            questionInput.addEventListener('keydown', (event) => {
                if (event.key === 'Enter') askAdvancedQuestion();
            });
        }

        if (shockBtn && !shockBtn.dataset.bound) {
            shockBtn.dataset.bound = 'true';
            shockBtn.addEventListener('click', runAdvancedShock);
        }

        document.querySelectorAll('.advanced-prompt').forEach(btn => {
            if (btn.dataset.bound) return;
            btn.dataset.bound = 'true';
            btn.addEventListener('click', () => {
                const input = document.getElementById('advanced-question');
                if (input) {
                    input.value = btn.dataset.prompt || '';
                    input.focus();
                }
            });
        });
    };

    const initAdvancedMode = () => {
        initAdvancedControls();
        const token = getAdvancedToken();
        setAdvancedUnlocked(!!token);
        if (token && !state.advancedBriefing) {
            loadAdvancedBriefing();
        }
    };

    const unlockAdvancedMode = async () => {
        const input = document.getElementById('advanced-password');
        const status = document.getElementById('advanced-auth-status');
        const btn = document.getElementById('advanced-unlock-btn');
        const password = input?.value || '';

        if (!password) {
            if (status) status.textContent = 'Enter the Advanced password.';
            return;
        }

        if (btn) {
            btn.disabled = true;
            btn.textContent = 'Verifying...';
        }
        if (status) status.textContent = 'Checking credentials...';

        try {
            const res = await fetch(`${CONFIG.apiBase}/advanced/verify`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ password })
            });

            if (!res.ok) {
                if (status) status.textContent = 'Password rejected.';
                showToast('Invalid Advanced password', 'error');
                return;
            }

            const data = await res.json();
            sessionStorage.setItem(ADVANCED_TOKEN_KEY, data.token);
            sessionStorage.setItem(ADVANCED_EXPIRY_KEY, data.expires_at);
            if (input) input.value = '';
            if (status) status.textContent = '';
            setAdvancedUnlocked(true);
            loadAdvancedBriefing(true);
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.textContent = 'Unlock Advanced';
            }
        }
    };

    const renderAdvancedMetrics = (cards = []) => {
        const container = document.getElementById('advanced-metrics');
        if (!container) return;
        container.innerHTML = cards.map(card => `
            <div class="bg-slate-950/50 border border-slate-800 rounded-2xl p-4">
                <div class="text-[10px] uppercase tracking-wider font-bold text-slate-500 mb-1">${escapeHtml(card.label)}</div>
                <div class="text-lg font-black text-white">${escapeHtml(card.value)}</div>
                <div class="text-xs text-slate-400 mt-1">${escapeHtml(card.detail)}</div>
            </div>
        `).join('');
    };

    const renderAnalystNotes = (notes = []) => {
        const container = document.getElementById('advanced-notes');
        if (!container) return;
        const colorFor = (severity) => {
            if (severity === 'alert') return 'border-red-500/30 text-red-300 bg-red-500/10';
            if (severity === 'warning') return 'border-amber-500/30 text-amber-300 bg-amber-500/10';
            if (severity === 'ok') return 'border-emerald-500/30 text-emerald-300 bg-emerald-500/10';
            return 'border-cyan-500/30 text-cyan-300 bg-cyan-500/10';
        };
        container.innerHTML = notes.map(note => `
            <div class="rounded-2xl border p-4 ${colorFor(note.severity)}">
                <div class="font-bold text-white mb-1">${escapeHtml(note.title)}</div>
                <div class="text-sm text-slate-300">${escapeHtml(note.detail)}</div>
            </div>
        `).join('');
    };

    const renderEvidenceChips = (containerId, cards = []) => {
        const container = document.getElementById(containerId);
        if (!container) return;
        container.innerHTML = cards.slice(0, 5).map(card => `
            <span class="text-[11px] bg-slate-900 border border-slate-700 rounded-full px-2.5 py-1 text-slate-300">
                ${escapeHtml(card.label)}: <span class="text-white font-bold">${escapeHtml(card.value)}</span>
            </span>
        `).join('');
    };

    const loadAdvancedBriefing = async (force = false) => {
        if (!getAdvancedToken()) return;
        if (state.advancedBriefing && !force) {
            renderAdvancedBriefing(state.advancedBriefing);
            return;
        }

        const status = document.getElementById('advanced-briefing-status');
        const briefing = document.getElementById('advanced-briefing');
        if (status) status.textContent = 'Generating...';
        if (briefing) {
            briefing.innerHTML = `
                <div class="pulse-skeleton h-8 rounded-xl w-3/4"></div>
                <div class="pulse-skeleton h-24 rounded-2xl w-full"></div>
                <div class="pulse-skeleton h-16 rounded-2xl w-full"></div>
            `;
        }

        const data = await advancedFetch(`/advanced/briefing?state=${state.currentStateCode}`);
        if (!data) return;
        state.advancedBriefing = data;
        renderAdvancedBriefing(data);
    };

    const renderAdvancedBriefing = (data) => {
        const status = document.getElementById('advanced-briefing-status');
        const briefing = document.getElementById('advanced-briefing');
        if (status) {
            status.textContent = data.disabled ? 'Local fallback' : 'Generated';
            status.className = data.disabled
                ? 'text-xs bg-amber-500/10 text-amber-300 px-3 py-1 rounded-full border border-amber-500/30'
                : 'text-xs bg-emerald-500/10 text-emerald-300 px-3 py-1 rounded-full border border-emerald-500/30';
        }
        renderAdvancedMetrics(data.metrics || data.evidence || []);
        renderAnalystNotes(data.analyst_notes || []);
        renderEvidenceChips('advanced-ask-evidence', data.evidence || []);

        const summary = Array.isArray(data.summary) ? data.summary : [data.summary].filter(Boolean);
        if (briefing) {
            briefing.innerHTML = `
                ${data.message ? `<div class="text-sm rounded-2xl p-3 bg-amber-500/10 border border-amber-500/20 text-amber-200">${escapeHtml(data.message)}</div>` : ''}
                <div>
                    <h4 class="text-2xl font-black text-white mb-3">${escapeHtml(data.title || 'Morning Fuel Briefing')}</h4>
                    <div class="space-y-3 text-slate-300 leading-relaxed">
                        ${summary.map(paragraph => `<p>${escapeHtml(paragraph)}</p>`).join('')}
                    </div>
                </div>
                <div class="bg-emerald-500/10 border border-emerald-500/20 rounded-2xl p-4">
                    <div class="text-xs text-emerald-300 uppercase font-bold tracking-wider mb-1">Action</div>
                    <div class="text-white font-bold">${escapeHtml(data.action || 'Monitor the dashboard.')}</div>
                </div>
                <div>
                    <div class="text-xs text-slate-500 uppercase font-bold tracking-wider mb-2">Risks</div>
                    <div class="space-y-2">
                        ${(data.risks || []).map(risk => `<div class="text-sm bg-slate-900/60 border border-slate-800 rounded-xl p-3 text-slate-300">${escapeHtml(risk)}</div>`).join('') || '<div class="text-sm text-slate-500">No explicit risks returned.</div>'}
                    </div>
                </div>
            `;
        }
    };

    const appendAdvancedMessage = (role, text, isLoading = false) => {
        const log = document.getElementById('advanced-chat-log');
        if (!log) return null;
        const msg = document.createElement('div');
        msg.className = role === 'user'
            ? 'ml-auto max-w-[88%] bg-cyan-500 text-slate-950 rounded-2xl px-4 py-3 text-sm font-semibold'
            : 'mr-auto max-w-[92%] bg-slate-900 border border-slate-800 text-slate-200 rounded-2xl px-4 py-3 text-sm leading-relaxed';
        msg.innerHTML = isLoading ? '<div class="animate-pulse">Analyst is reading the evidence...</div>' : escapeHtml(text);
        log.appendChild(msg);
        log.scrollTop = log.scrollHeight;
        return msg;
    };

    const askAdvancedQuestion = async () => {
        const input = document.getElementById('advanced-question');
        const btn = document.getElementById('advanced-ask-btn');
        const question = input?.value.trim();
        if (!question || !getAdvancedToken()) return;

        appendAdvancedMessage('user', question);
        if (input) input.value = '';
        const loading = appendAdvancedMessage('assistant', '', true);
        if (btn) btn.disabled = true;

        const data = await advancedFetch('/advanced/ask', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ state: state.currentStateCode, question })
        });

        if (loading) loading.remove();
        if (btn) btn.disabled = false;
        if (!data) return;

        appendAdvancedMessage('assistant', data.answer || 'No answer returned.');
        renderEvidenceChips('advanced-ask-evidence', data.evidence || []);
        renderAnalystNotes(data.analyst_notes || []);
        if (data.disabled && data.message) showToast(data.message, 'error');
    };

    const runAdvancedShock = async () => {
        const input = document.getElementById('advanced-shock-scenario');
        const output = document.getElementById('advanced-shock-output');
        const btn = document.getElementById('advanced-shock-btn');
        const scenario = input?.value.trim();
        if (!scenario || !getAdvancedToken()) {
            showToast('Enter a shock scenario first', 'error');
            return;
        }

        if (btn) {
            btn.disabled = true;
            btn.textContent = 'Modeling...';
        }
        if (output) {
            output.innerHTML = '<div class="pulse-skeleton h-32 rounded-2xl w-full"></div>';
        }

        const data = await advancedFetch('/advanced/shock', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ state: state.currentStateCode, scenario })
        });

        if (btn) {
            btn.disabled = false;
            btn.textContent = 'Run Shock Model';
        }
        if (!data) return;

        renderAdvancedShock(data);
        renderAdvancedMetrics(data.evidence || []);
        renderAnalystNotes(data.analyst_notes || []);
        if (data.disabled && data.message) showToast(data.message, 'error');
    };

    const renderAdvancedShock = (data) => {
        const output = document.getElementById('advanced-shock-output');
        if (!output) return;
        const parsed = data.parsed_variables || {};
        const impact = data.forecast_impact || {};
        output.innerHTML = `
            ${data.message ? `<div class="text-sm rounded-2xl p-3 bg-amber-500/10 border border-amber-500/20 text-amber-200">${escapeHtml(data.message)}</div>` : ''}
            <div class="grid grid-cols-2 gap-3">
                <div class="bg-slate-950/50 border border-slate-800 rounded-2xl p-4">
                    <div class="text-[10px] uppercase text-slate-500 font-bold">TGP Impact</div>
                    <div class="text-2xl font-black ${impact.tgp_delta_cpl >= 0 ? 'text-red-300' : 'text-emerald-300'}">${impact.tgp_delta_cpl >= 0 ? '+' : ''}${escapeHtml(impact.tgp_delta_cpl)}c</div>
                </div>
                <div class="bg-slate-950/50 border border-slate-800 rounded-2xl p-4">
                    <div class="text-[10px] uppercase text-slate-500 font-bold">Retail Impact</div>
                    <div class="text-2xl font-black ${impact.retail_delta_cpl >= 0 ? 'text-red-300' : 'text-emerald-300'}">${impact.retail_delta_cpl >= 0 ? '+' : ''}${escapeHtml(impact.retail_delta_cpl)}c</div>
                </div>
                <div class="bg-slate-950/50 border border-slate-800 rounded-2xl p-4">
                    <div class="text-[10px] uppercase text-slate-500 font-bold">AUD/USD After</div>
                    <div class="text-xl font-black text-white">${escapeHtml(impact.fx_after)}</div>
                </div>
                <div class="bg-slate-950/50 border border-slate-800 rounded-2xl p-4">
                    <div class="text-[10px] uppercase text-slate-500 font-bold">Brent After</div>
                    <div class="text-xl font-black text-white">$${escapeHtml(impact.brent_after_usd)}</div>
                </div>
            </div>
            <div class="bg-slate-900/60 border border-slate-800 rounded-2xl p-4">
                <div class="text-xs text-slate-500 uppercase font-bold tracking-wider mb-2">Parsed Variables</div>
                <div class="grid grid-cols-2 gap-2 text-sm text-slate-300">
                    <div>AUD delta: <span class="text-white font-bold">${escapeHtml(parsed.aud_usd_delta)}</span></div>
                    <div>Brent delta: <span class="text-white font-bold">${escapeHtml(parsed.brent_usd_delta)}</span></div>
                    <div>Supply risk: <span class="text-white font-bold">${escapeHtml(parsed.supply_risk_level)}</span></div>
                    <div>Demand risk: <span class="text-white font-bold">${escapeHtml(parsed.demand_risk_level)}</span></div>
                </div>
            </div>
            <div class="text-sm leading-relaxed text-slate-300 bg-slate-950/40 border border-slate-800 rounded-2xl p-4">${escapeHtml(data.explanation)}</div>
        `;
    };

    // --- Route Planner ---
    const runPlanner = async () => {
        const start = document.getElementById('planner-start')?.value;
        const end = document.getElementById('planner-end')?.value;
        const resDiv = document.getElementById('planner-results');
        const btn = document.getElementById('planner-btn');
        
        if (!start || !end) {
            showToast('Please enter both start and destination', 'error');
            return;
        }

        if(btn) {
            btn.innerHTML = '<div class="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin"></div>';
            btn.disabled = true;
        }

        try {
            const res = await fetchApi('/planner', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ start, end })
            });

            if (res && res.error) {
                showToast(res.error, 'error');
                if(resDiv) resDiv.innerHTML = `<div class="p-4 text-center text-red-400 text-sm bg-red-500/10 rounded-xl">${res.error}</div>`;
            } else if (res && res.stations) {
                // Draw route on map
                if (state.plannerMap) {
                    if (state.plannerLayer) state.plannerMap.removeLayer(state.plannerLayer);
                    
                    const points = [
                        [res.start_coords.lat, res.start_coords.lng],
                        [res.end_coords.lat, res.end_coords.lng]
                    ];
                    
                    const group = L.featureGroup();
                    
                    // Route line
                    L.polyline(points, {color: '#3b82f6', weight: 4, opacity: 0.5, dashArray: '10, 10'}).addTo(group);
                    
                    // Start/End markers
                    L.circleMarker(points[0], {radius: 6, fillColor: '#10b981', color: '#fff', weight: 2, fillOpacity: 1}).addTo(group);
                    L.circleMarker(points[1], {radius: 6, fillColor: '#ef4444', color: '#fff', weight: 2, fillOpacity: 1}).addTo(group);
                    
                    // Render List & Map Markers
                    let html = '';
                    res.stations.forEach((s, i) => {
                        html += `
                            <div class="bg-slate-800/80 p-4 rounded-xl border border-slate-700/50 flex justify-between items-center hover:bg-slate-700 transition-colors cursor-pointer" onclick="app.panPlanner(${s.latitude}, ${s.longitude})">
                                <div>
                                    <div class="font-bold text-white text-sm">${s.name}</div>
                                    <div class="text-xs text-slate-400 mt-1">Detour: ${s.detour_distance_km.toFixed(1)}km</div>
                                </div>
                                <div class="text-right">
                                    <div class="font-black text-emerald-400 text-xl">${s.price_cpl.toFixed(1)}<span class="text-xs text-emerald-500/70">c</span></div>
                                    <div class="text-[10px] text-slate-500 uppercase">${s.brand}</div>
                                </div>
                            </div>
                        `;
                        
                        L.circleMarker([s.latitude, s.longitude], {
                            radius: 8, fillColor: '#f59e0b', color: '#0f172a', weight: 2, fillOpacity: 1
                        }).addTo(group).bindPopup(`${s.name}: ${s.price_cpl.toFixed(1)}c`);
                    });
                    
                    resDiv.innerHTML = html;
                    state.plannerLayer = group.addTo(state.plannerMap);
                    state.plannerMap.fitBounds(group.getBounds(), {padding: [50, 50]});
                }
            }
        } finally {
            if(btn) {
                btn.innerHTML = 'Find Optimal Stops';
                btn.disabled = false;
            }
        }
    };

    // --- Drive Mode ---
    const initDriveMode = async () => {
        if (!navigator.geolocation) {
            showToast("Geolocation not supported by browser", "error");
            return;
        }

        try {
            if ('wakeLock' in navigator) {
                state.wakeLock = await navigator.wakeLock.request('screen');
            }
        } catch (err) {
            console.log("Wake Lock error:", err);
        }

        const nameEl = document.querySelector('.big-name');
        const priceEl = document.querySelector('.big-price');
        const metaEl = document.querySelector('.big-meta');
        const speedEl = document.getElementById('drive-speed');
        const accEl = document.getElementById('drive-acc');
        const navBtn = document.getElementById('drive-nav-btn');
        const scanner = document.getElementById('drive-scanner');
        const statusText = document.getElementById('drive-status-text');

        let lastFetchTime = 0;

        state.driveWatchId = navigator.geolocation.watchPosition(async (pos) => {
            const { latitude, longitude, speed, accuracy } = pos.coords;
            
            // Update Speed
            if (speed !== null && speedEl) {
                speedEl.textContent = Math.round(speed * 3.6); // m/s to km/h
            }
            
            // Update Accuracy
            if (accEl) {
                accEl.textContent = `±${Math.round(accuracy)}m`;
                accEl.className = accEl.className.replace(/text-\w+-500 bg-\w+-500\/10 border-\w+-500\/20/g, '');
                if (accuracy < 20) accEl.classList.add('text-emerald-500', 'bg-emerald-500/10', 'border-emerald-500/20');
                else if (accuracy < 100) accEl.classList.add('text-amber-500', 'bg-amber-500/10', 'border-amber-500/20');
                else accEl.classList.add('text-red-500', 'bg-red-500/10', 'border-red-500/20');
            }

            // Rate limit API calls to 1 per 30 seconds
            const now = Date.now();
            if (now - lastFetchTime > 30000) {
                lastFetchTime = now;
                if(scanner) scanner.classList.remove('hidden');
                if(statusText) statusText.textContent = "Scanning nearby...";
                
                const results = await fetchApi('/find_cheapest_nearby', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ latitude, longitude })
                });

                if(scanner) scanner.classList.add('hidden');
                if(statusText) statusText.textContent = "Best Option Nearby";

                if (results && results.length > 0) {
                    const best = results[0];
                    state.driveTargetStation = best;
                    
                    if(nameEl) nameEl.textContent = best.name;
                    if(priceEl) priceEl.textContent = best.price.toFixed(1);
                    if(metaEl) metaEl.innerHTML = `${best.brand} &bull; ${best.distance.toFixed(1)} km away`;
                    
                    if(navBtn) {
                        navBtn.classList.remove('opacity-50', 'pointer-events-none');
                        const query = encodeURIComponent(`${best.name} ${best.suburb}`);
                        navBtn.href = `https://www.google.com/maps/search/?api=1&query=${query}`;
                    }
                }
            }
        }, 
        (err) => {
            console.error("GPS Error:", err);
            if(accEl) {
                accEl.textContent = "Lost Signal";
                accEl.className = "text-sm font-bold text-red-500 bg-red-500/10 px-3 py-1 rounded-full border border-red-500/20 inline-block";
            }
        }, 
        { enableHighAccuracy: true, maximumAge: 10000, timeout: 5000 });
    };

    const stopDriveMode = () => {
        if (state.driveWatchId) {
            navigator.geolocation.clearWatch(state.driveWatchId);
            state.driveWatchId = null;
        }
        if (state.wakeLock) {
            state.wakeLock.release().catch(console.error);
            state.wakeLock = null;
        }
    };

    // --- Master Initialization ---
    const loadAllData = () => {
        loadMarketStatus();
        loadStations();
        
        // If intel view is active, reload its data too
        if (state.activeView === 'intel') {
            loadMarketContext();
            loadAnalytics();
            if(state.activeIntelTab === 'supply') { loadSupplyData(); loadTankers(); }
            if(state.activeIntelTab === 'news') loadSentiment();
            if(state.activeIntelTab === 'advanced' && getAdvancedToken()) loadAdvancedBriefing(true);
        }
    };

    const initLiveView = () => {
        initMaps();
        loadAllData();
        setInterval(loadAllData, CONFIG.refreshInterval);
    };

    // --- Public Methods (used by HTML onclicks) ---
    return {
        initLiveView,
        initNavigation,
        runPlanner,
        panPlanner: (lat, lng) => {
            if (state.plannerMap) {
                state.plannerMap.setView([lat, lng], 14, {animate: true});
            }
        }
    };
})();

// Export for global access if needed
window.app = app;
