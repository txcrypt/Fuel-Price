"""
Supply Data Engine — Australian Petroleum Supply Intelligence
Provides realistic supply-side data based on Australian Petroleum Statistics
published by the Department of Industry, Science and Resources.

Key data points are calibrated against public APS reports:
  - National stocks ≈ 4,200 ML (total refined product)
  - Days of cover  ≈ 22 days
  - Import dependency ≈ 91%
  - Daily consumption ≈ 195 ML across all products
"""

import math
import random
import hashlib
from datetime import datetime, timedelta


class SupplyDataEngine:
    """
    Simulates and serves Australian petroleum supply metrics.
    All figures are rooted in realistic APS baselines with small
    daily perturbations seeded by the current date to ensure
    consistency across calls on the same day.
    """

    # --- Fuel type baselines (ML = megalitres) ---
    FUEL_TYPES = {
        "petrol": {
            "display_name": "Petrol (ULP/PULP)",
            "daily_consumption_ml": 72.0,
            "stock_baseline_ml": 1550.0,
            "import_share": 0.55,
            "domestic_refinery_share": 0.45,
        },
        "diesel": {
            "display_name": "Diesel",
            "daily_consumption_ml": 83.0,
            "stock_baseline_ml": 1750.0,
            "import_share": 0.70,
            "domestic_refinery_share": 0.30,
        },
        "jet_fuel": {
            "display_name": "Jet Fuel (Avtur)",
            "daily_consumption_ml": 30.0,
            "stock_baseline_ml": 650.0,
            "import_share": 0.85,
            "domestic_refinery_share": 0.15,
        },
        "lpg": {
            "display_name": "LPG / Autogas",
            "daily_consumption_ml": 10.0,
            "stock_baseline_ml": 250.0,
            "import_share": 0.40,
            "domestic_refinery_share": 0.60,
        },
    }

    # National-level constants
    NATIONAL_IMPORT_DEPENDENCY = 0.91
    TOTAL_STOCK_BASELINE_ML = 4200.0
    DAYS_COVER_BASELINE = 22.0

    # Top import sources
    TOP_IMPORT_SOURCES = [
        {"country": "Singapore", "share": 0.32, "route_days": 7},
        {"country": "South Korea", "share": 0.26, "route_days": 12},
        {"country": "Japan", "share": 0.18, "route_days": 10},
        {"country": "India", "share": 0.08, "route_days": 14},
        {"country": "Malaysia", "share": 0.06, "route_days": 8},
        {"country": "China", "share": 0.05, "route_days": 13},
        {"country": "Other", "share": 0.05, "route_days": 15},
    ]

    # Domestic refineries (post-2021 closures)
    DOMESTIC_REFINERIES = [
        {
            "name": "Lytton (Ampol)",
            "location": "Brisbane, QLD",
            "capacity_bpd": 109_000,
            "status": "Operational",
        },
        {
            "name": "Geelong (Viva Energy)",
            "location": "Geelong, VIC",
            "capacity_bpd": 120_000,
            "status": "Operational",
        },
    ]

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _daily_seed() -> random.Random:
        """Return a Random instance seeded by today's date for consistency."""
        seed_str = datetime.now().strftime("%Y-%m-%d")
        seed_int = int(hashlib.md5(seed_str.encode()).hexdigest()[:8], 16)
        return random.Random(seed_int)

    def _perturb(self, base: float, pct: float = 0.03) -> float:
        """Add a small daily-consistent perturbation."""
        rng = self._daily_seed()
        return round(base * (1 + rng.uniform(-pct, pct)), 1)

    # ------------------------------------------------------------------ #
    #  Public API
    # ------------------------------------------------------------------ #

    def get_national_stocks(self) -> dict:
        """
        National refined-product stockholding summary.

        Returns
        -------
        dict with total_stocks_ml, days_of_cover, by_product breakdown,
        import_dependency, trend, last_updated
        """
        rng = self._daily_seed()

        by_product: list[dict] = []
        total_ml = 0.0
        total_daily_consumption = 0.0

        for key, info in self.FUEL_TYPES.items():
            stock = self._perturb(info["stock_baseline_ml"])
            consumption = self._perturb(info["daily_consumption_ml"], 0.02)
            days_cover = round(stock / consumption, 1) if consumption > 0 else 0
            total_ml += stock
            total_daily_consumption += consumption

            by_product.append({
                "fuel_type": key,
                "display_name": info["display_name"],
                "stock_ml": stock,
                "daily_consumption_ml": consumption,
                "days_of_cover": days_cover,
                "import_share": info["import_share"],
                "domestic_refinery_share": info["domestic_refinery_share"],
            })

        overall_days_cover = (
            round(total_ml / total_daily_consumption, 1)
            if total_daily_consumption > 0
            else 0
        )

        # Trend: simulate a gentle multi-day pattern
        day_of_year = datetime.now().timetuple().tm_yday
        trend_val = math.sin(day_of_year / 30.0) * 2  # ±2 %
        if trend_val > 0.5:
            trend = "BUILDING"
        elif trend_val < -0.5:
            trend = "DRAWING"
        else:
            trend = "STABLE"

        return {
            "total_stocks_ml": round(total_ml, 1),
            "days_of_cover": overall_days_cover,
            "daily_consumption_ml": round(total_daily_consumption, 1),
            "by_product": by_product,
            "import_dependency": self.NATIONAL_IMPORT_DEPENDENCY,
            "trend": trend,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M AEST"),
        }

    def get_import_statistics(self) -> dict:
        """
        Monthly import volumes & sources plus refinery production.

        Returns
        -------
        dict with monthly_imports_ml, top_sources, refinery_production,
        self_sufficiency_ratio, last_updated
        """
        rng = self._daily_seed()

        # Total monthly imports ≈ daily consumption * 30 * import share
        total_daily = sum(v["daily_consumption_ml"] for v in self.FUEL_TYPES.values())
        monthly_base = total_daily * 30 * self.NATIONAL_IMPORT_DEPENDENCY
        monthly_imports = self._perturb(monthly_base, 0.05)

        # Source breakdown
        sources: list[dict] = []
        for src in self.TOP_IMPORT_SOURCES:
            vol = round(monthly_imports * src["share"], 1)
            sources.append({
                "country": src["country"],
                "volume_ml": vol,
                "share_pct": round(src["share"] * 100, 1),
                "avg_transit_days": src["route_days"],
            })

        # Domestic refinery production
        total_refinery_bpd = sum(
            r["capacity_bpd"] for r in self.DOMESTIC_REFINERIES
            if r["status"] == "Operational"
        )
        # Convert bpd → ML/month  (1 barrel ≈ 0.159 kL)
        refinery_monthly_ml = round(
            total_refinery_bpd * 0.159 * 30 * rng.uniform(0.85, 0.95), 1
        )

        self_sufficiency = round(
            1 - self.NATIONAL_IMPORT_DEPENDENCY, 2
        )

        return {
            "monthly_imports_ml": round(monthly_imports, 1),
            "top_sources": sources,
            "refinery_production": {
                "monthly_ml": refinery_monthly_ml,
                "refineries": self.DOMESTIC_REFINERIES,
                "total_capacity_bpd": total_refinery_bpd,
                "utilisation_pct": round(rng.uniform(85, 95), 1),
            },
            "self_sufficiency_ratio": self_sufficiency,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M AEST"),
        }

    def calculate_fuel_allocation(self) -> dict:
        """
        Translate national stocks into tangible equivalents.

        Returns
        -------
        dict with per-product allocations: tank_fills_50l, flights_equivalent,
        days_at_current_rate, litres_total
        """
        TANK_SIZE_LITRES = 50
        # Average domestic flight ≈ 5,000 litres jet fuel
        FLIGHT_FUEL_LITRES = 5_000

        stocks = self.get_national_stocks()
        allocations: list[dict] = []

        for product in stocks["by_product"]:
            litres = product["stock_ml"] * 1_000_000  # ML → litres
            tank_fills = int(litres / TANK_SIZE_LITRES)
            days = product["days_of_cover"]

            entry = {
                "fuel_type": product["fuel_type"],
                "display_name": product["display_name"],
                "stock_ml": product["stock_ml"],
                "litres_total": litres,
                "tank_fills_50l": tank_fills,
                "days_at_current_rate": days,
            }

            # Only compute flight equivalents for jet fuel
            if product["fuel_type"] == "jet_fuel":
                entry["flights_equivalent"] = int(litres / FLIGHT_FUEL_LITRES)
            else:
                entry["flights_equivalent"] = None

            allocations.append(entry)

        total_litres = sum(a["litres_total"] for a in allocations)
        total_tank_fills = sum(a["tank_fills_50l"] for a in allocations)
        total_flights = sum(
            a["flights_equivalent"]
            for a in allocations
            if a["flights_equivalent"] is not None
        )

        return {
            "allocations": allocations,
            "total_litres": total_litres,
            "total_tank_fills_50l": total_tank_fills,
            "total_flights_equivalent": total_flights,
            "days_of_cover": stocks["days_of_cover"],
            "last_updated": stocks["last_updated"],
        }

    def get_supply_summary(self) -> dict:
        """
        High-level supply health assessment.

        Returns
        -------
        dict with overall_health, fuel_security_rating (A-F),
        key_risks, recommendations, metrics snapshot
        """
        stocks = self.get_national_stocks()
        imports = self.get_import_statistics()

        days_cover = stocks["days_of_cover"]
        import_dep = stocks["import_dependency"]
        trend = stocks["trend"]

        # --- Security rating (A–F) ---
        if days_cover >= 28:
            rating = "A"
            health = "STRONG"
        elif days_cover >= 22:
            rating = "B"
            health = "ADEQUATE"
        elif days_cover >= 16:
            rating = "C"
            health = "MODERATE"
        elif days_cover >= 10:
            rating = "D"
            health = "CONCERNING"
        else:
            rating = "F"
            health = "CRITICAL"

        # --- Key risks ---
        risks: list[dict] = []

        if import_dep > 0.85:
            risks.append({
                "risk": "High Import Dependency",
                "severity": "HIGH",
                "detail": (
                    f"Australia imports {import_dep*100:.0f}% of refined fuel. "
                    "Any major disruption to Asian refinery output or shipping "
                    "lanes (e.g. Strait of Malacca) would rapidly impact supply."
                ),
            })

        if days_cover < 20:
            risks.append({
                "risk": "Low Days of Cover",
                "severity": "HIGH",
                "detail": (
                    f"National stocks cover only {days_cover:.0f} days. "
                    "IEA recommends 90 days; Australia holds significantly less."
                ),
            })
        elif days_cover < 28:
            risks.append({
                "risk": "Below IEA Benchmark",
                "severity": "MEDIUM",
                "detail": (
                    f"Days of cover ({days_cover:.0f}) remains below the IEA "
                    "90-day net-import benchmark, though the Fuel Security Act "
                    "2021 provides a minimum stockholding obligation."
                ),
            })

        if trend == "DRAWING":
            risks.append({
                "risk": "Stocks Drawing Down",
                "severity": "MEDIUM",
                "detail": "National stocks are trending lower. Continued draws "
                          "would reduce days of cover further.",
            })

        # Refinery concentration risk
        operational = [
            r for r in self.DOMESTIC_REFINERIES
            if r["status"] == "Operational"
        ]
        if len(operational) <= 2:
            risks.append({
                "risk": "Refinery Concentration",
                "severity": "MEDIUM",
                "detail": (
                    f"Only {len(operational)} refineries remain operational "
                    "domestically. An unplanned outage at either Lytton or "
                    "Geelong would materially reduce domestic production."
                ),
            })

        # --- Recommendations ---
        recommendations: list[str] = []
        if days_cover < 20:
            recommendations.append(
                "Monitor stockholding levels daily; consider contingency imports."
            )
        if trend == "DRAWING":
            recommendations.append(
                "Stock draw trend should be reviewed against seasonal demand patterns."
            )
        recommendations.append(
            "Maintain diversified import sourcing across Singapore, Korea, and Japan."
        )

        return {
            "overall_health": health,
            "fuel_security_rating": rating,
            "days_of_cover": days_cover,
            "import_dependency_pct": round(import_dep * 100, 1),
            "total_stocks_ml": stocks["total_stocks_ml"],
            "daily_consumption_ml": stocks["daily_consumption_ml"],
            "trend": trend,
            "key_risks": risks,
            "recommendations": recommendations,
            "refineries_operational": len(operational),
            "import_sources": len(self.TOP_IMPORT_SOURCES),
            "last_updated": stocks["last_updated"],
        }
