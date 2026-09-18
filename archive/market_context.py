"""
Market Context Engine — Australian Fuel Price Intelligence
Explains WHY fuel is priced as it is by decomposing the price into
its constituent components and analysing the driving factors.
"""

import math
from datetime import datetime


class MarketContextEngine:
    """
    Generates rich market context explaining current fuel prices
    by decomposing wholesale & retail components and identifying
    the dominant price drivers.
    """

    # --- Tax & Logistics Constants (AUD cents per litre) ---
    EXCISE_CPL = 51.1          # Federal fuel excise (indexed bi-annually)
    GST_RATE = 0.10            # Goods & Services Tax (10%)
    SHIPPING_CPL = 3.5         # Avg shipping cost Singapore→Aus
    QUALITY_PREMIUM_CPL = 2.0  # ULP95 quality premium over MOPS92

    # --- Baseline benchmarks for driver analysis ---
    _AUD_BASELINE = 0.67       # Long-run AUD/USD average
    _OIL_BASELINE = 75.0       # USD/bbl Brent baseline
    _BARRELS_PER_LITRE = 158.987  # litres per barrel

    # ------------------------------------------------------------------ #
    #  Public API
    # ------------------------------------------------------------------ #

    def generate_context(
        self,
        state: str,
        current_avg: float,
        tgp: float,
        brent_usd: float,
        aud_usd: float,
        cycle_info: dict | None = None,
        news_items: list | None = None,
    ) -> dict:
        """
        Build the full market-context payload.

        Parameters
        ----------
        state        : Australian state abbreviation (e.g. "QLD")
        current_avg  : Current average retail ULP price in cpl
        tgp          : Terminal Gate Price in cpl
        brent_usd    : Brent crude in USD per barrel
        aud_usd      : AUD/USD exchange rate
        cycle_info   : Optional dict from CycleDetector
        news_items   : Optional list of news article dicts

        Returns
        -------
        dict with keys: price_breakdown, cycle_position, driving_factors,
                        narrative, market_health
        """
        breakdown = self._build_price_breakdown(
            current_avg, tgp, brent_usd, aud_usd
        )
        cycle_position = self._interpret_cycle(cycle_info)
        factors = self._analyse_driving_factors(
            brent_usd, aud_usd, breakdown, cycle_info, news_items
        )
        narrative = self._compose_narrative(
            state, current_avg, tgp, breakdown, factors, cycle_position
        )
        health = self._assess_market_health(breakdown, factors)

        return {
            "price_breakdown": breakdown,
            "cycle_position": cycle_position,
            "driving_factors": factors,
            "narrative": narrative,
            "market_health": health,
        }

    # ------------------------------------------------------------------ #
    #  Price decomposition
    # ------------------------------------------------------------------ #

    def _build_price_breakdown(
        self,
        current_avg: float,
        tgp: float,
        brent_usd: float,
        aud_usd: float,
    ) -> dict:
        """Decompose the retail price into its components."""
        # Guard against division by zero
        safe_aud = aud_usd if aud_usd > 0 else 0.65

        # Crude-oil component in AUD cpl
        crude_oil_component = round(
            brent_usd / self._BARRELS_PER_LITRE / safe_aud * 100, 2
        )

        # Refining margin (crack spread proxy ≈ TGP minus crude, shipping, quality)
        refining_margin = round(
            max(tgp - crude_oil_component - self.SHIPPING_CPL - self.QUALITY_PREMIUM_CPL, 0),
            2,
        )

        # Subtotal before GST
        pre_gst = tgp + self.EXCISE_CPL
        gst = round(pre_gst * self.GST_RATE, 2)

        # Retail margin
        retail_margin = round(current_avg - tgp - self.EXCISE_CPL - gst, 2)

        total_estimated = round(
            crude_oil_component
            + refining_margin
            + self.SHIPPING_CPL
            + self.QUALITY_PREMIUM_CPL
            + self.EXCISE_CPL
            + gst
            + retail_margin,
            2,
        )

        return {
            "crude_oil_component": crude_oil_component,
            "refining_margin": refining_margin,
            "shipping": self.SHIPPING_CPL,
            "quality_premium": self.QUALITY_PREMIUM_CPL,
            "tgp": round(tgp, 2),
            "excise": self.EXCISE_CPL,
            "gst": gst,
            "retail_margin": retail_margin,
            "total_estimated": total_estimated,
            "actual_retail": round(current_avg, 2),
        }

    # ------------------------------------------------------------------ #
    #  Cycle interpretation
    # ------------------------------------------------------------------ #

    @staticmethod
    def _interpret_cycle(cycle_info: dict | None) -> dict:
        """Normalise cycle detector output into a consistent shape."""
        if cycle_info is None:
            return {
                "phase": "UNKNOWN",
                "raw_phase": "UNKNOWN",
                "days_in_phase": 0,
                "estimated_days_remaining": 0,
                "confidence": 0.0,
                "visual_position_percent": 0.0,
                "cycle_progress_percent": 0.0,
                "description": "Cycle data not available.",
            }

        raw_phase = cycle_info.get("phase", "UNKNOWN")
        phase = cycle_info.get("market_phase") or {
            "RESTORATION": "RISING",
            "UNDERCUTTING": "FALLING",
        }.get(raw_phase, raw_phase)
        days_in = cycle_info.get("days_in_phase", 0)
        remaining = cycle_info.get("estimated_days_remaining", 0)
        confidence = cycle_info.get("confidence", 0.0)
        visual_position = cycle_info.get("visual_position_percent", 0.0)
        cycle_progress = cycle_info.get("cycle_progress_percent", 0.0)

        descriptions = {
            "RISING": (
                f"Prices have been climbing for {days_in} day(s). "
                f"Approximately {remaining} day(s) of increases expected before the peak."
            ),
            "PEAK": (
                f"Prices are near the cycle peak (day {days_in}). "
                "A drop is imminent — avoid filling up now if possible."
            ),
            "FALLING": (
                f"Prices are declining — day {days_in} of the downswing. "
                f"Roughly {remaining} day(s) until the trough; consider waiting."
            ),
            "TROUGH": (
                f"Prices are at or near the cycle bottom (day {days_in}). "
                "This is the best time to fill up."
            ),
        }

        return {
            "phase": phase,
            "raw_phase": raw_phase,
            "days_in_phase": days_in,
            "estimated_days_remaining": remaining,
            "confidence": round(confidence, 2),
            "visual_position_percent": round(float(visual_position or 0.0), 1),
            "cycle_progress_percent": round(float(cycle_progress or 0.0), 1),
            "recent_trough_cpl": cycle_info.get("recent_trough_cpl"),
            "recent_peak_cpl": cycle_info.get("recent_peak_cpl"),
            "cycle_amplitude_cpl": cycle_info.get("cycle_amplitude_cpl"),
            "description": descriptions.get(
                phase, "Cycle phase cannot be determined."
            ),
        }

    # ------------------------------------------------------------------ #
    #  Driving-factor analysis
    # ------------------------------------------------------------------ #

    def _analyse_driving_factors(
        self,
        brent_usd: float,
        aud_usd: float,
        breakdown: dict,
        cycle_info: dict | None,
        news_items: list | None,
    ) -> list[dict]:
        """Return a ranked list of factors currently moving the price."""
        factors: list[dict] = []

        # 1. AUD/USD vs baseline
        fx_delta = aud_usd - self._AUD_BASELINE
        if abs(fx_delta) > 0.005:
            # A weaker AUD raises import costs
            impact_cpl = round(-fx_delta * 100, 1)  # rough ≈ 1c / 0.01 move
            direction = "UP" if fx_delta < 0 else "DOWN"
            factors.append({
                "factor": "AUD/USD Exchange Rate",
                "impact_cpl": abs(impact_cpl),
                "direction": direction,
                "explanation": (
                    f"AUD is {'below' if fx_delta < 0 else 'above'} the "
                    f"{self._AUD_BASELINE:.2f} baseline at {aud_usd:.4f}. "
                    f"{'Weaker dollar makes imports more expensive.' if fx_delta < 0 else 'Stronger dollar lowers import costs.'}"
                ),
            })

        # 2. Oil price vs baseline
        oil_delta = brent_usd - self._OIL_BASELINE
        if abs(oil_delta) > 1.0:
            safe_aud = aud_usd if aud_usd > 0 else 0.65
            impact_cpl = round(
                oil_delta / self._BARRELS_PER_LITRE / safe_aud * 100, 1
            )
            direction = "UP" if oil_delta > 0 else "DOWN"
            factors.append({
                "factor": "Brent Crude Oil Price",
                "impact_cpl": abs(impact_cpl),
                "direction": direction,
                "explanation": (
                    f"Brent at US${brent_usd:.2f}/bbl is "
                    f"{'above' if oil_delta > 0 else 'below'} the "
                    f"US${self._OIL_BASELINE:.0f} baseline by "
                    f"US${abs(oil_delta):.2f}."
                ),
            })

        # 3. Cycle position
        if cycle_info:
            phase = cycle_info.get("market_phase") or cycle_info.get("phase", "UNKNOWN")
            cycle_impact = {
                "RISING": ("UP", 3.0, "Price cycle is in the upswing."),
                "PEAK": ("UP", 5.0, "Cycle is near the peak — prices are at their highest."),
                "FALLING": ("DOWN", 3.0, "Cycle is falling — prices headed to trough."),
                "TROUGH": ("DOWN", 5.0, "Prices at cycle trough — best time to buy."),
                "RESTORATION": ("UP", 3.0, "Price restoration is underway."),
                "UNDERCUTTING": ("DOWN", 3.0, "Retailers are undercutting toward the cycle floor."),
            }
            if phase in cycle_impact:
                d, imp, expl = cycle_impact[phase]
                factors.append({
                    "factor": "Fuel Price Cycle",
                    "impact_cpl": imp,
                    "direction": d,
                    "explanation": expl,
                })

        # 4. Retail margin squeeze / expansion
        margin = breakdown.get("retail_margin", 0)
        if margin < 5.0:
            factors.append({
                "factor": "Retail Margin Squeeze",
                "impact_cpl": round(abs(margin), 1),
                "direction": "DOWN",
                "explanation": (
                    f"Retailers are operating on a thin {margin:.1f} cpl margin, "
                    "suggesting competitive pressure is keeping prices low."
                ),
            })
        elif margin > 18.0:
            factors.append({
                "factor": "Retail Margin Expansion",
                "impact_cpl": round(margin - 12.0, 1),
                "direction": "UP",
                "explanation": (
                    f"Retail margin at {margin:.1f} cpl is elevated. "
                    "Retailers are capturing extra profit above wholesale costs."
                ),
            })

        # 5. News-driven factors (if available)
        if news_items:
            bullish_count = sum(
                1 for n in news_items
                if isinstance(n, dict) and n.get("sentiment", 0) < -0.3
            )
            bearish_count = sum(
                1 for n in news_items
                if isinstance(n, dict) and n.get("sentiment", 0) > 0.3
            )
            if bullish_count >= 3:
                factors.append({
                    "factor": "News Sentiment — Bullish",
                    "impact_cpl": round(bullish_count * 0.8, 1),
                    "direction": "UP",
                    "explanation": (
                        f"{bullish_count} recent articles indicate upward "
                        "price pressure from geopolitical or supply concerns."
                    ),
                })
            elif bearish_count >= 3:
                factors.append({
                    "factor": "News Sentiment — Bearish",
                    "impact_cpl": round(bearish_count * 0.8, 1),
                    "direction": "DOWN",
                    "explanation": (
                        f"{bearish_count} recent articles point to easing "
                        "supply conditions or demand weakness."
                    ),
                })

        # Sort by absolute impact descending
        factors.sort(key=lambda f: f["impact_cpl"], reverse=True)
        return factors

    # ------------------------------------------------------------------ #
    #  Narrative builder
    # ------------------------------------------------------------------ #

    @staticmethod
    def _compose_narrative(
        state: str,
        current_avg: float,
        tgp: float,
        breakdown: dict,
        factors: list[dict],
        cycle_position: dict,
    ) -> str:
        """Generate a 2-3 sentence analyst-style summary."""
        margin = breakdown.get("retail_margin", 0)
        oil_cpl = breakdown.get("crude_oil_component", 0)

        # Opening sentence — current situation
        opening = (
            f"Average ULP in {state} is {current_avg:.1f} cpl against a "
            f"terminal gate price of {tgp:.1f} cpl, implying a retail "
            f"margin of {margin:.1f} cpl."
        )

        # Middle sentence — dominant driver
        if factors:
            top = factors[0]
            middle = (
                f"The primary driver is {top['factor']}, pushing prices "
                f"{'higher' if top['direction'] == 'UP' else 'lower'} by "
                f"an estimated {top['impact_cpl']:.1f} cpl."
            )
        else:
            middle = "No single dominant driver has been identified."

        # Closing sentence — cycle / outlook
        phase = cycle_position.get("phase", "UNKNOWN")
        remaining = cycle_position.get("estimated_days_remaining", 0)
        if phase == "TROUGH":
            closing = "The cycle is near its trough — fill up now for the best price."
        elif phase == "PEAK":
            closing = "Prices are at or near the peak; a correction is expected soon."
        elif phase == "RISING":
            closing = (
                f"Prices are still rising with ~{remaining} day(s) until "
                "the expected peak."
            )
        elif phase == "FALLING":
            closing = (
                f"The downswing continues with ~{remaining} day(s) "
                "until the expected trough."
            )
        else:
            closing = "Cycle timing is uncertain; monitor daily movements."

        return f"{opening} {middle} {closing}"

    # ------------------------------------------------------------------ #
    #  Market health
    # ------------------------------------------------------------------ #

    @staticmethod
    def _assess_market_health(breakdown: dict, factors: list[dict]) -> dict:
        """
        Classify the market as HEALTHY, STRESSED, or VOLATILE.

        - HEALTHY:  Margin 5-18 cpl, no extreme drivers
        - STRESSED: Margin <5 or >25, or multiple strong drivers
        - VOLATILE: Large opposing forces present simultaneously
        """
        margin = breakdown.get("retail_margin", 0)
        total_impact = sum(f["impact_cpl"] for f in factors)

        up_impact = sum(
            f["impact_cpl"] for f in factors if f["direction"] == "UP"
        )
        down_impact = sum(
            f["impact_cpl"] for f in factors if f["direction"] == "DOWN"
        )

        # Check for volatility (strong opposing forces)
        if up_impact > 5 and down_impact > 5:
            return {
                "status": "VOLATILE",
                "detail": "Strong opposing upward and downward drivers are both present.",
                "up_impact_cpl": round(up_impact, 1),
                "down_impact_cpl": round(down_impact, 1),
            }

        # Check for stress
        if margin < 5.0 or margin > 25.0 or total_impact > 15.0:
            return {
                "status": "STRESSED",
                "detail": "Retail margin or combined driver impact is outside normal bounds.",
                "up_impact_cpl": round(up_impact, 1),
                "down_impact_cpl": round(down_impact, 1),
            }

        return {
            "status": "HEALTHY",
            "detail": "Margins and drivers are within normal operating bounds.",
            "up_impact_cpl": round(up_impact, 1),
            "down_impact_cpl": round(down_impact, 1),
        }
