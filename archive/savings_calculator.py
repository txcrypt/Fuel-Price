class SavingsCalculator:
    """
    A sophisticated fuel savings calculator based on Edgeworth Price Cycle economics.
    
    The Edgeworth Cycle is an asymmetric pricing pattern common in retail petrol markets (like Brisbane):
    1. Restoration Phase: Prices jump up rapidly (often 20-40c/L within days) to restore margins.
    2. Relenting Phase: Prices slowly undercut competitors (1-2c/day) until they hit a 'floor'.
    
    This calculator quantifies the value of timing these phases.
    """

    def __init__(self, current_avg_price, best_local_price, cycle_phase, predicted_bottom, tank_size=50, fill_frequency=52):
        """
        Initialize the calculator with market and user data.

        Args:
            current_avg_price (float): Market average in cents per litre (cpl).
            best_local_price (float): Cheapest option in user's radius (cpl).
            cycle_phase (str): 'Restoration' (Hike) or 'Relenting' (Drop).
            predicted_bottom (float): Forecasted cycle floor (cpl).
            tank_size (int): User's tank capacity in Litres.
            fill_frequency (int): Number of fills per year.
        """
        self.avg_price = float(current_avg_price)
        self.best_price = float(best_local_price)
        self.phase = cycle_phase
        self.pred_bottom = float(predicted_bottom)
        self.tank = int(tank_size)
        self.annual_fills = int(fill_frequency)
        
        # Heuristic: Edgeworth cycle spikes usually peak ~45c above the bottom
        self.pred_peak = self.pred_bottom + 45.0

    def calculate_instant_savings(self):
        """
        Calculates savings by choosing the best local station over the market average.
        Returns: float (Dollars)
        """
        # If best price is somehow higher than average, saving is 0 (or negative)
        diff_cpl = self.avg_price - self.best_price
        total_dollars = (diff_cpl * self.tank) / 100.0
        return max(0.0, total_dollars)

    def calculate_opportunity(self):
        """
        Calculates 'Cycle Opportunity':
        - In 'Relenting': The money saved by WAITING for the bottom.
        - In 'Restoration': The cost AVOIDED by filling now before the peak.
        
        Returns: float (Dollars)
        """
        opportunity_cpl = 0.0

        if self.phase == "Relenting":
            # Strategy: WAIT.
            # Opportunity is the difference between buying now vs buying at the bottom.
            if self.best_price > self.pred_bottom:
                opportunity_cpl = self.best_price - self.pred_bottom
            else:
                # We are already at or below the predicted bottom
                opportunity_cpl = 0.0

        elif self.phase == "Restoration":
            # Strategy: BUY NOW.
            # Opportunity is the avoided cost of paying the peak price.
            # We assume if they don't buy now, they'll be forced to buy at the peak/high average.
            if self.best_price < self.pred_peak:
                opportunity_cpl = self.pred_peak - self.best_price
            else:
                opportunity_cpl = 0.0

        return (opportunity_cpl * self.tank) / 100.0

    def calculate_annualized(self):
        """
        ACCC benchmark: Savvy drivers save $240-$740/year (Avg ~$490) based on 50L/week.
        We scale this baseline by the user's actual usage volume.
        """
        standard_volume = 50 * 52 # 2600 L/year
        user_volume = self.tank * self.annual_fills
        
        scale_factor = user_volume / standard_volume if standard_volume > 0 else 0
        
        # ACCC Baseline Average ~ $490
        projected = 490.0 * scale_factor
        return round(projected, 2)

    def generate_recommendation(self, opp_cost):
        """Generates semantic advice based on the metrics."""
        
        save_str = f"${opp_cost:.2f}"
        
        if self.phase == "Restoration":
            return f"🚨 PRICE HIKE ALERT: Fill {self.tank}L NOW. Waiting could cost you an extra {save_str}."
            
        elif self.phase == "Relenting":
            if opp_cost > 5.0:
                # Significant saving potential
                return f"📉 Prices Dropping: Fill only 10L today. Wait for the cycle bottom to save ~{save_str} on a full tank."
            elif opp_cost > 1.0:
                return f"📉 Prices Dropping slowly. You can wait, but current savings are minimal ({save_str})."
            else:
                return "✅ BOTTOM OF CYCLE: Excellent time to fill up. Market is at its cheapest."
        
        return "Market is stable. Fill as needed."

    def get_report(self):
        """Returns the full JSON-serializable dictionary."""
        instant = self.calculate_instant_savings()
        opportunity = self.calculate_opportunity()
        annual = self.calculate_annualized()
        rec = self.generate_recommendation(opportunity)
        
        return {
            "immediate_saving_dollars": round(instant, 2),
            "opportunity_cost_dollars": round(opportunity, 2),
            "projected_annual_saving": annual,
            "recommendation_text": rec,
            "meta": {
                "phase": self.phase,
                "tank_size": self.tank,
                "benchmark_price": self.avg_price
            }
        }

if __name__ == "__main__":
    # Test Case 1: Relenting (Prices dropping)
    calc = SavingsCalculator(
        current_avg_price=185.5,
        best_local_price=175.0,
        cycle_phase="Relenting",
        predicted_bottom=160.0,
        tank_size=60
    )
    print("Relenting Scenario:", calc.get_report())

    # Test Case 2: Restoration (Prices spiking)
    calc2 = SavingsCalculator(
        current_avg_price=175.0,
        best_local_price=168.0,
        cycle_phase="Restoration",
        predicted_bottom=160.0,
        tank_size=50
    )
    print("Restoration Scenario:", calc2.get_report())
