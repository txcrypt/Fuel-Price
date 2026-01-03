import numpy as np
import pandas as pd
from datetime import timedelta

class NeuralForecaster:
    """
    A 'Mock' Deep Learning Forecaster.
    
    In a full production environment, this would load a TabM/PyTorch model.
    Here, it uses a 'Physical Simulation' (Math-based logic) that mimics 
    the learned shape of a Deep Learning model (The 'Shark Fin' Cycle).
    """
    
    def __init__(self, tgp: float, current_price: float, days_since_hike: int, status: str):
        self.tgp = tgp
        self.current_price = current_price
        self.days_since_hike = days_since_hike
        self.status = status
        
        # Cycle Physics Constants (Learned parameters)
        self.HIKE_PEAK_OFFSET = 24.0 # Cents above TGP
        self.BOTTOM_OFFSET = 2.0     # Cents above TGP
        self.CYCLE_LENGTH = 35       # Average days
        self.HIKE_DURATION = 5       # Days to reach peak
        self.DECAY_RATE = 0.85       # Smoothing factor

    def predict_next_14_days(self, start_date=None):
        """
        Generates a 14-day price trace.
        """
        forecast_prices = []
        forecast_dates = []
        
        # Initialize state
        sim_price = self.current_price
        sim_days = self.days_since_hike
        
        base_date = start_date if start_date else pd.Timestamp.now()
        
        for i in range(1, 15):
            date = base_date + timedelta(days=i)
            sim_days += 1
            
            # --- The 'Model' Logic ---
            
            # 1. Trigger Hike?
            # If we are effectively at the bottom and cycle is 'old', probability of hike rises.
            # A DL model would output a probability logit here.
            hike_prob = 0.0
            if sim_days > 25 and sim_price < (self.tgp + 4.0):
                hike_prob = (sim_days - 25) / 10.0 # Linear increase in risk
            
            # Force hike if status says so
            if self.status in ["HIKE_STARTED", "HIKE_IMMINENT"] and i < 3:
                hike_prob = 1.0

            # 2. Execute Dynamics
            if hike_prob > 0.8:
                # HIKE PHASE: Fast linear rise
                target = self.tgp + self.HIKE_PEAK_OFFSET
                # Move 50% of the way to target per day (Fast)
                sim_price += (target - sim_price) * 0.5
                
                # If we hit peak, reset days (Cycle restart)
                if sim_price > (target - 2.0):
                    sim_days = 0 
                    
            elif sim_days < self.HIKE_DURATION:
                # PEAK STABILIZATION
                target = self.tgp + self.HIKE_PEAK_OFFSET
                sim_price += (target - sim_price) * 0.2
                
            else:
                # DECAY PHASE: Non-linear decay (Edgeworth cycle)
                # Price drops faster when high, slower when low.
                
                excess = max(0, sim_price - (self.tgp + self.BOTTOM_OFFSET))
                
                # Decay function: Drop ~1.5c - 2.0c per day usually
                drop = 1.5 + (excess * 0.02)
                
                sim_price -= drop
                
                # Hard Floor
                sim_price = max(sim_price, self.tgp + 0.5)

            forecast_prices.append(round(sim_price, 1))
            forecast_dates.append(date.strftime('%Y-%m-%d'))
            
        return {"dates": forecast_dates, "prices": forecast_prices}
