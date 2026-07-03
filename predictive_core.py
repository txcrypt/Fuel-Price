import os
import joblib
import logging
import warnings
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import xgboost as xgb

from cycle_detector import CycleDetector

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)

class DeepCycleModel:
    """
    Hybrid Prediction Engine for Australian Fuel Price Cycles.
    
    Combines:
    1. XGBoost Dynamic Regression (classifier for hikes + regressor for deltas)
    2. Empirical Edgeworth Physical Model (self-calibrating from historical data)
    3. Cycle State Transitioning via CycleDetector
    
    Provides:
    - 14-day forecasts
    - Confidence intervals (low/high bounds)
    - Hike probabilities and cycle regime tags
    """
    
    def __init__(self, model_dir='models/'):
        self.model_dir = model_dir
        os.makedirs(self.model_dir, exist_ok=True)
        
        # Classifier: predicts if next-day price increase is > 3 cpl
        self.classifier = xgb.XGBClassifier(
            n_estimators=600,
            learning_rate=0.03,
            max_depth=7,
            objective='binary:logistic',
            scale_pos_weight=4,
            eval_metric='logloss',
            random_state=42
        )
        
        # Regressor: predicts tomorrow's price change (cents per litre)
        self.regressor = xgb.XGBRegressor(
            n_estimators=1200,
            learning_rate=0.015,
            max_depth=6,
            objective='reg:squarederror',
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42
        )
        
        self.feature_cols = []
        self.tgp_proxy_col = 'tgp_proxy_14d'
        self.loaded = False
        self.detector = CycleDetector()

    def _feature_engineering(self, df):
        """Build lag and rolling indicators for the regression models."""
        df = df.copy()
        df = df.sort_values('date').reset_index(drop=True)
        
        # 1. Baseline Proxy (Wholesale estimation)
        df[self.tgp_proxy_col] = df['price_cpl'].rolling(window=14, min_periods=1).min().shift(1)
        df[self.tgp_proxy_col] = df[self.tgp_proxy_col].fillna(method='bfill')
        
        # 2. Lags
        lags = [1, 2, 3, 7, 14, 21, 28, 35, 42]
        for lag in lags:
            df[f'lag_{lag}'] = df['price_cpl'].shift(lag)
            
        # 3. Technical/Velocity Indicators
        df['velo_1d'] = df['price_cpl'] - df['lag_1']
        df['velo_7d'] = df['price_cpl'] - df['lag_7']
        df['accel_1d'] = df['velo_1d'] - (df['lag_1'] - df['lag_2'])
        df['volatility_7d'] = df['price_cpl'].rolling(7, min_periods=1).std()
        df['gross_margin'] = df['price_cpl'] - df[self.tgp_proxy_col]
        
        # 4. Cycle Detector Features (Days since peak/trough + day of week)
        df['day_of_week'] = df['date'].dt.dayofweek
        
        # Peak and trough distance features
        pt = CycleDetector.find_peaks_and_troughs(df['price_cpl'])
        peaks = pt['peak_indices']
        troughs = pt['trough_indices']
        
        days_since_peak = []
        days_since_trough = []
        
        for idx in range(len(df)):
            past_peaks = peaks[peaks <= idx]
            past_troughs = troughs[troughs <= idx]
            
            days_since_peak.append(idx - past_peaks[-1] if len(past_peaks) > 0 else 30)
            days_since_trough.append(idx - past_troughs[-1] if len(past_troughs) > 0 else 30)
            
        df['days_since_peak'] = days_since_peak
        df['days_since_trough'] = days_since_trough
        
        # Add placeholder for regime_probability (populated recursively during inference)
        df['regime_probability'] = 0.5
        
        # Clean null values resulting from lags/std calculations
        df = df.ffill().bfill()
        
        # Exclude non-feature columns
        self.feature_cols = [
            c for c in df.columns if c not in [
                'date', 'price_next', 'target_delta', 'is_hike', 'reported_at', 
                'site_id', 'region', 'latitude', 'longitude', 'state', 'scraped_at'
            ]
        ]
        
        return df

    def train(self, csv_path, city_name='brisbane'):
        """Train classifier and regressor from clean historical daily prices."""
        logger.info("🚀 Training hybrid model for %s...", city_name)
        df = pd.read_csv(csv_path)
        
        # Column cleanup
        df.columns = [c.lower().strip() for c in df.columns]
        
        # Date column resolving
        date_candidates = ['reported_at', 'scraped_at', 'date', 'timestamp']
        date_col = next((c for c in date_candidates if c in df.columns), None)
        if not date_col:
            date_col = next((c for c in df.columns if 'date' in c or 'time' in c), 'date')
            
        df['date'] = pd.to_datetime(df[date_col])
        df = df.sort_values('date').reset_index(drop=True)
        
        # Group to daily median prices
        df_daily = df.groupby(df['date'].dt.date)['price_cpl'].median().reset_index()
        df_daily['date'] = pd.to_datetime(df_daily['date'])
        
        # Feature Engineering
        df_feats = self._feature_engineering(df_daily)
        
        # Set targets
        df_feats['price_next'] = df_feats['price_cpl'].shift(-1)
        df_feats['target_delta'] = df_feats['price_next'] - df_feats['price_cpl']
        df_feats['is_hike'] = (df_feats['target_delta'] > 3.0).astype(int)
        
        # Drop last row since it doesn't have target labels
        df_feats = df_feats.dropna(subset=['target_delta'])
        
        X = df_feats[self.feature_cols]
        y_class = df_feats['is_hike']
        y_reg = df_feats['target_delta']
        
        # Train
        self.classifier.fit(X, y_class)
        
        # Stack predicted hike probability into regressor features
        X_reg = X.copy()
        X_reg['hike_prob'] = self.classifier.predict_proba(X)[:, 1]
        self.regressor.fit(X_reg, y_reg)
        
        self.loaded = True
        self.save(city_name)
        logger.info("✅ Training complete. Model saved as %s.pkl", city_name)

    def save(self, name):
        """Serialize models and feature structure."""
        joblib.dump({
            'classifier': self.classifier,
            'regressor': self.regressor,
            'features': self.feature_cols
        }, os.path.join(self.model_dir, f"{name}.pkl"))

    def load(self, name):
        """De-serialize model package."""
        path = os.path.join(self.model_dir, f"{name}.pkl")
        if os.path.exists(path):
            try:
                data = joblib.load(path)
                self.classifier = data['classifier']
                self.regressor = data['regressor']
                self.feature_cols = data['features']
                self.loaded = True
                return True
            except Exception as e:
                logger.error("Error loading model %s: %s", name, e)
        self.loaded = False
        return False

    def predict_horizon_physical(self, history_df, days=14, tgp=None):
        """
        Deterministic, self-calibrating physical Edgeworth cycle model.
        Used as fallback when ML is unloaded, or to blend into hybrid forecasts.
        """
        history_df = history_df.copy().sort_values('date').reset_index(drop=True)
        
        # Calibrate physical parameters using historical data
        self.detector.fit(history_df['price_cpl'])
        
        daily_decay = self.detector.daily_decay
        spike_magnitude = self.detector.spike_magnitude
        floor_margin = self.detector.floor_margin
        
        # Current status
        cycle_info = self.detector.detect_current_regime(history_df['price_cpl'])
        current_phase = cycle_info['phase']
        
        # Fallback TGP if not provided
        if tgp is None:
            tgp = history_df['price_cpl'].min() - 5.0
            
        current_price = history_df['price_cpl'].iloc[-1]
        current_date = history_df['date'].iloc[-1]
        
        # Keep track of simulated cycle state
        is_hiking = (current_phase == "RESTORATION")
        hike_days_left = 2 if is_hiking else 0
        
        future_preds = []
        
        for step in range(1, days + 1):
            current_date += timedelta(days=1)
            margin = current_price - tgp
            
            # Sigmoid probability of a hike based on margin
            hike_prob = 1.0 / (1.0 + np.exp((margin - floor_margin - 2.0) / 2.0))
            
            if is_hiking:
                if hike_days_left > 0:
                    # Distribute spike: 65% day 1, 35% day 2
                    step_spike = spike_magnitude * (0.65 if hike_days_left == 2 else 0.35)
                    current_price += step_spike
                    hike_days_left -= 1
                if hike_days_left == 0:
                    is_hiking = False
            else:
                # Decaying phase
                if margin <= floor_margin or hike_prob > 0.65:
                    is_hiking = True
                    hike_days_left = 2
                    current_price += spike_magnitude * 0.65
                    hike_days_left -= 1
                else:
                    current_price -= daily_decay
                    if current_price - tgp < floor_margin:
                        current_price = tgp + floor_margin

            future_preds.append({
                'date': current_date.strftime('%Y-%m-%d'),
                'predicted_price': round(current_price, 2),
                'hike_probability': round(float(hike_prob), 3),
                'trend': 'ROCKET 🚀' if is_hiking or hike_prob > 0.5 else 'FEATHER 🪶',
                'regime': 'RESTORATION' if is_hiking else 'UNDERCUTTING'
            })
            
        return pd.DataFrame(future_preds)

    def predict_horizon(self, history_df, days=14, tgp=None):
        """
        Ensemble prediction blending recursive XGBoost (short range)
        and calibrated Physical model (long range) to prevent compounding errors.
        """
        # Standarize Date
        history_df = history_df.copy()
        if 'reported_at' in history_df.columns:
            history_df['date'] = pd.to_datetime(history_df['reported_at'])
        else:
            history_df['date'] = pd.to_datetime(history_df['date'])
            
        # Group and drop duplicates
        history_df = history_df.groupby(history_df['date'].dt.date)['price_cpl'].median().reset_index()
        history_df['date'] = pd.to_datetime(history_df['date'])
        history_df = history_df.sort_values('date').reset_index(drop=True)
        
        # 1. Run physical model to get baseline
        phys_df = self.predict_horizon_physical(history_df, days=days, tgp=tgp)
        
        # If ML model isn't loaded, return physical predictions directly with uncertainty bands
        if not self.loaded:
            phys_df['predicted_low'] = (phys_df['predicted_price'] - 1.5 * np.sqrt(np.arange(1, days + 1))).round(2)
            phys_df['predicted_high'] = (phys_df['predicted_price'] + 1.5 * np.sqrt(np.arange(1, days + 1))).round(2)
            
            # Clamp low bound to TGP
            if tgp:
                phys_df['predicted_low'] = phys_df['predicted_low'].clip(lower=tgp)
            return phys_df
            
        # 2. Run recursive ML model
        ml_preds = []
        ml_history = history_df.copy()
        current_date = ml_history['date'].iloc[-1]
        
        try:
            self.detector.fit(history_df['price_cpl'])
            
            for step in range(1, days + 1):
                # Prepare features
                feats_df = self._feature_engineering(ml_history)
                last_row = feats_df.iloc[[-1]].copy()
                
                # Dynamic cycle probabilities from detector
                cycle_info = self.detector.detect_current_regime(ml_history['price_cpl'])
                hike_prob = float(cycle_info['probabilities'][CycleDetector.RESTORATION])
                
                # Set dynamic features
                last_row['regime_probability'] = hike_prob
                
                # Predict
                X = last_row[self.feature_cols]
                hike_prob_xgb = self.classifier.predict_proba(X)[0, 1]
                
                # Blended hike probability (XGBoost + Cycle State Engine)
                hike_prob_final = 0.6 * hike_prob_xgb + 0.4 * hike_prob
                
                # Regress delta
                X_reg = X.copy()
                X_reg['hike_prob'] = hike_prob_final
                pred_delta = self.regressor.predict(X_reg)[0]
                
                # Update price state
                current_price = last_row['price_cpl'].values[0]
                new_price = current_price + pred_delta
                current_date += timedelta(days=1)
                
                ml_preds.append({
                    'date': current_date.strftime('%Y-%m-%d'),
                    'predicted_price': round(new_price, 2),
                    'hike_probability': round(hike_prob_final, 3),
                    'trend': 'ROCKET 🚀' if hike_prob_final > 0.5 else 'FEATHER 🪶',
                    'regime': 'RESTORATION' if hike_prob_final > 0.5 else 'UNDERCUTTING'
                })
                
                # Append to rolling history
                new_row = pd.DataFrame({'date': [current_date], 'price_cpl': [new_price]})
                ml_history = pd.concat([ml_history, new_row], ignore_index=True)
                
            ml_df = pd.DataFrame(ml_preds)
            
            # 3. Blending (Decaying ML weight to prevent long-term recursive errors)
            blended_preds = []
            for i in range(days):
                # Decaying ML weight: 0.70 at day 1, sliding down to 0.10 at day 14
                w_ml = max(0.10, 0.70 - (i * 0.05))
                w_phys = 1.0 - w_ml
                
                ml_row = ml_df.iloc[i]
                phys_row = phys_df.iloc[i]
                
                pred_price = (ml_row['predicted_price'] * w_ml) + (phys_row['predicted_price'] * w_phys)
                hike_prob = (ml_row['hike_probability'] * w_ml) + (phys_row['hike_probability'] * w_phys)
                
                # Calculate growing confidence interval
                uncertainty = 1.8 * np.sqrt(i + 1)
                low_bound = round(pred_price - uncertainty, 2)
                high_bound = round(pred_price + uncertainty, 2)
                
                if tgp:
                    low_bound = max(low_bound, tgp)
                
                blended_preds.append({
                    'date': ml_row['date'],
                    'predicted_price': round(pred_price, 2),
                    'predicted_low': low_bound,
                    'predicted_high': high_bound,
                    'hike_probability': round(hike_prob, 3),
                    'trend': 'ROCKET 🚀' if hike_prob > 0.5 else 'FEATHER 🪶',
                    'regime': 'RESTORATION' if hike_prob > 0.5 else 'UNDERCUTTING'
                })
                
            return pd.DataFrame(blended_preds)
            
        except Exception as e:
            logger.error("XGBoost prediction failed: %s. Returning physical model forecast.", e)
            # Add basic confidence bands to physical fallback
            phys_df['predicted_low'] = (phys_df['predicted_price'] - 1.5 * np.sqrt(np.arange(1, days + 1))).round(2)
            phys_df['predicted_high'] = (phys_df['predicted_price'] + 1.5 * np.sqrt(np.arange(1, days + 1))).round(2)
            if tgp:
                phys_df['predicted_low'] = phys_df['predicted_low'].clip(lower=tgp)
            return phys_df