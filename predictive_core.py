import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import os
from datetime import timedelta
import warnings

warnings.filterwarnings('ignore')

class DeepCycleModel:
    """
    Production Engine for Fuel Price Forecasting.
    
    Features:
    - Delta-based Prediction (XGBoost Regressor)
    - Regime Detection (XGBoost Classifier)
    - Recursive 7-Day Forecasting
    - Automatic Wholesale Proxy (TGP)
    """
    
    def __init__(self, model_dir='models/'):
        self.model_dir = model_dir
        os.makedirs(self.model_dir, exist_ok=True)
        
        # 1. The Watchdog (Classifier)
        # Removed early_stopping_rounds for production (uses all data)
        self.classifier = xgb.XGBClassifier(
            n_estimators=600,
            learning_rate=0.03,
            max_depth=7,
            objective='binary:logistic',
            scale_pos_weight=4,
            eval_metric='logloss',
            random_state=42
        )
        
        # 2. The Oracle (Regressor)
        # Removed early_stopping_rounds for production (uses all data)
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

    def _feature_engineering(self, df):
        """Internal method to transform raw data into model features."""
        df = df.copy()
        df = df.sort_values('date')
        
        # 1. Baseline Proxy (Wholesale estimation)
        # Using 14-day rolling minimum as TGP proxy
        df[self.tgp_proxy_col] = df['price_cpl'].rolling(window=14).min().shift(1)
        df[self.tgp_proxy_col] = df[self.tgp_proxy_col].fillna(method='bfill')
        
        # 2. Deep Memory (Lags)
        lags = [1, 2, 3, 7, 14, 21, 28, 35, 42]
        for lag in lags:
            df[f'lag_{lag}'] = df['price_cpl'].shift(lag)
            
        # 3. Technical Indicators
        df['velo_1d'] = df['price_cpl'] - df['lag_1']
        df['velo_7d'] = df['price_cpl'] - df['lag_7']
        df['accel_1d'] = df['velo_1d'] - (df['lag_1'] - df['lag_2'])
        df['volatility_7d'] = df['price_cpl'].rolling(7).std()
        df['gross_margin'] = df['price_cpl'] - df[self.tgp_proxy_col]
        
        # 4. Cleanup
        # We preserve columns needed for feature generation but exclude non-numeric/targets
        self.feature_cols = [c for c in df.columns if c not in 
                             ['date', 'price_next', 'target_delta', 'is_hike', 'reported_at', 
                              'site_id', 'region', 'latitude', 'longitude']]
        return df

    def train(self, csv_path, city_name='brisbane'):
        """Trains and saves the model for a specific city."""
        print(f"🚀 Training model for {city_name}...")
        
        # Load
        df = pd.read_csv(csv_path)
        
        # Column standardization
        df.columns = [c.lower().strip() for c in df.columns]
        
        # Robust Date Detection
        date_candidates = ['reported_at', 'scraped_at', 'date', 'timestamp']
        date_col = next((c for c in date_candidates if c in df.columns), None)
        if not date_col:
             # Fallback
             date_col = next((c for c in df.columns if 'date' in c or 'time' in c or 'reported' in c), None)
        
        if not date_col:
            raise ValueError(f"Could not find date column. Columns: {df.columns}")
            
        df.rename(columns={date_col: 'reported_at'}, inplace=True)
        df['reported_at'] = pd.to_datetime(df['reported_at'])
        
        # Aggregate to Daily (City Level)
        df['date'] = df['reported_at'].dt.date
        daily_df = df.groupby('date')['price_cpl'].mean().reset_index()
        daily_df['date'] = pd.to_datetime(daily_df['date'])
        
        # Features
        processed = self._feature_engineering(daily_df)
        
        # Targets
        processed['price_next'] = processed['price_cpl'].shift(-1)
        processed['target_delta'] = processed['price_next'] - processed['price_cpl']
        processed['is_hike'] = (processed['target_delta'] > 3.0).astype(int)
        
        # Drop NaN caused by shifting/lags
        processed = processed.dropna()
        
        X = processed[self.feature_cols]
        y_reg = processed['target_delta']
        y_class = processed['is_hike']
        
        print(f"   📊 Learning from {len(processed)} historical data points...")
        
        # Fit (No Early Stopping -> Uses 100% of data)
        self.classifier.fit(X, y_class, verbose=False)
        
        # Stack Probabilities
        hike_probs = self.classifier.predict_proba(X)[:, 1]
        X_s = X.copy()
        X_s['hike_prob'] = hike_probs
        self.regressor.fit(X_s, y_reg, verbose=False)
        
        # Save
        self.save(city_name)
        print(f"✅ Model saved to {self.model_dir}{city_name}.pkl")

    def predict_horizon(self, history_df, days=7):
        """
        Generates a recursive 7-day forecast.
        Returns a DataFrame of future dates and prices.
        """
        # Prepare initial state
        history_df = history_df.copy()
        
        # Ensure date format
        if 'reported_at' in history_df.columns:
            history_df['date'] = pd.to_datetime(history_df['reported_at']).dt.date
            history_df['date'] = pd.to_datetime(history_df['date'])
            # Group by date if it's raw station data
            if len(history_df) > history_df['date'].nunique():
                 history_df = history_df.groupby('date')['price_cpl'].mean().reset_index()
        else:
             history_df['date'] = pd.to_datetime(history_df['date'])

        future_preds = []
        
        current_date = history_df['date'].max()
        
        for _ in range(days):
            # 1. Feature Engineering on current history
            processed = self._feature_engineering(history_df)
            last_row = processed.iloc[[-1]].copy()
            
            if last_row.empty:
                break

            # 2. Predict
            X = last_row[self.feature_cols]
            hike_prob = self.classifier.predict_proba(X)[0, 1]
            
            X_s = X.copy()
            X_s['hike_prob'] = hike_prob
            pred_delta = self.regressor.predict(X_s)[0]
            
            # 3. Update State
            current_price = last_row['price_cpl'].values[0]
            new_price = current_price + pred_delta
            current_date += timedelta(days=1)
            
            # Record
            future_preds.append({
                'date': current_date.strftime('%Y-%m-%d'),
                'predicted_price': round(new_price, 2),
                'hike_probability': round(float(hike_prob), 2),
                'trend': 'ROCKET 🚀' if hike_prob > 0.5 else 'FEATHER 🪶'
            })
            
            # Append to history for next iteration (Recursive Step)
            new_row = pd.DataFrame({'date': [current_date], 'price_cpl': [new_price]})
            history_df = pd.concat([history_df, new_row], ignore_index=True)
            
        return pd.DataFrame(future_preds)

    def save(self, name):
        joblib.dump({
            'classifier': self.classifier,
            'regressor': self.regressor,
            'features': self.feature_cols
        }, f"{self.model_dir}/{name}.pkl")

    def load(self, name):
        path = f"{self.model_dir}/{name}.pkl"
        if os.path.exists(path):
            data = joblib.load(path)
            self.classifier = data['classifier']
            self.regressor = data['regressor']
            self.feature_cols = data['features']
            return True
        return False