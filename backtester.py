import pandas as pd
import numpy as np
from datetime import timedelta
import cycle_prediction

def run_backtest(df, lookahead_days=7, ground_truth_threshold=5.0, algo_threshold=None):
    """
    Backtests the cycle prediction algorithm on historical data.
    
    Args:
        df: DataFrame with 'date' and 'price_cpl'.
        lookahead_days: How many days to check ahead for a hike to verify a 'HIKE' prediction.
        ground_truth_threshold: Cents per litre increase required to count as a "Hike" (Ground Truth).
        algo_threshold: Cents per litre increase the ALGORITHM uses to detect cycles (Prediction Sensitivity).
    """
    # 1. Preprocess
    if 'date' not in df.columns:
        ts_col = 'reported_at' if 'reported_at' in df.columns else 'scraped_at'
        df['date'] = pd.to_datetime(df[ts_col]).dt.normalize()
        
    daily = df.groupby('date')['price_cpl'].median().reset_index().sort_values('date')
    daily = daily.set_index('date')
    
    results = []
    
    # Dynamic history requirement to support smaller datasets
    min_history = max(5, int(len(daily) * 0.2))
    
    if len(daily) < (min_history + lookahead_days + 1):
        return None, None
    
    dates = daily.index
    stop_idx = len(dates) - lookahead_days
    if min_history >= stop_idx:
        min_history = max(0, stop_idx - 5)
    
    for i in range(min_history, stop_idx):
        current_date = dates[i]
        current_price = daily.iloc[i]['price_cpl']
        
        # 1. Historical Window
        history_window = daily.iloc[:i+1].reset_index()
        
        # 2. Run Algorithm with Dynamic Threshold
        avg_len, avg_relent, last_hike = cycle_prediction.analyze_cycles(history_window, hike_threshold=algo_threshold)
        pred = cycle_prediction.predict_status(avg_relent, last_hike)
        predicted_phase = pred.get('status', 'UNKNOWN')
        
        # 3. Determine Signal
        signal_hike = 1 if predicted_phase in ['HIKE', 'OVERDUE'] else 0
        
        # 4. Ground Truth
        future_window = daily.iloc[i+1 : i+1+lookahead_days]
        max_future_price = future_window['price_cpl'].max()
        actual_hike = 1 if (max_future_price - current_price) > ground_truth_threshold else 0
        
        results.append({
            'date': current_date,
            'price': current_price,
            'signal_hike': signal_hike,
            'actual_hike': actual_hike
        })
        
    results_df = pd.DataFrame(results)
    if results_df.empty: return None, None
        
    # 5. Calculate Metrics
    tp = len(results_df[(results_df['signal_hike'] == 1) & (results_df['actual_hike'] == 1)])
    fp = len(results_df[(results_df['signal_hike'] == 1) & (results_df['actual_hike'] == 0)])
    tn = len(results_df[(results_df['signal_hike'] == 0) & (results_df['actual_hike'] == 0)])
    fn = len(results_df[(results_df['signal_hike'] == 0) & (results_df['actual_hike'] == 1)])
    
    accuracy = (tp + tn) / len(results_df)
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    metrics = {
        'total_days': len(results_df),
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1_score': f1,
        'confusion': {'tp': tp, 'fp': fp, 'tn': tn, 'fn': fn}
    }
    
    return metrics, results_df

def optimize_algorithm(df, ground_truth_threshold=5.0):
    """Grid search to find the best hike_threshold."""
    best_score = -1.0
    best_thresh = 8.0
    
    thresholds = np.arange(4.0, 12.5, 0.5)
    
    # Baseline
    current_config = cycle_prediction.load_config()
    current_thresh = current_config.get('hike_threshold', 8.0)
    current_score = 0.0
    
    for thresh in thresholds:
        metrics, _ = run_backtest(df, ground_truth_threshold=ground_truth_threshold, algo_threshold=thresh)
        
        if metrics:
            score = metrics['f1_score']
            if score == 0: score = metrics['accuracy']
            
            if abs(thresh - current_thresh) < 0.1:
                current_score = score
            
            if score > best_score:
                best_score = score
                best_thresh = thresh
    
    improvement = best_score - current_score
    if improvement > 0.01:
        msg = f"Found optimized threshold: {best_thresh}c (Score: {best_score:.2f}, +{improvement:.2%})"
    else:
        msg = f"Current threshold ({current_thresh}c) is optimal."
        best_thresh = current_thresh
        
    return {'hike_threshold': best_thresh}, msg
