# TabM: Tabular Deep Learning for Fuel Price Prediction

## Overview
This document outlines a strategy for implementing **TabM (Tabular Deep Learning with Mini-Batch Ensemble)** to predict Brisbane fuel prices. TabM is a state-of-the-art architecture designed to outperform Gradient Boosted Decision Trees (GBDTs) like XGBoost on tabular datasets.

## The Concept
Fuel price cycles are "tabular" time-series data. Traditional deep learning (MLPs) often struggles with tabular data compared to tree-based models. **TabM** solves this by processing a "mini-batch" of samples (e.g., prices from the last 30 days) together, allowing the model to learn context and relationships *between* samples in a batch, acting like an ensemble at inference time.

## Architecture Design

### 1. Inputs (Features)
To train a TabM model for fuel cycles, we would construct a feature vector $X$ containing:

*   **Global Market Indicators (The "Physics"):**
    *   `Brent_Oil_Price_USD` (Lagged -10d, -7d, -1d)
    *   `AUD_USD_Exchange_Rate`
    *   `Terminal_Gate_Price (TGP)`
*   **Cycle Context:**
    *   `Days_Since_Last_Hike` (Integer)
    *   `Current_Median_Price` (CPL)
    *   `Rate_of_Change_7d` (Slope of price)
*   **Temporal Features:**
    *   `Day_of_Week` (0-6)
    *   `Month` (1-12)
    *   `Is_Holiday` (Boolean)

### 2. The Model (TabM)
The TabM architecture consists of:
1.  **Embeddings:** Categorical variables (Day of Week) are mapped to dense vectors.
2.  **Mini-Batch Ensemble Layer:** Instead of treating each row independently, the layer attends to other rows in the batch to find similar market conditions (e.g., "This looks like the cycle from last March").
3.  **GLU Block (Gated Linear Units):** Allows the network to select which features are important for the current phase (e.g., TGP matters more in the "Bottom" phase, but "Days Since Hike" matters more in the "Hike" phase).
4.  **Output Head:** A scalar regression outputting `Predicted_Price_CPL`.

### 3. Training Strategy
*   **Loss Function:** MSE (Mean Squared Error) or Huber Loss (robust to outliers).
*   **Data Augmentation:** "MixUp" can be used on the tabular features to generalize better.

## Implementation Roadmap
1.  **Data Collection:** We already have `live_snapshot.csv` and historical traces. We need to build a `training_set.csv` with at least 2 years of daily data.
2.  **Preprocessing:** Normalize continuous variables (StandardScaler) and Encode categoricals.
3.  **Model Definition (PyTorch):**
    ```python
    class TabM(nn.Module):
        def __init__(self, num_features, hidden_dim, k_ensemble=32):
            super().__init__()
            self.ensemble = MiniBatchEnsemble(k_ensemble)
            self.layers = nn.Sequential(
                nn.Linear(num_features, hidden_dim),
                nn.ReLU(),
                nn.Linear(hidden_dim, 1)
            )
        ...
    ```
4.  **Inference:**
    The Dashboard would load the `.pth` weights and run a forward pass on today's indicators to generate the 14-day forecast curve.

## Why this is better?
Current logic uses "If/Else" rules (e.g., "If dropping, decay by 10%"). A TabM model learns the *exact shape* of the decay curve and the *exact trigger point* of the hike based on subtle oil price movements that humans miss.
