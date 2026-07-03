"""
cycle_detector.py — Markov-like regime detector for Australian Edgeworth
fuel-price cycles.

Approach
--------
Instead of statsmodels (which struggles with sparse daily fuel data), this
module uses an empirical strategy:

1. Compute daily price deltas.
2. Classify each day:  delta > +5 cpl → RESTORATION, else → UNDERCUTTING.
3. Build a 2×2 transition matrix from the classified sequence.
4. Use scipy.signal.find_peaks (with rolling-window fallback) for
   peak / trough detection.
5. Calibrate physical-model parameters from the observed data.
"""

import logging
from typing import Dict, List, Tuple, Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Try scipy for peak detection; fall back to rolling-window approach
try:
    from scipy.signal import find_peaks as _scipy_find_peaks
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False
    logger.info("scipy not available — using rolling-window peak detection")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_RESTORATION_THRESHOLD = 5.0   # cpl jump that marks a restoration day
_MIN_PEAK_PROMINENCE   = 8.0   # prominence for scipy peak detection
_MIN_TROUGH_PROMINENCE = 8.0
_ROLLING_WINDOW        = 7     # window for fallback peak/trough detection


# ---------------------------------------------------------------------------
# Helper: rolling-window peak / trough finder (scipy-free)
# ---------------------------------------------------------------------------
def _rolling_find_peaks(prices: np.ndarray, window: int = _ROLLING_WINDOW) -> np.ndarray:
    """
    Identify local maxima using a simple sliding-window comparison.
    A point at index *i* is a peak if it is the maximum within
    ``prices[i - window//2 : i + window//2 + 1]``.
    """
    half = window // 2
    peaks: list[int] = []
    for i in range(half, len(prices) - half):
        segment = prices[i - half: i + half + 1]
        if prices[i] == np.max(segment) and prices[i] > prices[i - 1]:
            peaks.append(i)
    return np.array(peaks, dtype=int)


def _rolling_find_troughs(prices: np.ndarray, window: int = _ROLLING_WINDOW) -> np.ndarray:
    """Mirror of ``_rolling_find_peaks`` for local minima."""
    half = window // 2
    troughs: list[int] = []
    for i in range(half, len(prices) - half):
        segment = prices[i - half: i + half + 1]
        if prices[i] == np.min(segment) and prices[i] < prices[i - 1]:
            troughs.append(i)
    return np.array(troughs, dtype=int)


# ---------------------------------------------------------------------------
# CycleDetector
# ---------------------------------------------------------------------------
class CycleDetector:
    """
    Empirical Edgeworth-cycle detector.

    Usage::

        cd = CycleDetector()
        cd.fit(daily_prices_series)
        info = cd.detect_current_regime(daily_prices_series)
    """

    # States
    UNDERCUTTING = "UNDERCUTTING"
    RESTORATION  = "RESTORATION"

    def __init__(self):
        # Transition matrix  [from_state, to_state]
        # row 0 = UNDERCUTTING, row 1 = RESTORATION
        self.transition_matrix: np.ndarray = np.array(
            [[0.85, 0.15],
             [0.70, 0.30]], dtype=float
        )

        # Calibrated physical-model parameters (defaults until fit() runs)
        self.daily_decay: float       = 1.3   # cpl/day during undercutting
        self.spike_magnitude: float   = 25.0  # cpl jump on restoration day 1
        self.floor_margin: float      = 6.0   # margin above TGP that triggers hike
        self.cycle_period: float      = 28.0  # average full-cycle days

        # Cycle metrics (populated by fit)
        self.cycle_metrics: Dict[str, float] = {}

        # Internal
        self._fitted = False

    # ------------------------------------------------------------------
    # Peak / trough detection (public)
    # ------------------------------------------------------------------
    @staticmethod
    def find_peaks_and_troughs(
        prices_series: pd.Series,
    ) -> Dict[str, np.ndarray]:
        """
        Detect peaks and troughs in *prices_series*.

        Returns
        -------
        dict with keys ``peak_indices``, ``trough_indices``,
        ``peak_values``, ``trough_values``.
        """
        prices = np.asarray(prices_series, dtype=float)
        if len(prices) < 5:
            return {
                "peak_indices": np.array([], dtype=int),
                "trough_indices": np.array([], dtype=int),
                "peak_values": np.array([]),
                "trough_values": np.array([]),
            }

        if _HAS_SCIPY:
            peak_idx, _ = _scipy_find_peaks(prices, prominence=_MIN_PEAK_PROMINENCE, distance=5)
            trough_idx, _ = _scipy_find_peaks(-prices, prominence=_MIN_TROUGH_PROMINENCE, distance=5)
        else:
            peak_idx = _rolling_find_peaks(prices)
            trough_idx = _rolling_find_troughs(prices)

        return {
            "peak_indices": peak_idx,
            "trough_indices": trough_idx,
            "peak_values": prices[peak_idx] if len(peak_idx) else np.array([]),
            "trough_values": prices[trough_idx] if len(trough_idx) else np.array([]),
        }

    # ------------------------------------------------------------------
    # Regime classification helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _classify_days(deltas: np.ndarray) -> np.ndarray:
        """0 = UNDERCUTTING, 1 = RESTORATION."""
        return (deltas > _RESTORATION_THRESHOLD).astype(int)

    def _build_transition_matrix(self, labels: np.ndarray) -> np.ndarray:
        """
        Build a 2×2 row-stochastic transition matrix from a sequence of
        regime labels (0 or 1).
        """
        mat = np.zeros((2, 2), dtype=float)
        for i in range(len(labels) - 1):
            mat[labels[i], labels[i + 1]] += 1

        # Normalise rows
        row_sums = mat.sum(axis=1, keepdims=True)
        row_sums[row_sums == 0] = 1.0
        mat /= row_sums
        return mat

    # ------------------------------------------------------------------
    # fit()
    # ------------------------------------------------------------------
    def fit(self, daily_prices_series: pd.Series) -> "CycleDetector":
        """
        Calibrate the detector from historical daily median prices.

        Parameters
        ----------
        daily_prices_series : pd.Series
            Index-agnostic series of daily median prices (cpl), ordered
            chronologically.

        Returns
        -------
        self
        """
        prices = np.asarray(daily_prices_series, dtype=float)
        prices = prices[~np.isnan(prices)]

        if len(prices) < 10:
            logger.warning("fit: insufficient data (%d points), using defaults", len(prices))
            self._fitted = True
            self.cycle_metrics = self._default_metrics()
            return self

        # 1. Deltas & labels
        deltas = np.diff(prices)
        labels = self._classify_days(deltas)

        # 2. Transition matrix
        self.transition_matrix = self._build_transition_matrix(labels)

        # 3. Peak / trough analysis
        pt = self.find_peaks_and_troughs(pd.Series(prices))
        peak_idx = pt["peak_indices"]
        trough_idx = pt["trough_indices"]
        peak_vals = pt["peak_values"]
        trough_vals = pt["trough_values"]

        # 4. Cycle-length statistics
        if len(peak_idx) >= 2:
            cycle_lengths = np.diff(peak_idx).astype(float)
            avg_cycle = float(np.mean(cycle_lengths))
        else:
            cycle_lengths = np.array([])
            avg_cycle = 28.0  # fallback

        self.cycle_period = avg_cycle

        # 5. Phase durations — run-length encoding on labels
        undercut_runs: list[int] = []
        restore_runs: list[int] = []
        if len(labels) > 0:
            current_label = labels[0]
            run_len = 1
            for lbl in labels[1:]:
                if lbl == current_label:
                    run_len += 1
                else:
                    (undercut_runs if current_label == 0 else restore_runs).append(run_len)
                    current_label = lbl
                    run_len = 1
            (undercut_runs if current_label == 0 else restore_runs).append(run_len)

        avg_undercut = float(np.mean(undercut_runs)) if undercut_runs else 20.0
        avg_restore = float(np.mean(restore_runs)) if restore_runs else 2.0

        # 6. Calibrate physical-model parameters
        # Daily decay = average of negative deltas during undercutting
        neg_deltas = deltas[labels == 0]
        if len(neg_deltas) > 0:
            self.daily_decay = float(np.abs(np.median(neg_deltas)))
            if self.daily_decay < 0.3:
                self.daily_decay = 0.3  # sanity floor
        else:
            self.daily_decay = 1.3

        # Spike magnitude = average of positive deltas during restoration
        pos_deltas = deltas[labels == 1]
        if len(pos_deltas) > 0:
            self.spike_magnitude = float(np.mean(pos_deltas))
        else:
            self.spike_magnitude = 25.0

        # Floor margin: average trough above rolling-14d min
        if len(trough_idx) > 0 and len(prices) >= 14:
            rolling_min = pd.Series(prices).rolling(14, min_periods=1).min().values
            trough_margins = prices[trough_idx] - rolling_min[trough_idx]
            self.floor_margin = float(np.mean(np.abs(trough_margins)))
            if self.floor_margin < 2.0:
                self.floor_margin = 2.0
        else:
            self.floor_margin = 6.0

        # 7. Assemble cycle metrics
        self.cycle_metrics = {
            "avg_cycle_length": round(avg_cycle, 1),
            "avg_undercutting_duration": round(avg_undercut, 1),
            "avg_restoration_duration": round(avg_restore, 1),
            "avg_trough_price": round(float(np.mean(trough_vals)), 2) if len(trough_vals) else None,
            "avg_peak_price": round(float(np.mean(peak_vals)), 2) if len(peak_vals) else None,
            "avg_amplitude": (
                round(float(np.mean(peak_vals) - np.mean(trough_vals)), 2)
                if len(peak_vals) and len(trough_vals)
                else None
            ),
        }

        self._fitted = True
        logger.info(
            "CycleDetector.fit: period=%.1f days, decay=%.2f cpl/d, spike=%.1f cpl, "
            "floor_margin=%.1f cpl",
            self.cycle_period,
            self.daily_decay,
            self.spike_magnitude,
            self.floor_margin,
        )
        return self

    @staticmethod
    def _default_metrics() -> Dict[str, Any]:
        return {
            "avg_cycle_length": 28.0,
            "avg_undercutting_duration": 20.0,
            "avg_restoration_duration": 2.0,
            "avg_trough_price": None,
            "avg_peak_price": None,
            "avg_amplitude": None,
        }

    # ------------------------------------------------------------------
    # detect_current_regime()
    # ------------------------------------------------------------------
    def detect_current_regime(
        self, daily_prices_series: pd.Series
    ) -> Dict[str, Any]:
        """
        Analyse the tail of a price series and return a regime summary.

        Returns
        -------
        dict
            phase             : str   — 'UNDERCUTTING' or 'RESTORATION'
            confidence        : float — 0..1
            days_in_phase     : int
            estimated_days_remaining : int
            probabilities     : dict  — {UNDERCUTTING: p, RESTORATION: 1-p}
            cycle_metrics     : dict  — from fit()
        """
        prices = np.asarray(daily_prices_series, dtype=float)
        prices = prices[~np.isnan(prices)]

        # Edge case
        if len(prices) < 3:
            return self._default_regime()

        # Auto-fit if not yet fitted
        if not self._fitted:
            self.fit(pd.Series(prices))

        deltas = np.diff(prices)
        labels = self._classify_days(deltas)

        # Current phase = last label
        current_label = int(labels[-1])
        phase = self.RESTORATION if current_label == 1 else self.UNDERCUTTING

        # Days in current phase (run from the tail)
        days_in_phase = 1
        for lbl in reversed(labels[:-1]):
            if lbl == current_label:
                days_in_phase += 1
            else:
                break

        # Transition probabilities from current state
        prob_undercut = float(self.transition_matrix[current_label, 0])
        prob_restore  = float(self.transition_matrix[current_label, 1])

        # Confidence = probability of staying in current phase
        confidence = prob_undercut if current_label == 0 else prob_restore

        # Estimated days remaining in this phase
        if phase == self.UNDERCUTTING:
            avg_dur = self.cycle_metrics.get("avg_undercutting_duration", 20.0) or 20.0
        else:
            avg_dur = self.cycle_metrics.get("avg_restoration_duration", 2.0) or 2.0

        remaining = max(0, int(round(avg_dur - days_in_phase)))

        return {
            "phase": phase,
            "confidence": round(confidence, 3),
            "days_in_phase": days_in_phase,
            "estimated_days_remaining": remaining,
            "probabilities": {
                self.UNDERCUTTING: round(prob_undercut, 3),
                self.RESTORATION: round(prob_restore, 3),
            },
            "cycle_metrics": self.cycle_metrics,
        }

    def _default_regime(self) -> Dict[str, Any]:
        return {
            "phase": self.UNDERCUTTING,
            "confidence": 0.5,
            "days_in_phase": 0,
            "estimated_days_remaining": 0,
            "probabilities": {
                self.UNDERCUTTING: 0.5,
                self.RESTORATION: 0.5,
            },
            "cycle_metrics": self.cycle_metrics if self.cycle_metrics else self._default_metrics(),
        }
