"""
test_monitoring.py
Week 4 — Tests for monitoring logic

Tests cover:
  - compute_psi: correct values, edge cases
  - metric_psi_lag_features: detects deflation
  - metric_baseline_correlation: detects correlation collapse
  - metric_peak_shift: detects temporal inversion
  - metric_manhattan_weekend_ratio: detects concept drift
  - check_drift_thresholds evaluate(): correct WARNING/CRITICAL/OK assignment

Run with:
    pytest week4/tests/test_monitoring.py -v
"""

import json
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Make scripts/ importable regardless of working directory
# ---------------------------------------------------------------------------
SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from compute_monitoring_metrics import (
    compute_psi,
    pearson_corr,
    BASELINE_END,
    DRIFT_START,
)
from check_drift_thresholds import evaluate, THRESHOLDS


# ===========================================================================
# Fixtures — synthetic DataFrames
# ===========================================================================

def _make_df(n_base=2000, n_drift=500, seed=42):
    """
    Build a minimal synthetic demand DataFrame with two time periods:
      - baseline: time_bucket <= BASELINE_END
      - drift:    time_bucket >= DRIFT_START

    Injected drift patterns:
      1. lag_1day deflated ×0.45 for borough 0 (Manhattan) in drift period
      2. zone_slot_baseline decorrelated from trip_count for borough 1 (Queens)
      3. Early slots (20-27) boosted ×15, late slots (36-43) reduced ×0.5 in drift period
      4. Manhattan weekends reduced ×0.72 in drift period
    """
    rng = np.random.default_rng(seed)

    # -- baseline rows -------------------------------------------------------
    base_dates = pd.date_range("2025-01-01", periods=n_base, freq="15min")
    base = pd.DataFrame({
        "time_bucket":        base_dates,
        "borough_id":         rng.integers(0, 3, n_base),
        "PULocationID":       rng.integers(1, 10, n_base),
        "trip_count":         rng.integers(5, 50, n_base).astype(float),
        "lag_1day":           rng.uniform(10, 30, n_base).astype("float32"),
        "lag_1week":          rng.uniform(10, 30, n_base).astype("float32"),
        "roll_mean_1day":     rng.uniform(10, 30, n_base).astype("float32"),
        "zone_slot_baseline": rng.uniform(5, 40, n_base),
        "slot_of_day":        rng.integers(0, 96, n_base),
        "is_weekend":         rng.integers(0, 2, n_base).astype("int8"),
    })
    # Align trip_count with zone_slot_baseline in baseline (positive correlation)
    base["trip_count"] = base["zone_slot_baseline"] + rng.uniform(-5, 5, n_base)

    # -- drift rows ----------------------------------------------------------
    drift_dates = pd.date_range("2026-02-02", periods=n_drift, freq="15min")
    drift = pd.DataFrame({
        "time_bucket":        drift_dates,
        "borough_id":         rng.integers(0, 3, n_drift),
        "PULocationID":       rng.integers(1, 10, n_drift),
        "trip_count":         rng.integers(5, 50, n_drift).astype(float),
        "lag_1day":           rng.uniform(10, 30, n_drift).astype("float32"),
        "lag_1week":          rng.uniform(10, 30, n_drift).astype("float32"),
        "roll_mean_1day":     rng.uniform(10, 30, n_drift).astype("float32"),
        "zone_slot_baseline": rng.uniform(5, 40, n_drift),
        "slot_of_day":        rng.integers(0, 96, n_drift),
        "is_weekend":         rng.integers(0, 2, n_drift).astype("int8"),
    })
    drift["trip_count"] = drift["zone_slot_baseline"] + rng.uniform(-5, 5, n_drift)

    # Inject drift 1: Manhattan lag deflation ×0.45
    mnh_mask = drift["borough_id"] == 0
    for col in ["lag_1day", "lag_1week", "roll_mean_1day"]:
        drift.loc[mnh_mask, col] = (drift.loc[mnh_mask, col] * 0.45).astype("float32")

    # Inject drift 3: Queens baseline scramble (decorrelate)
    qns_mask = drift["borough_id"] == 1
    drift.loc[qns_mask, "zone_slot_baseline"] = rng.uniform(0, 100, qns_mask.sum())
    # trip_count stays correlated with original — now uncorrelated with new baseline

    # Inject drift 1: temporal peak shift
    early = (drift["slot_of_day"] >= 20) & (drift["slot_of_day"] <= 27)
    late  = (drift["slot_of_day"] >= 36) & (drift["slot_of_day"] <= 43)
    drift.loc[early, "trip_count"] = (drift.loc[early, "trip_count"] * 15).astype(float)
    drift.loc[late,  "trip_count"] = (drift.loc[late,  "trip_count"] * 0.5).astype(float)

    # Inject drift 4: Manhattan weekend concept drift ×0.72
    mnh_wkend = (drift["borough_id"] == 0) & (drift["is_weekend"] == 1)
    drift.loc[mnh_wkend, "trip_count"] = (drift.loc[mnh_wkend, "trip_count"] * 0.72).astype(float)

    return pd.concat([base, drift], ignore_index=True)


@pytest.fixture(scope="module")
def synthetic_df():
    return _make_df()


@pytest.fixture(scope="module")
def parquet_path(synthetic_df, tmp_path_factory):
    path = tmp_path_factory.mktemp("data") / "demand_test.parquet"
    synthetic_df.to_parquet(path, index=False)
    return str(path)


# ===========================================================================
# 1. compute_psi
# ===========================================================================

class TestComputePsi:
    def test_identical_distributions_returns_zero(self):
        arr = np.random.default_rng(0).uniform(0, 10, 1000)
        psi = compute_psi(arr, arr.copy())
        assert psi < 0.01, f"Expected near-zero PSI for identical distributions, got {psi:.4f}"

    def test_shifted_distribution_returns_high_psi(self):
        rng = np.random.default_rng(1)
        base = rng.normal(10, 2, 2000)
        curr = rng.normal(20, 2, 2000)   # completely different mean
        psi = compute_psi(base, curr)
        assert psi > 0.20, f"Expected PSI > 0.20 for heavily shifted distribution, got {psi:.4f}"

    def test_moderate_shift_in_warning_range(self):
        rng = np.random.default_rng(2)
        base = rng.normal(10, 2, 2000)
        curr = rng.normal(10.5, 2, 2000)  # small shift — should be in warning zone
        psi = compute_psi(base, curr)
        assert 0.05 < psi < 0.50, f"Expected moderate PSI, got {psi:.4f}"

    def test_constant_array_returns_zero(self):
        arr = np.ones(500)
        psi = compute_psi(arr, arr)
        assert psi == 0.0

    def test_non_negative(self):
        rng = np.random.default_rng(3)
        base = rng.uniform(0, 10, 500)
        curr = rng.uniform(0, 15, 500)
        psi = compute_psi(base, curr)
        assert psi >= 0.0


# ===========================================================================
# 2. pearson_corr
# ===========================================================================

class TestPearsonCorr:
    def test_perfect_positive_correlation(self):
        x = pd.Series(np.arange(100, dtype=float))
        y = x * 2 + 5
        assert abs(pearson_corr(x, y) - 1.0) < 1e-6

    def test_no_correlation(self):
        rng = np.random.default_rng(10)
        x = pd.Series(rng.uniform(0, 10, 1000))
        y = pd.Series(rng.uniform(0, 10, 1000))
        corr = pearson_corr(x, y)
        assert abs(corr) < 0.15, f"Expected near-zero correlation, got {corr:.4f}"

    def test_handles_nan(self):
        x = pd.Series([1.0, 2.0, np.nan, 4.0])
        y = pd.Series([2.0, 4.0, 6.0, 8.0])
        corr = pearson_corr(x, y)
        assert not np.isnan(corr)

    def test_insufficient_data_returns_nan(self):
        x = pd.Series([1.0])
        y = pd.Series([2.0])
        result = pearson_corr(x, y)
        assert np.isnan(result)


# ===========================================================================
# 3. Metric functions (integration-style: run on parquet fixture)
# ===========================================================================

class TestMetricFunctions:
    """Run the actual metric functions on the synthetic parquet and check direction of results."""

    def test_psi_lag_deflation_detected(self, parquet_path):
        """Manhattan lag PSI should be HIGH (drift injected)."""
        from compute_monitoring_metrics import metric_psi_lag_features
        results = metric_psi_lag_features(parquet_path)
        mnh_psi = results["psi_lag_1day_manhattan"]["value"]
        assert mnh_psi > 0.10, (
            f"Expected PSI > 0.10 for Manhattan lag_1day (drift injected), got {mnh_psi:.4f}"
        )

    def test_psi_stable_for_non_manhattan_lag(self, parquet_path):
        """Global trip_count PSI should be non-trivially computable (no crash)."""
        from compute_monitoring_metrics import metric_psi_trip_count
        results = metric_psi_trip_count(parquet_path)
        assert "psi_trip_count_global" in results
        assert results["psi_trip_count_global"]["value"] >= 0.0

    def test_correlation_collapse_detected_queens(self, parquet_path):
        """Queens zone_slot_baseline correlation should be LOW (scramble injected)."""
        from compute_monitoring_metrics import metric_baseline_correlation
        results = metric_baseline_correlation(parquet_path)
        qns_corr = results["corr_baseline_vs_actual_queens"]["value"]
        assert qns_corr < 0.35, (
            f"Expected low correlation for Queens (scramble injected), got {qns_corr:.4f}"
        )

    def test_correlation_drop_is_positive_for_queens(self, parquet_path):
        """Baseline corr should be higher than drift corr — drop should be positive."""
        from compute_monitoring_metrics import metric_baseline_correlation
        results = metric_baseline_correlation(parquet_path)
        drop = results["corr_baseline_vs_actual_queens"]["drop"]
        assert drop > 0, f"Expected positive correlation drop for Queens, got {drop:.4f}"

    def test_peak_ratio_shift_detected(self, parquet_path):
        """Peak ratio shift should be strongly positive (early peak boosted ×15)."""
        from compute_monitoring_metrics import metric_peak_shift
        results = metric_peak_shift(parquet_path)
        shift = results["peak_ratio_shift"]["value"]
        assert shift > 1.0, (
            f"Expected peak ratio shift > 1.0 (temporal inversion injected), got {shift:.4f}"
        )

    def test_manhattan_weekend_ratio_shift_detected(self, parquet_path):
        """Manhattan weekend ratio should drop (×0.72 injected)."""
        from compute_monitoring_metrics import metric_manhattan_weekend_ratio
        results = metric_manhattan_weekend_ratio(parquet_path)
        shift = results["manhattan_weekend_ratio_shift"]["value"]
        assert shift < -0.05, (
            f"Expected negative ratio shift (concept drift injected), got {shift:.4f}"
        )

    def test_manhattan_lag_mean_ratio_below_one(self, parquet_path):
        """Manhattan lag_1day mean ratio should be < 0.80 (deflation injected)."""
        from compute_monitoring_metrics import metric_manhattan_lag_mean_ratio
        results = metric_manhattan_lag_mean_ratio(parquet_path)
        ratio = results["manhattan_lag1day_mean_ratio"]["value"]
        assert ratio < 0.80, (
            f"Expected ratio < 0.80 (×0.45 deflation injected), got {ratio:.4f}"
        )


# ===========================================================================
# 4. check_drift_thresholds — evaluate()
# ===========================================================================

class TestEvaluate:
    """Test that the threshold evaluator assigns correct statuses."""

    def _make_metrics(self, overrides: dict) -> dict:
        """Build a clean (all-OK) metrics dict with selective overrides."""
        defaults = {
            "psi_lag_1day_global":                     {"value": 0.05},
            "psi_lag_1week_global":                    {"value": 0.05},
            "psi_roll_mean_1day_global":               {"value": 0.05},
            "psi_roll_mean_1day_manhattan":            {"value": 0.05},
            "psi_trip_count_global":                   {"value": 0.05},
            "corr_baseline_vs_actual_queens":          {"value": 0.50},
            "corr_baseline_vs_actual_brooklyn":        {"value": 0.50},
            "peak_ratio_shift":                        {"value": 0.10},
            "manhattan_weekend_ratio_shift":           {"value": -0.02},
            "manhattan_lag1day_mean_ratio":            {"value": 0.95},
        }
        defaults.update(overrides)
        return defaults

    def test_all_ok_when_within_thresholds(self):
        metrics = self._make_metrics({})
        results = evaluate(metrics)
        statuses = {r.metric_key: r.status for r in results}
        for key, status in statuses.items():
            assert status == "OK", f"{key} should be OK but got {status}"

    def test_critical_psi_lag(self):
        metrics = self._make_metrics({"psi_lag_1day_global": {"value": 0.35}})
        results = evaluate(metrics)
        r = next(r for r in results if r.metric_key == "psi_lag_1day_global")
        assert r.status == "CRITICAL"

    def test_warning_psi_lag(self):
        metrics = self._make_metrics({"psi_lag_1day_global": {"value": 0.15}})
        results = evaluate(metrics)
        r = next(r for r in results if r.metric_key == "psi_lag_1day_global")
        assert r.status == "WARNING"

    def test_critical_correlation_collapse_queens(self):
        metrics = self._make_metrics({"corr_baseline_vs_actual_queens": {"value": 0.05}})
        results = evaluate(metrics)
        r = next(r for r in results if r.metric_key == "corr_baseline_vs_actual_queens")
        assert r.status == "CRITICAL"

    def test_warning_correlation_queens(self):
        metrics = self._make_metrics({"corr_baseline_vs_actual_queens": {"value": 0.25}})
        results = evaluate(metrics)
        r = next(r for r in results if r.metric_key == "corr_baseline_vs_actual_queens")
        assert r.status == "WARNING"

    def test_critical_peak_ratio_shift(self):
        metrics = self._make_metrics({"peak_ratio_shift": {"value": 1.95}})
        results = evaluate(metrics)
        r = next(r for r in results if r.metric_key == "peak_ratio_shift")
        assert r.status == "CRITICAL"

    def test_critical_weekend_ratio_shift(self):
        metrics = self._make_metrics({"manhattan_weekend_ratio_shift": {"value": -0.20}})
        results = evaluate(metrics)
        r = next(r for r in results if r.metric_key == "manhattan_weekend_ratio_shift")
        assert r.status == "CRITICAL"

    def test_critical_lag_mean_ratio(self):
        metrics = self._make_metrics({"manhattan_lag1day_mean_ratio": {"value": 0.44}})
        results = evaluate(metrics)
        r = next(r for r in results if r.metric_key == "manhattan_lag1day_mean_ratio")
        assert r.status == "CRITICAL"

    def test_missing_metric_handled_gracefully(self):
        metrics = self._make_metrics({})
        del metrics["psi_lag_1day_global"]
        results = evaluate(metrics)
        r = next(r for r in results if r.metric_key == "psi_lag_1day_global")
        assert r.status == "MISSING"

    def test_all_critical_on_week4_data(self):
        """Simulate the actual Week 4 observed values — should fire CRITICAL on 9 metrics."""
        metrics = self._make_metrics({
            "psi_lag_1day_global":              {"value": 0.2886},
            "psi_lag_1week_global":             {"value": 0.2624},
            "psi_roll_mean_1day_global":        {"value": 0.6382},
            "psi_roll_mean_1day_manhattan":     {"value": 1.0174},
            "psi_trip_count_global":            {"value": 0.0545},   # only OK one
            "corr_baseline_vs_actual_queens":   {"value": 0.0052},
            "corr_baseline_vs_actual_brooklyn": {"value": 0.0930},
            "peak_ratio_shift":                 {"value": 1.9550},
            "manhattan_weekend_ratio_shift":    {"value": -0.1666},
            "manhattan_lag1day_mean_ratio":     {"value": 0.4364},
        })
        results = evaluate(metrics)
        critical = [r for r in results if r.status == "CRITICAL"]
        ok       = [r for r in results if r.status == "OK"]
        assert len(critical) == 9, f"Expected 9 CRITICAL, got {len(critical)}"
        assert len(ok) == 1,       f"Expected 1 OK, got {len(ok)}"
        assert ok[0].metric_key == "psi_trip_count_global"
