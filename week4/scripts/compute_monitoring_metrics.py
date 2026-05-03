#!/usr/bin/env python3
"""
compute_monitoring_metrics.py
Week 4 — Monitoring, Drift Detection & Retraining Strategy

Computes 8 monitoring metrics by comparing a baseline period
(training data up to 2026-01-15) against a recent window
(2026-02-02 to 2026-02-28) in demand_enriched_week4.parquet.

Metrics computed
----------------
1.  PSI — lag_1day            (global)
2.  PSI — lag_1week           (global)
3.  PSI — roll_mean_1day      (global)
4.  PSI — roll_mean_1day      (Manhattan only)
5.  PSI — trip_count          (global)
6.  Correlation drop — zone_slot_baseline vs trip_count (Queens)
7.  Correlation drop — zone_slot_baseline vs trip_count (Brooklyn)
8.  Early/Late morning peak ratio shift  (slots 20-27 vs 36-43)
9.  Manhattan weekend/weekday trip ratio
10. Manhattan lag_1day mean ratio

Output
------
Writes monitoring_metrics.json to the same directory as --output (default: data/processed/).

Usage
-----
python scripts/compute_monitoring_metrics.py \
    --input  data/processed/demand_enriched_week4.parquet \
    --baseline data/processed/monitoring_baseline.json \
    --output data/processed/monitoring_metrics.json
"""

import argparse
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ── Period boundaries ────────────────────────────────────────────────────────
BASELINE_END = pd.Timestamp("2026-01-15")
DRIFT_START  = pd.Timestamp("2026-02-02")

# ── PSI helper ───────────────────────────────────────────────────────────────
def compute_psi(baseline: np.ndarray, current: np.ndarray, bins: int = 10) -> float:
    """
    Population Stability Index.
    PSI < 0.10  → no significant shift
    PSI 0.10-0.20 → moderate shift, monitor closely
    PSI > 0.20  → significant shift, likely retraining needed
    """
    combined = np.concatenate([baseline, current])
    lo, hi = np.nanpercentile(combined, 1), np.nanpercentile(combined, 99)
    if lo == hi:
        return 0.0
    breakpoints = np.linspace(lo, hi, bins + 1)

    def _bucket(arr: np.ndarray) -> np.ndarray:
        counts, _ = np.histogram(arr, bins=breakpoints)
        pct = counts / max(len(arr), 1)
        return np.where(pct == 0, 1e-4, pct)  # avoid log(0)

    b_pct = _bucket(baseline)
    c_pct = _bucket(current)
    return float(np.sum((c_pct - b_pct) * np.log(c_pct / b_pct)))


# ── Correlation helper ────────────────────────────────────────────────────────
def pearson_corr(x: pd.Series, y: pd.Series) -> float:
    mask = x.notna() & y.notna()
    if mask.sum() < 2:
        return float("nan")
    return float(x[mask].corr(y[mask]))


# ── Individual metric computers ───────────────────────────────────────────────

def metric_psi_lag_features(path: str) -> dict:
    """Metrics 1-4: PSI for lag features (global and Manhattan)."""
    cols = ["time_bucket", "borough_id", "lag_1day", "lag_1week", "roll_mean_1day"]
    df = pd.read_parquet(path, columns=cols)

    results = {}
    for feat in ["lag_1day", "lag_1week", "roll_mean_1day"]:
        # Global
        base = df[df["time_bucket"] <= BASELINE_END][feat].dropna().values
        curr = df[df["time_bucket"] >= DRIFT_START][feat].dropna().values
        results[f"psi_{feat}_global"] = {
            "value": compute_psi(base, curr),
            "description": f"PSI for {feat} across all zones",
            "baseline_mean": float(np.nanmean(base)),
            "current_mean": float(np.nanmean(curr)),
        }
        # Manhattan only
        base_m = df[(df["time_bucket"] <= BASELINE_END) & (df["borough_id"] == 0)][feat].dropna().values
        curr_m = df[(df["time_bucket"] >= DRIFT_START)  & (df["borough_id"] == 0)][feat].dropna().values
        results[f"psi_{feat}_manhattan"] = {
            "value": compute_psi(base_m, curr_m),
            "description": f"PSI for {feat} in Manhattan only",
            "baseline_mean": float(np.nanmean(base_m)),
            "current_mean": float(np.nanmean(curr_m)),
        }
    return results


def metric_psi_trip_count(path: str) -> dict:
    """Metric 5: PSI for trip_count distribution."""
    cols = ["time_bucket", "trip_count"]
    df = pd.read_parquet(path, columns=cols)
    base = df[df["time_bucket"] <= BASELINE_END]["trip_count"].dropna().values
    curr = df[df["time_bucket"] >= DRIFT_START]["trip_count"].dropna().values
    return {
        "psi_trip_count_global": {
            "value": compute_psi(base, curr),
            "description": "PSI for trip_count distribution across all zones",
            "baseline_mean": float(np.nanmean(base)),
            "current_mean": float(np.nanmean(curr)),
        }
    }


def metric_baseline_correlation(path: str) -> dict:
    """Metrics 6-7: Pearson correlation between zone_slot_baseline and trip_count per borough."""
    cols = ["time_bucket", "borough_id", "zone_slot_baseline", "trip_count"]
    df = pd.read_parquet(path, columns=cols)

    results = {}
    for borough_id, borough_name in [(1, "queens"), (2, "brooklyn"), (0, "manhattan")]:
        sub = df[df["borough_id"] == borough_id]
        base_corr = pearson_corr(
            sub[sub["time_bucket"] <= BASELINE_END]["zone_slot_baseline"],
            sub[sub["time_bucket"] <= BASELINE_END]["trip_count"],
        )
        curr_corr = pearson_corr(
            sub[sub["time_bucket"] >= DRIFT_START]["zone_slot_baseline"],
            sub[sub["time_bucket"] >= DRIFT_START]["trip_count"],
        )
        results[f"corr_baseline_vs_actual_{borough_name}"] = {
            "value": curr_corr,
            "baseline_value": base_corr,
            "drop": round(base_corr - curr_corr, 4),
            "description": (
                f"Pearson correlation between zone_slot_baseline and trip_count "
                f"in {borough_name.title()} (drop = baseline - current)"
            ),
        }
    return results


def metric_peak_shift(path: str) -> dict:
    """Metric 8: Early-morning (5-7am) vs late-morning (9-11am) trip count ratio."""
    cols = ["time_bucket", "slot_of_day", "trip_count"]
    df = pd.read_parquet(path, columns=cols)

    results = {}
    for period_name, mask in [
        ("baseline", df["time_bucket"] <= BASELINE_END),
        ("current",  df["time_bucket"] >= DRIFT_START),
    ]:
        sub = df[mask]
        early = sub[(sub["slot_of_day"] >= 20) & (sub["slot_of_day"] <= 27)]["trip_count"].mean()
        late  = sub[(sub["slot_of_day"] >= 36) & (sub["slot_of_day"] <= 43)]["trip_count"].mean()
        results[f"peak_ratio_{period_name}"] = {
            "early_5_7am_mean": round(float(early), 4),
            "late_9_11am_mean": round(float(late), 4),
            "ratio_early_over_late": round(float(early / late), 4) if late else None,
        }

    base_ratio = results["peak_ratio_baseline"]["ratio_early_over_late"]
    curr_ratio = results["peak_ratio_current"]["ratio_early_over_late"]
    results["peak_ratio_shift"] = {
        "value": round(curr_ratio - base_ratio, 4),
        "baseline_ratio": base_ratio,
        "current_ratio": curr_ratio,
        "description": (
            "Change in (5-7am avg trips) / (9-11am avg trips). "
            "Positive means morning demand moved earlier."
        ),
    }
    return results


def metric_manhattan_weekend_ratio(path: str) -> dict:
    """Metric 9: Manhattan weekend/weekday trip count ratio."""
    cols = ["time_bucket", "borough_id", "is_weekend", "trip_count"]
    df = pd.read_parquet(path, columns=cols)
    mnh = df[df["borough_id"] == 0]

    results = {}
    for period_name, mask in [
        ("baseline", mnh["time_bucket"] <= BASELINE_END),
        ("current",  mnh["time_bucket"] >= DRIFT_START),
    ]:
        sub = mnh[mask]
        wkend = sub[sub["is_weekend"] == 1]["trip_count"].mean()
        wkday = sub[sub["is_weekend"] == 0]["trip_count"].mean()
        ratio = float(wkend / wkday) if wkday else None
        results[f"manhattan_weekend_ratio_{period_name}"] = {
            "weekend_mean": round(float(wkend), 4),
            "weekday_mean": round(float(wkday), 4),
            "ratio": round(ratio, 4) if ratio else None,
        }

    base_r = results["manhattan_weekend_ratio_baseline"]["ratio"]
    curr_r = results["manhattan_weekend_ratio_current"]["ratio"]
    results["manhattan_weekend_ratio_shift"] = {
        "value": round(curr_r - base_r, 4),
        "baseline_ratio": base_r,
        "current_ratio": curr_r,
        "description": (
            "Manhattan weekend/weekday ratio shift. "
            "Negative means weekends are relatively quieter than in training."
        ),
    }
    return results


def metric_manhattan_lag_mean_ratio(path: str) -> dict:
    """Metric 10: Manhattan lag_1day mean ratio (current / baseline)."""
    cols = ["time_bucket", "borough_id", "lag_1day"]
    df = pd.read_parquet(path, columns=cols)
    mnh = df[df["borough_id"] == 0]
    base_mean = mnh[mnh["time_bucket"] <= BASELINE_END]["lag_1day"].mean()
    curr_mean = mnh[mnh["time_bucket"] >= DRIFT_START]["lag_1day"].mean()
    ratio = float(curr_mean / base_mean) if base_mean else None
    return {
        "manhattan_lag1day_mean_ratio": {
            "value": round(ratio, 4) if ratio else None,
            "baseline_mean": round(float(base_mean), 4),
            "current_mean": round(float(curr_mean), 4),
            "description": (
                "Ratio of current-period lag_1day mean to baseline mean for Manhattan. "
                "Values well below 1.0 indicate lag deflation."
            ),
        }
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Compute drift monitoring metrics")
    parser.add_argument(
        "--input",
        default="data/processed/demand_enriched_week4.parquet",
        help="Path to demand_enriched_week4.parquet",
    )
    parser.add_argument(
        "--baseline",
        default="data/processed/monitoring_baseline.json",
        help="Path to monitoring_baseline.json (written by simulate_week4.py)",
    )
    parser.add_argument(
        "--output",
        default="data/processed/monitoring_metrics.json",
        help="Where to write the computed metrics JSON",
    )
    args = parser.parse_args()

    logger.info(f"Loading parquet: {args.input}")
    logger.info(f"Baseline period : up to {BASELINE_END.date()}")
    logger.info(f"Current window  : {DRIFT_START.date()} – 2026-02-28")

    all_metrics: dict = {}

    logger.info("Computing PSI for lag features...")
    all_metrics.update(metric_psi_lag_features(args.input))

    logger.info("Computing PSI for trip_count...")
    all_metrics.update(metric_psi_trip_count(args.input))

    logger.info("Computing baseline–actual correlation...")
    all_metrics.update(metric_baseline_correlation(args.input))

    logger.info("Computing peak shift ratio...")
    all_metrics.update(metric_peak_shift(args.input))

    logger.info("Computing Manhattan weekend ratio...")
    all_metrics.update(metric_manhattan_weekend_ratio(args.input))

    logger.info("Computing Manhattan lag mean ratio...")
    all_metrics.update(metric_manhattan_lag_mean_ratio(args.input))

    # Load baseline JSON for reference
    if Path(args.baseline).exists():
        with open(args.baseline) as f:
            baseline_ref = json.load(f)
        all_metrics["_baseline_reference"] = baseline_ref

    output = {
        "computed_at": pd.Timestamp.now().isoformat(),
        "baseline_period_end": BASELINE_END.isoformat(),
        "current_window_start": DRIFT_START.isoformat(),
        "metrics": all_metrics,
    }

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2, default=str)

    logger.info(f"✓ Metrics written to {args.output}")
    logger.info(f"  Total metrics computed: {len(all_metrics)}")

    # Print a quick summary
    print("\n── Metric Summary ─────────────────────────────────────────────")
    for k, v in all_metrics.items():
        if k.startswith("_"):
            continue
        val = v.get("value") if isinstance(v, dict) else None
        if val is not None:
            print(f"  {k:<50s}  {val:.4f}")
    print("────────────────────────────────────────────────────────────────")


if __name__ == "__main__":
    main()
