#!/usr/bin/env python3
"""
Week 4 Drift Simulation Script (TA ONLY)

Injects 4 drift patterns into demand_enriched.parquet to simulate
real-world data drift and concept drift after 4 weeks of operation.

Drifts injected:
1. Temporal peak shift — trip counts shift earlier in the day
2. Manhattan lag deflation — lag features reduced for Manhattan only
3. Outer borough baseline scramble — zone_slot_baseline becomes uncorrelated
4. Manhattan weekend concept drift — weekend pattern reversal in Manhattan

Usage:
  python simulate_week4.py --input data/processed/demand_enriched.parquet \
    --output data/processed/demand_enriched_week4.parquet --seed 42
"""

import pandas as pd
import numpy as np
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import logging
import json

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Drift parameters
OPERATIONAL_DATE = pd.Timestamp("2026-01-15")
WEEK3_PERIOD_END = pd.Timestamp(
    "2026-02-01"
)  # Week 3: Jan 16 - Feb 1 (data quality issues)
DRIFT_START = pd.Timestamp(
    "2026-02-02"
)  # Lag deflation & concept drift: Feb 2+ (after Week 3 fixes)
TEMPORAL_DRIFT_START = pd.Timestamp(
    "2026-02-02"
)  # Temporal peak shift: Feb 2+ (Week 4 period)


def inject_temporal_peak_shift(df: pd.DataFrame, seed: int = 42) -> tuple:
    """Inject temporal peak shift — demand moves earlier in the day."""
    np.random.seed(seed)

    # Affected rows: all data after Nov 1, 2025
    mask_temporal = df["time_bucket"] >= TEMPORAL_DRIFT_START

    # Increase 5am-7am demand (slot_of_day 20-27)
    mask_early = mask_temporal & (df["slot_of_day"] >= 20) & (df["slot_of_day"] <= 27)
    df.loc[mask_early, "trip_count"] = (df.loc[mask_early, "trip_count"] * 1.45).astype(
        int
    )

    # Decrease 9am-11am demand (slot_of_day 36-43)
    mask_late = mask_temporal & (df["slot_of_day"] >= 36) & (df["slot_of_day"] <= 43)
    df.loc[mask_late, "trip_count"] = (df.loc[mask_late, "trip_count"] * 0.65).astype(
        int
    )

    count_early = mask_early.sum()
    count_late = mask_late.sum()

    logger.info(
        f"✓ Temporal peak shift: +45% for 5-7am ({count_early:,} rows), "
        f"-35% for 9-11am ({count_late:,} rows)"
    )

    return df, {
        "type": "temporal_peak_shift",
        "start_date": TEMPORAL_DRIFT_START.isoformat(),
        "early_peak_boost": 1.45,
        "late_peak_reduction": 0.65,
        "early_slots": "20-27 (5-7am)",
        "late_slots": "36-43 (9-11am)",
        "rows_affected": int(count_early + count_late),
    }


def inject_manhattan_lag_deflation(df: pd.DataFrame, seed: int = 42) -> tuple:
    """Inject lag feature reduction for Manhattan zones."""
    np.random.seed(seed)

    # Affected: Manhattan zones (borough_id=0) after Sep 1, 2025
    mask = (df["borough_id"] == 0) & (df["time_bucket"] >= DRIFT_START)

    # Reduce lag features by 45% (multiply by 0.55)
    for col in ["lag_1day", "lag_1week", "roll_mean_1day"]:
        df.loc[mask, col] = df.loc[mask, col] * 0.55

    count = mask.sum()
    logger.info(
        f"✓ Manhattan lag deflation: ×0.55 for lag_1day, lag_1week, roll_mean_1day "
        f"({count:,} rows)"
    )

    return df, {
        "type": "manhattan_lag_deflation",
        "start_date": DRIFT_START.isoformat(),
        "borough": "Manhattan (0)",
        "multiplier": 0.55,
        "affected_features": ["lag_1day", "lag_1week", "roll_mean_1day"],
        "rows_affected": int(count),
    }


def inject_outer_borough_baseline_scramble(df: pd.DataFrame, seed: int = 42) -> tuple:
    """Inject scrambled zone_slot_baseline for Queens/Brooklyn (breaks feature-target correlation)."""
    np.random.seed(seed)

    # Get unique outer borough zones
    outer_zone_mask = df["borough_id"].isin([1, 2])  # Queens=1, Brooklyn=2
    outer_zones = df[outer_zone_mask]["PULocationID"].unique()

    # For each zone, apply zone-specific multiplier (alternating between low and high)
    multipliers = {}
    for i, zone in enumerate(sorted(outer_zones)):
        if i % 2 == 0:
            multipliers[zone] = 0.22  # Very low
        else:
            multipliers[zone] = 3.8  # Very high

    # Apply multipliers
    for zone, mult in multipliers.items():
        mask_zone = (df["PULocationID"] == zone) & outer_zone_mask
        df.loc[mask_zone, "zone_slot_baseline"] = (
            df.loc[mask_zone, "zone_slot_baseline"] * mult
        )

    count = outer_zone_mask.sum()
    logger.info(
        f"✓ Outer borough baseline scramble: applied zone-specific multipliers to "
        f"{len(outer_zones)} zones ({count:,} rows)"
    )

    return df, {
        "type": "outer_borough_baseline_scramble",
        "boroughs": ["Queens (1)", "Brooklyn (2)"],
        "zones_affected": len(outer_zones),
        "multipliers_applied": "alternating 0.22 and 3.8 per zone",
        "rows_affected": int(count),
        "purpose": "breaks correlation between zone_slot_baseline and trip_count",
    }


def inject_manhattan_weekend_concept_drift(df: pd.DataFrame, seed: int = 42) -> tuple:
    """Inject weekend concept drift for Manhattan — weekends become less busy."""
    np.random.seed(seed)

    # Affected: Manhattan (borough_id=0), weekends (is_weekend=1), after Sep 1
    mask = (
        (df["borough_id"] == 0)
        & (df["is_weekend"] == 1)
        & (df["time_bucket"] >= DRIFT_START)
    )

    # Reduce trip_count by 28% on Manhattan weekends (reversal of training pattern)
    df.loc[mask, "trip_count"] = (df.loc[mask, "trip_count"] * 0.72).astype(int)

    count = mask.sum()
    logger.info(
        f"✓ Manhattan weekend concept drift: ×0.72 (28% reduction) for "
        f"{count:,} weekend rows in Manhattan"
    )

    return df, {
        "type": "manhattan_weekend_concept_drift",
        "start_date": DRIFT_START.isoformat(),
        "condition": "borough_id=0 AND is_weekend=1",
        "multiplier": 0.72,
        "rows_affected": int(count),
        "reason": "Weekend demand pattern changed in Manhattan (tourism down, hybrid work effect)",
    }


def compute_baseline_metrics(df: pd.DataFrame, cutoff_date: pd.Timestamp) -> dict:
    """Compute reference metrics for the training/baseline period."""
    df_baseline = df[df["time_bucket"] <= cutoff_date]

    metrics = {
        "period": f"2023-01-01 to {cutoff_date.date()}",
        "row_count": len(df_baseline),
        "zones": int(df_baseline["PULocationID"].nunique()),
        "feature_statistics": {},
    }

    for feature in ["lag_1day", "lag_1week", "zone_slot_baseline", "trip_count"]:
        feature_data = df_baseline[feature].dropna()
        if len(feature_data) > 0:
            metrics["feature_statistics"][feature] = {
                "mean": float(feature_data.mean()),
                "std": float(feature_data.std()),
                "p25": float(feature_data.quantile(0.25)),
                "p50": float(feature_data.quantile(0.50)),
                "p75": float(feature_data.quantile(0.75)),
                "p95": float(feature_data.quantile(0.95)),
                "min": float(feature_data.min()),
                "max": float(feature_data.max()),
            }

    return metrics


def main():
    parser = argparse.ArgumentParser(description="Week 4 drift simulation")
    parser.add_argument(
        "--input",
        type=str,
        default="data/processed/demand_enriched.parquet",
        help="Input parquet file (clean)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed/demand_enriched_week4.parquet",
        help="Output parquet file (drifted)",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for reproducibility"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Describe what would be corrupted without writing",
    )

    args = parser.parse_args()

    logger.info(f"Loading {args.input}...")
    df = pd.read_parquet(args.input)

    logger.info(f"Loaded: {len(df):,} rows, {len(df.columns)} columns")
    logger.info(
        f"Date range: {df['time_bucket'].min().date()} to {df['time_bucket'].max().date()}"
    )
    logger.info(f"Operational date (baseline): {OPERATIONAL_DATE.date()}")
    logger.info(f"Drift period: {DRIFT_START.date()} onwards\n")

    # Compute baseline metrics BEFORE corruption
    logger.info("Computing baseline metrics...")
    baseline_metrics = compute_baseline_metrics(df, OPERATIONAL_DATE)

    # Store original for comparison
    df_original = df.copy()

    # Apply drifts
    drift_manifest = []

    df, manifest1 = inject_temporal_peak_shift(df, args.seed)
    drift_manifest.append(manifest1)

    df, manifest2 = inject_manhattan_lag_deflation(df, args.seed)
    drift_manifest.append(manifest2)

    df, manifest3 = inject_outer_borough_baseline_scramble(df, args.seed)
    drift_manifest.append(manifest3)

    df, manifest4 = inject_manhattan_weekend_concept_drift(df, args.seed)
    drift_manifest.append(manifest4)

    # Summary
    logger.info(f"\nDrift Summary:")
    logger.info(f"  Total rows: {len(df):,}")
    logger.info(
        f"  Rows with 5-7am boost: {(df['time_bucket'] >= TEMPORAL_DRIFT_START).sum():,}"
    )
    logger.info(
        f"  Manhattan zones: {df[df['borough_id'] == 0]['PULocationID'].nunique()} zones"
    )

    if not args.dry_run:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(args.output, index=False)
        logger.info(f"\n✓ Drifted parquet written to {args.output}")

        # Write baseline metrics for student reference
        baseline_path = output_path.parent / "monitoring_baseline.json"
        with open(baseline_path, "w") as f:
            json.dump(baseline_metrics, f, indent=2, default=str)
        logger.info(f"✓ Baseline metrics written to {baseline_path}")
        logger.info(
            f"  (Students use this to detect drift via PSI/correlation analysis)"
        )

        # Write drift manifest
        manifest_path = output_path.parent / "week4_drift_manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(drift_manifest, f, indent=2, default=str)
        logger.info(f"✓ Drift manifest written to {manifest_path}")
    else:
        logger.info(f"\n[DRY RUN] Would write to {args.output}")
        logger.info(
            f"[DRY RUN] Would write baseline metrics to monitoring_baseline.json"
        )
        logger.info("[DRY RUN] No files written")


if __name__ == "__main__":
    main()
