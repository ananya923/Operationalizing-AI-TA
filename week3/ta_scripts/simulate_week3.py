#!/usr/bin/env python3
"""
Week 3 Data Corruption Simulation Script (TA ONLY)

Injects 4 data quality issues into demand_enriched.parquet to simulate
an upstream pipeline failure that occurred on Jan 16, 2026.

Issues injected:
1. Duplicate rows (up to 6 zones, last 21 days)
2. Out-of-range trip_count (~850 rows with negative/extreme values)
3. is_holiday stuck at 1 (2-week window, all rows)
4. lag_1week cross-contamination (3 zones, values from different zone)

Usage:
  python simulate_week3.py --input data/processed/demand_enriched.parquet \
    --output data/processed/demand_enriched_week3.parquet --seed 42

  python simulate_week3.py --input ... --output ... --dry-run
"""

import pandas as pd
import numpy as np
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Corruption parameters
OPERATIONAL_DATE = pd.Timestamp("2026-01-15")
CORRUPTION_START = OPERATIONAL_DATE + timedelta(days=1)  # 2026-01-16

ZONES_DUPLICATE = [4, 43, 87, 107, 152, 229]
DAYS_DUPLICATED = 21

ZONES_LAG_CONTAMINATED = [161, 162, 186]
SOURCE_ZONE_FOR_LAG = 237

HOLIDAY_WINDOW_START = pd.Timestamp("2026-01-07 00:00:00")
HOLIDAY_WINDOW_END = pd.Timestamp("2026-01-21 23:45:00")

OUT_OF_RANGE_COUNT = 850


def validate_zones_exist(df: pd.DataFrame, zones: list, label: str) -> list:
    """Return the subset of `zones` that actually appear in df['PULocationID'].

    Logs a warning for any missing zones. Raises ValueError if none remain.
    """
    present = set(df["PULocationID"].unique())
    valid = [z for z in zones if z in present]
    missing = [z for z in zones if z not in present]

    if missing:
        logger.warning(
            f"⚠ {label}: zones {missing} not found in data — dropping. "
            f"Proceeding with {valid} (n={len(valid)})"
        )

    if not valid:
        raise ValueError(
            f"{label}: none of the requested zones {zones} exist in the data"
        )

    return valid


def inject_duplicates(
    df: pd.DataFrame, zones: list, days: int, seed: int = 42
) -> tuple:
    """Inject duplicate rows for selected zones in recent period."""
    np.random.seed(seed)

    corruption_window_start = df["time_bucket"].max() - timedelta(days=days)

    mask = (df["PULocationID"].isin(zones)) & (
        df["time_bucket"] >= corruption_window_start
    )
    dup_rows = df[mask].copy()

    df_corrupted = pd.concat([df, dup_rows], ignore_index=True)

    logger.info(
        f"✓ Injected {len(dup_rows):,} duplicate rows for zones {zones}, last {days} days"
    )

    return df_corrupted, {
        "type": "duplicates",
        "zones": zones,
        "window_start": corruption_window_start.isoformat(),
        "window_end": df["time_bucket"].max().isoformat(),
        "rows_duplicated": len(dup_rows),
    }


def inject_out_of_range_trip_count(
    df: pd.DataFrame, count: int, seed: int = 42
) -> tuple:
    """Inject out-of-range trip_count values."""
    np.random.seed(seed)

    mask = df["time_bucket"] >= CORRUPTION_START
    corrupted_indices = np.random.choice(df[mask].index, size=count, replace=False)

    # Alternate between negative and extreme positive values
    bad_values = np.random.choice([-1, -5, -10, 9999, 99999], size=count)
    df.loc[corrupted_indices, "trip_count"] = bad_values

    logger.info(f"✓ Injected {count} out-of-range trip_count values")

    return df, {
        "type": "out_of_range_trip_count",
        "count": count,
        "values": list(set(bad_values)),
        "window_start": CORRUPTION_START.isoformat(),
    }


def inject_is_holiday_drift(df: pd.DataFrame) -> tuple:
    """Inject is_holiday flag stuck at 1 for a 2-week window."""
    mask = (df["time_bucket"] >= HOLIDAY_WINDOW_START) & (
        df["time_bucket"] <= HOLIDAY_WINDOW_END
    )
    affected_count = mask.sum()

    df.loc[mask, "is_holiday"] = 1

    logger.info(
        f"✓ Set is_holiday=1 for {affected_count:,} rows in window {HOLIDAY_WINDOW_START} to {HOLIDAY_WINDOW_END}"
    )

    return df, {
        "type": "is_holiday_drift",
        "window_start": HOLIDAY_WINDOW_START.isoformat(),
        "window_end": HOLIDAY_WINDOW_END.isoformat(),
        "rows_affected": int(affected_count),
        "affected_rate": float(affected_count / len(df)),
    }


def inject_lag_cross_contamination(
    df: pd.DataFrame, contaminated_zones: list, source_zone: int, seed: int = 42
) -> tuple:
    """Replace lag_1week for certain zones with values from a different zone.

    Uses a time_bucket -> lag_1week lookup from the source zone via Series.map(),
    which preserves the original df index throughout (unlike merge, which resets it).
    """
    np.random.seed(seed)

    # Build a time_bucket -> lag_1week lookup from the source zone.
    # If the source zone has duplicate time_buckets, set_index would still work
    # but .map() would be ambiguous; we assert uniqueness defensively.
    source_subset = df[df["PULocationID"] == source_zone][["time_bucket", "lag_1week"]]
    if source_subset["time_bucket"].duplicated().any():
        raise ValueError(
            f"Source zone {source_zone} has duplicate time_buckets — "
            f"lag contamination requires a unique lookup key. "
            f"(Did duplicate injection happen to include the source zone?)"
        )
    source_lag_by_bucket = source_subset.set_index("time_bucket")["lag_1week"]

    rows_replaced = 0
    for zone_id in contaminated_zones:
        zone_mask = df["PULocationID"] == zone_id
        # .map() returns a Series whose index matches df.loc[zone_mask].index.
        new_values = df.loc[zone_mask, "time_bucket"].map(source_lag_by_bucket)
        valid = new_values.notna()
        df.loc[new_values[valid].index, "lag_1week"] = new_values[valid].values
        rows_replaced += int(valid.sum())

    logger.info(
        f"✓ Contaminated lag_1week for zones {contaminated_zones} "
        f"with values from zone {source_zone} ({rows_replaced:,} rows)"
    )

    return df, {
        "type": "lag_contamination",
        "contaminated_zones": contaminated_zones,
        "source_zone": source_zone,
        "rows_affected": rows_replaced,
        "description": "lag_1week replaced with values from different zone",
    }


def main():
    parser = argparse.ArgumentParser(description="Week 3 data corruption simulation")
    parser.add_argument(
        "--input",
        type=str,
        default="data/processed/demand_enriched.parquet",
        help="Input parquet file (clean)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed/demand_enriched_week3.parquet",
        help="Output parquet file (corrupted)",
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
    logger.info(f"Corruption window: {CORRUPTION_START.date()} onwards\n")

    # Validate that requested zones exist in the data; drop any that don't.
    zones_dup_valid = validate_zones_exist(df, ZONES_DUPLICATE, "duplicates")
    zones_lag_valid = validate_zones_exist(
        df, ZONES_LAG_CONTAMINATED, "lag_contamination"
    )
    validate_zones_exist(df, [SOURCE_ZONE_FOR_LAG], "lag_contamination_source")

    # Store original for comparison
    df_original = df.copy()

    # Apply corruptions
    corruption_manifest = []

    df, manifest1 = inject_duplicates(df, zones_dup_valid, DAYS_DUPLICATED, args.seed)
    corruption_manifest.append(manifest1)

    df, manifest2 = inject_out_of_range_trip_count(df, OUT_OF_RANGE_COUNT, args.seed)
    corruption_manifest.append(manifest2)

    df, manifest3 = inject_is_holiday_drift(df)
    corruption_manifest.append(manifest3)

    df, manifest4 = inject_lag_cross_contamination(
        df, zones_lag_valid, SOURCE_ZONE_FOR_LAG, args.seed
    )
    corruption_manifest.append(manifest4)

    # Summary
    logger.info(f"\nCorruption Summary:")
    logger.info(f"  Original rows: {len(df_original):,}")
    logger.info(f"  After corruption: {len(df):,}")
    logger.info(f"  Rows added: {len(df) - len(df_original):,}")
    logger.info(
        f"  Triplet_count min/max: {df['trip_count'].min():.0f} / {df['trip_count'].max():.0f}"
    )
    logger.info(
        f"  is_holiday rate (new data): {df[df['time_bucket'] >= CORRUPTION_START]['is_holiday'].mean():.1%}"
    )
    logger.info(f"  is_holiday rate (all data): {df['is_holiday'].mean():.1%}")

    if not args.dry_run:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(args.output, index=False)
        logger.info(f"\n✓ Corrupted parquet written to {args.output}")

        # Also write manifest
        import json

        manifest_path = output_path.parent / "week3_corruption_manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(corruption_manifest, f, indent=2, default=str)
        logger.info(f"✓ Corruption manifest written to {manifest_path}")
    else:
        logger.info(f"\n[DRY RUN] Would write to {args.output}")
        logger.info("[DRY RUN] No files written")


if __name__ == "__main__":
    main()
