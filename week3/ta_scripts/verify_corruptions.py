#!/usr/bin/env python3
"""
Week 3 TA verification script.

Spot-checks that all 4 corruptions injected by simulate_week3.py are
actually present in the corrupted parquet. Run after simulate_week3.py.

Usage (from Operationalizing-AI-TA/week3/):
  python ta_scripts/verify_corruptions.py
"""

import pandas as pd
import numpy as np
from pathlib import Path

CORRUPT_PATH = Path("../data/processed/demand_enriched.parquet")
CLEAN_PATH = Path("../data/processed/demand_enriched.clean.parquet")

CORRUPTION_START = pd.Timestamp("2026-01-16")
HOLIDAY_WINDOW_START = pd.Timestamp("2026-01-07 00:00:00")
HOLIDAY_WINDOW_END = pd.Timestamp("2026-01-21 23:45:00")

# Zone 152 was requested but doesn't exist in the dataset; the simulator
# drops it with a warning. We verify against the actual expected set.
ZONES_DUPLICATE_EXPECTED = [4, 43, 87, 107, 229]
ZONES_LAG_CONTAMINATED = [161, 162, 186]
SOURCE_ZONE_FOR_LAG = 237


def main():
    print("Loading parquets...")
    df = pd.read_parquet(CORRUPT_PATH)
    clean = pd.read_parquet(CLEAN_PATH)
    new = df[df["time_bucket"] >= CORRUPTION_START]
    print(
        f"Corrupt: {len(df):,} rows | Clean: {len(clean):,} rows | Diff: {len(df) - len(clean):,}\n"
    )

    print("=" * 70)
    print("ISSUE 1: Duplicate rows")
    print("=" * 70)
    dup_mask = df.duplicated(subset=["PULocationID", "time_bucket"], keep=False)
    n_dups = dup_mask.sum()
    affected_zones = sorted(df[dup_mask]["PULocationID"].unique().tolist())
    print(
        f"  Total rows involved in (PULocationID, time_bucket) duplication: {n_dups:,}"
    )
    print(f"  Distinct zones with duplicates: {affected_zones}")
    print(f"  Expected zones: {ZONES_DUPLICATE_EXPECTED}")
    print(
        f"  PASS"
        if set(affected_zones) == set(ZONES_DUPLICATE_EXPECTED)
        else "  FAIL — zone mismatch"
    )

    print("\n" + "=" * 70)
    print("ISSUE 2: Out-of-range trip_count")
    print("=" * 70)
    n_negative = (new["trip_count"] < 0).sum()
    n_extreme = (new["trip_count"] > 5000).sum()
    print(f"  New-data rows with trip_count < 0:    {n_negative}")
    print(f"  New-data rows with trip_count > 5000: {n_extreme}")
    print(f"  Total out-of-range:                   {n_negative + n_extreme}")
    print(f"  Expected: ~850 total")
    print(f"  trip_count min in new data: {new['trip_count'].min()}")
    print(f"  trip_count max in new data: {new['trip_count'].max()}")
    print(f"  PASS" if 800 <= (n_negative + n_extreme) <= 900 else "  FAIL — count off")

    print("\n" + "=" * 70)
    print("ISSUE 3: is_holiday drift")
    print("=" * 70)
    in_window = df[
        (df["time_bucket"] >= HOLIDAY_WINDOW_START)
        & (df["time_bucket"] <= HOLIDAY_WINDOW_END)
    ]
    rate_in_window = in_window["is_holiday"].mean()
    rate_overall = df["is_holiday"].mean()
    rate_clean_window = clean[
        (clean["time_bucket"] >= HOLIDAY_WINDOW_START)
        & (clean["time_bucket"] <= HOLIDAY_WINDOW_END)
    ]["is_holiday"].mean()
    print(f"  is_holiday rate, Jan 7–21 window (corrupted): {rate_in_window:.1%}")
    print(f"  is_holiday rate, Jan 7–21 window (clean):     {rate_clean_window:.1%}")
    print(f"  is_holiday rate, all data (corrupted):        {rate_overall:.1%}")
    print(f"  PASS" if rate_in_window == 1.0 else "  FAIL — drift not 100%")

    print("\n" + "=" * 70)
    print("ISSUE 4: lag_1week cross-contamination")
    print("=" * 70)
    # Source zone 237 should have unique time_buckets (it's not in the dup list);
    # drop_duplicates defensively just in case.
    src = (
        df[df["PULocationID"] == SOURCE_ZONE_FOR_LAG]
        .drop_duplicates(subset="time_bucket")
        .set_index("time_bucket")["lag_1week"]
    )

    all_pass = True
    for zone in ZONES_LAG_CONTAMINATED:
        z = (
            df[df["PULocationID"] == zone]
            .drop_duplicates(subset="time_bucket")
            .set_index("time_bucket")["lag_1week"]
        )
        aligned = pd.concat([z.rename("zone"), src.rename("src")], axis=1).dropna()
        match_rate = (aligned["zone"] == aligned["src"]).mean()
        # Expect near-100% match for properly contaminated zones.
        ok = match_rate >= 0.95
        all_pass &= ok
        print(
            f"  Zone {zone} lag_1week matches zone {SOURCE_ZONE_FOR_LAG}: {match_rate:.1%}  {'PASS' if ok else 'FAIL'}"
        )

    # Sanity: a non-contaminated zone should NOT match.
    control_zone = next(
        z
        for z in df["PULocationID"].unique()
        if z not in ZONES_LAG_CONTAMINATED + [SOURCE_ZONE_FOR_LAG]
    )
    z = (
        df[df["PULocationID"] == control_zone]
        .drop_duplicates(subset="time_bucket")
        .set_index("time_bucket")["lag_1week"]
    )
    aligned = pd.concat([z.rename("zone"), src.rename("src")], axis=1).dropna()
    control_match = (aligned["zone"] == aligned["src"]).mean()
    control_ok = control_match < 0.20
    all_pass &= control_ok
    print(
        f"  Control zone {control_zone} matches zone {SOURCE_ZONE_FOR_LAG}: {control_match:.1%} (should be low) {'PASS' if control_ok else 'FAIL'}"
    )
    print(f"  ISSUE 4 OVERALL: {'PASS' if all_pass else 'FAIL'}")

    print("\nDone.")


if __name__ == "__main__":
    main()
