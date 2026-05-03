"""
Generate a small synthetic parquet that intentionally contains all 4 known
corruption patterns. Used by CI to exercise the validator and tests without
needing the full 70MB upstream parquet.

Usage:
    python week3/scripts/generate_test_fixture.py [--output PATH] [--seed N]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def generate_fixture(seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    # 10 zones, 30 days at 15-min resolution = 28,800 rows. Small but non-trivial.
    zones = [4, 24, 43, 87, 107, 161, 162, 186, 229, 237]
    start = pd.Timestamp("2025-12-15")
    end = pd.Timestamp("2026-02-01")
    times = pd.date_range(start, end, freq="15min", inclusive="left")
    cutoff = pd.Timestamp("2026-01-16")

    rows = []
    for z in zones:
        for t in times:
            # Realistic-ish base demand by hour
            hour_factor = 0.5 + 0.5 * np.sin(2 * np.pi * (t.hour - 6) / 24)
            base = max(0, int(rng.normal(loc=20 * hour_factor + 5, scale=3)))
            # lag_1week ~ trip_count from 1 week ago (we'll set this to current
            # baseline + noise, which gives a natural ~0.85 correlation).
            rows.append(
                {
                    "PULocationID": z,
                    "time_bucket": t,
                    "trip_count": base,
                    "is_holiday": 1 if (t.month, t.day) in {(1, 1), (12, 25)} else 0,
                    "lag_1week": float(base + rng.normal(0, 2)),
                    "zone_slot_baseline": float(20 * hour_factor + 5),
                }
            )

    df = pd.DataFrame(rows)

    # ── Inject corruption #1: duplicates in a few zones, in the new period
    dup_zones = [4, 43, 87, 107, 229]
    dup_mask = df["PULocationID"].isin(dup_zones) & (df["time_bucket"] >= cutoff)
    duplicates = df[dup_mask].copy()
    df = pd.concat([df, duplicates], ignore_index=True)

    # ── Corruption #2: out-of-range trip_count, scattered in new period
    new_idx = df.index[df["time_bucket"] >= cutoff].tolist()
    bad_idx = rng.choice(new_idx, size=20, replace=False)
    df.loc[bad_idx[:5], "trip_count"] = -10
    df.loc[bad_idx[5:10], "trip_count"] = -1
    df.loc[bad_idx[10:15], "trip_count"] = 9999
    df.loc[bad_idx[15:20], "trip_count"] = 99999

    # ── Corruption #3: is_holiday stuck at 1 for Jan 7 → Jan 21
    stuck_mask = (df["time_bucket"] >= pd.Timestamp("2026-01-07")) & (
        df["time_bucket"] < pd.Timestamp("2026-01-22")
    )
    df.loc[stuck_mask, "is_holiday"] = 1

    # ── Corruption #4: lag_1week cross-contamination
    # Zones 161, 162, 186 get lag_1week values from zone 237 in the new period.
    src_zone = 237
    target_zones = [161, 162, 186]
    new_period = df["time_bucket"] >= cutoff
    src_lookup = df.loc[
        (df["PULocationID"] == src_zone) & new_period, ["time_bucket", "lag_1week"]
    ].rename(columns={"lag_1week": "lag_src"})
    for tz in target_zones:
        merged = df.loc[(df["PULocationID"] == tz) & new_period].merge(
            src_lookup, on="time_bucket", how="left"
        )
        df.loc[(df["PULocationID"] == tz) & new_period, "lag_1week"] = merged[
            "lag_src"
        ].values

    # Match real schema dtypes
    df["PULocationID"] = df["PULocationID"].astype("int64")
    df["trip_count"] = df["trip_count"].astype("int64")
    df["is_holiday"] = df["is_holiday"].astype("int8")
    df["lag_1week"] = df["lag_1week"].astype("float32")
    df["zone_slot_baseline"] = df["zone_slot_baseline"].astype("float64")

    return df


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--output",
        type=Path,
        default=Path("week3/tests/fixtures/demand_corrupted.parquet"),
    )
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    df = generate_fixture(seed=args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False)
    print(f"Wrote {len(df):,} rows × {len(df.columns)} cols → {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
