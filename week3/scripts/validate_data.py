"""
Data quality validator for demand_enriched.parquet.

Usage:
    python week3/scripts/validate_data.py <parquet_path> [--output report.json] [--strict]

Exit codes:
    0 = all checks passed (warnings allowed)
    1 = at least one ERROR-severity check failed
    2 = invalid arguments / file not found
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

EXPECTED_SCHEMA: dict[str, str] = {
    # name -> dtype family ('int', 'float', 'datetime')
    "PULocationID": "int",
    "time_bucket": "datetime",
    "trip_count": "int",
    "is_holiday": "int",
    "lag_1week": "float",
}
NATURAL_KEY = ["PULocationID", "time_bucket"]
TRIP_COUNT_RANGE = (0, 5000)
HOLIDAY_RUN_MAX = 3  # >3 consecutive fully-flagged days = suspicious
HOLIDAY_RATE_MAX = 0.10  # global rate ceiling
LAG1W_CORR_MIN = 0.50  # per-zone correlation floor (healthy ~0.85)
CUTOFF = pd.Timestamp("2026-01-16")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
)
log = logging.getLogger("validate_data")


# --------------------------------------------------------------------------- #
# Result types
# --------------------------------------------------------------------------- #


@dataclass
class CheckResult:
    name: str
    severity: str  # "error" | "warning" | "info"
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# Individual checks
# --------------------------------------------------------------------------- #


def check_schema(df: pd.DataFrame) -> CheckResult:
    missing = [c for c in EXPECTED_SCHEMA if c not in df.columns]
    if missing:
        return CheckResult(
            "schema",
            "error",
            False,
            f"Missing required columns: {missing}",
            {"missing": missing},
        )

    bad_dtypes = {}
    for col, family in EXPECTED_SCHEMA.items():
        dt = df[col].dtype
        ok = (
            (family == "int" and pd.api.types.is_integer_dtype(dt))
            or (family == "float" and pd.api.types.is_float_dtype(dt))
            or (family == "datetime" and pd.api.types.is_datetime64_any_dtype(dt))
        )
        if not ok:
            bad_dtypes[col] = {"expected_family": family, "actual": str(dt)}

    if bad_dtypes:
        return CheckResult(
            "schema",
            "error",
            False,
            f"Wrong dtypes for {list(bad_dtypes)}",
            {"bad_dtypes": bad_dtypes},
        )
    return CheckResult(
        "schema", "error", True, "All required columns present with valid dtypes."
    )


def check_duplicates(df: pd.DataFrame) -> CheckResult:
    mask = df.duplicated(subset=NATURAL_KEY, keep=False)
    n = int(mask.sum())
    if n == 0:
        return CheckResult(
            "duplicates", "error", True, "No duplicate (zone, time_bucket) rows."
        )

    dups = df.loc[mask, NATURAL_KEY + ["trip_count"]]
    zones = sorted(dups["PULocationID"].unique().tolist())
    return CheckResult(
        "duplicates",
        "error",
        False,
        f"Found {n:,} duplicate rows across {len(zones)} zones.",
        {
            "row_count": n,
            "affected_zones": zones,
            "period_start": str(dups["time_bucket"].min()),
            "period_end": str(dups["time_bucket"].max()),
        },
    )


def check_trip_count_range(df: pd.DataFrame) -> CheckResult:
    lo, hi = TRIP_COUNT_RANGE
    bad = df[(df["trip_count"] < lo) | (df["trip_count"] > hi)]
    n = len(bad)
    if n == 0:
        return CheckResult(
            "trip_count_range", "error", True, f"trip_count is within [{lo}, {hi}]."
        )
    return CheckResult(
        "trip_count_range",
        "error",
        False,
        f"{n:,} rows have trip_count outside [{lo}, {hi}].",
        {
            "row_count": n,
            "min_observed": int(bad["trip_count"].min()),
            "max_observed": int(bad["trip_count"].max()),
            "bad_values": sorted(bad["trip_count"].unique().tolist())[:20],
        },
    )


LAG1W_CORR_MIN = 0.50  # new-period correlation floor
LAG1W_DROP_MIN = 0.30  # required drop from historical to flag


def check_is_holiday_runs(df: pd.DataFrame) -> CheckResult:
    daily = df.groupby(df["time_bucket"].dt.normalize())["is_holiday"].mean()
    overall_rate = float(df["is_holiday"].mean())

    stuck = daily[daily == 1.0].sort_index()
    if stuck.empty:
        return CheckResult(
            "is_holiday_distribution",
            "error",
            True,
            f"is_holiday distribution healthy (overall rate {overall_rate:.3f}).",
        )

    # Find the longest consecutive run of fully-flagged days
    dates = pd.Series(stuck.index)
    gaps = dates.diff().dt.days.fillna(1)
    run_id = (gaps > 1).cumsum()
    runs = (
        pd.DataFrame({"date": dates, "run": run_id})
        .groupby("run")
        .agg(start=("date", "min"), end=("date", "max"), n_days=("date", "count"))
        .sort_values("n_days", ascending=False)
    )
    longest = runs.iloc[0]
    longest_n = int(longest["n_days"])

    failed = longest_n > HOLIDAY_RUN_MAX or overall_rate > HOLIDAY_RATE_MAX
    return CheckResult(
        "is_holiday_distribution",
        "error",
        not failed,
        (
            f"Longest stuck-holiday run = {longest_n} days "
            f"(threshold {HOLIDAY_RUN_MAX}); overall rate {overall_rate:.3f}."
        ),
        {
            "overall_rate": overall_rate,
            "longest_run_days": longest_n,
            "longest_run_start": str(longest["start"].date()),
            "longest_run_end": str(longest["end"].date()),
            "max_allowed_run_days": HOLIDAY_RUN_MAX,
            "max_allowed_rate": HOLIDAY_RATE_MAX,
        },
    )


def check_lag1week_correlation(df: pd.DataFrame) -> CheckResult:
    """Compare per-zone lag_1week ↔ trip_count correlation: new period vs historical."""
    hist = df[(df["time_bucket"] < CUTOFF) & df["lag_1week"].notna()]
    new = df[(df["time_bucket"] >= CUTOFF) & df["lag_1week"].notna()]

    if hist.empty or new.empty:
        return CheckResult(
            "lag1week_correlation",
            "warning",
            True,
            "Insufficient data to compare new vs historical correlation.",
        )

    def per_zone_corr(frame: pd.DataFrame) -> pd.Series:
        return (
            frame.groupby("PULocationID")
            .apply(lambda g: g["lag_1week"].corr(g["trip_count"]))
            .dropna()
        )

    hist_corr = per_zone_corr(hist)
    new_corr = per_zone_corr(new)

    common = hist_corr.index.intersection(new_corr.index)
    drops = hist_corr.loc[common] - new_corr.loc[common]

    # Broken = correlation collapsed in the new period
    broken_mask = (new_corr.loc[common] < LAG1W_CORR_MIN) & (drops >= LAG1W_DROP_MIN)
    broken_zones = common[broken_mask]

    if broken_zones.empty:
        return CheckResult(
            "lag1week_correlation",
            "warning",
            True,
            f"No zones show degraded lag_1week correlation "
            f"(median hist={hist_corr.median():.2f}, median new={new_corr.median():.2f}).",
        )

    return CheckResult(
        "lag1week_correlation",
        "warning",
        False,
        f"{len(broken_zones)} zones show collapsed lag_1week correlation in new data.",
        {
            "new_corr_threshold": LAG1W_CORR_MIN,
            "min_required_drop": LAG1W_DROP_MIN,
            "median_hist_corr": float(hist_corr.median()),
            "median_new_corr": float(new_corr.median()),
            "broken_zones": {
                int(z): {
                    "historical": round(float(hist_corr.loc[z]), 3),
                    "new": round(float(new_corr.loc[z]), 3),
                    "drop": round(float(drops.loc[z]), 3),
                }
                for z in broken_zones
            },
        },
    )


CHECKS = [
    check_schema,
    check_duplicates,
    check_trip_count_range,
    check_is_holiday_runs,
    check_lag1week_correlation,
]

# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #


def run_validation(parquet_path: Path) -> tuple[list[CheckResult], dict[str, Any]]:
    log.info("Loading %s", parquet_path)
    df = pd.read_parquet(parquet_path)
    log.info("Loaded %s rows × %s columns", f"{len(df):,}", len(df.columns))

    results: list[CheckResult] = []
    for fn in CHECKS:
        try:
            r = fn(df)
        except Exception as e:  # never let a check kill the runner
            r = CheckResult(fn.__name__, "error", False, f"Check raised exception: {e}")
        results.append(r)
        status = "PASS" if r.passed else r.severity.upper()
        log.info("[%s] %s — %s", status, r.name, r.message)

    summary = {
        "total": len(results),
        "passed": sum(r.passed for r in results),
        "errors": sum(1 for r in results if not r.passed and r.severity == "error"),
        "warnings": sum(1 for r in results if not r.passed and r.severity == "warning"),
    }
    return results, summary


def main() -> int:
    p = argparse.ArgumentParser(
        description="Validate demand_enriched parquet for data quality issues."
    )
    p.add_argument("parquet", type=Path, help="Path to parquet file to validate.")
    p.add_argument(
        "--output", type=Path, default=None, help="Write JSON report to this path."
    )
    p.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if any warnings (not only errors) are present.",
    )
    args = p.parse_args()

    if not args.parquet.exists():
        log.error("File not found: %s", args.parquet)
        return 2

    results, summary = run_validation(args.parquet)

    report = {
        "parquet": str(args.parquet),
        "summary": summary,
        "checks": [asdict(r) for r in results],
    }
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, default=str))
        log.info("Wrote report → %s", args.output)
    else:
        print(json.dumps(report, indent=2, default=str))

    if summary["errors"] > 0:
        log.error("Validation FAILED with %d error(s).", summary["errors"])
        return 1
    if args.strict and summary["warnings"] > 0:
        log.error(
            "Validation FAILED in --strict mode with %d warning(s).",
            summary["warnings"],
        )
        return 1
    log.info("Validation PASSED (%d warning(s)).", summary["warnings"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
