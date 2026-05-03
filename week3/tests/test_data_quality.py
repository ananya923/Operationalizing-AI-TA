"""
Pytest suite for data quality regressions.

Each of the 4 documented corruptions has at least one test that would catch
it in future incoming data. Tests run against the parquet at:
    data/processed/demand_enriched.parquet

Run from repo root:
    pytest week3/tests/ -v
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import os

# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

REPO_ROOT = Path(__file__).resolve().parents[2]
# DATA_PATH = REPO_ROOT / "data" / "processed" / "demand_enriched.parquet"
DATA_PATH = Path(
    os.environ.get(
        "WEEK3_DATA_PATH",
        str(REPO_ROOT / "data" / "processed" / "demand_enriched.parquet"),
    )
)
CUTOFF = pd.Timestamp("2026-01-16")

REQUIRED_COLUMNS = {
    "PULocationID": "int",
    "time_bucket": "datetime",
    "trip_count": "int",
    "is_holiday": "int",
    "lag_1week": "float",
    "zone_slot_baseline": "float",
}

TRIP_COUNT_RANGE = (0, 5000)
HOLIDAY_RATE_MAX = 0.10
HOLIDAY_RUN_MAX = 3  # max consecutive 100%-holiday days
LAG1W_NEW_MIN = 0.20  # any zone below this in the new period is suspect
LAG1W_DROP_MAX = 0.50  # max allowed historical→new correlation drop


@pytest.fixture(scope="module")
def df() -> pd.DataFrame:
    """Load the parquet once for all tests."""
    if not DATA_PATH.exists():
        pytest.skip(f"Data file missing: {DATA_PATH}")
    frame = pd.read_parquet(DATA_PATH)
    frame["time_bucket"] = pd.to_datetime(frame["time_bucket"])
    return frame


@pytest.fixture(scope="module")
def df_hist(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["time_bucket"] < CUTOFF]


@pytest.fixture(scope="module")
def df_new(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["time_bucket"] >= CUTOFF]


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #


class TestSchema:
    def test_required_columns_present(self, df):
        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        assert not missing, f"Missing required columns: {missing}"

    @pytest.mark.parametrize("col,family", REQUIRED_COLUMNS.items())
    def test_column_dtype(self, df, col, family):
        dt = df[col].dtype
        if family == "int":
            assert pd.api.types.is_integer_dtype(dt), f"{col} expected int, got {dt}"
        elif family == "float":
            assert pd.api.types.is_float_dtype(dt), f"{col} expected float, got {dt}"
        elif family == "datetime":
            assert pd.api.types.is_datetime64_any_dtype(
                dt
            ), f"{col} expected datetime, got {dt}"


# --------------------------------------------------------------------------- #
# Issue #1 — Duplicates
# --------------------------------------------------------------------------- #


class TestDuplicates:
    """Catches corruption #1: duplicate rows on (PULocationID, time_bucket)."""

    def test_no_duplicate_natural_key(self, df):
        dup_count = int(df.duplicated(subset=["PULocationID", "time_bucket"]).sum())
        assert dup_count == 0, (
            f"Found {dup_count:,} duplicate (zone, time_bucket) rows. "
            "Each (zone, time_bucket) pair must be unique."
        )

    def test_unique_key_per_zone(self, df):
        """Per-zone version: every zone's time series must be strictly unique."""
        offenders = df.groupby("PULocationID").apply(
            lambda g: g["time_bucket"].duplicated().sum(), include_groups=False
        )
        bad_zones = offenders[offenders > 0]
        assert (
            bad_zones.empty
        ), f"Zones with duplicate timestamps: {bad_zones.to_dict()}"


# --------------------------------------------------------------------------- #
# Issue #2 — Out-of-range trip_count
# --------------------------------------------------------------------------- #


class TestTripCountRange:
    """Catches corruption #2: negative and absurdly large trip_count values."""

    def test_trip_count_non_negative(self, df):
        n_neg = int((df["trip_count"] < 0).sum())
        assert n_neg == 0, f"{n_neg} rows have negative trip_count."

    def test_trip_count_upper_bound(self, df):
        lo, hi = TRIP_COUNT_RANGE
        n_high = int((df["trip_count"] > hi).sum())
        assert n_high == 0, f"{n_high} rows exceed trip_count upper bound of {hi}."

    def test_trip_count_full_range(self, df):
        lo, hi = TRIP_COUNT_RANGE
        bad = df[(df["trip_count"] < lo) | (df["trip_count"] > hi)]
        assert bad.empty, (
            f"{len(bad)} rows outside [{lo}, {hi}]; "
            f"min={df['trip_count'].min()}, max={df['trip_count'].max()}."
        )


# --------------------------------------------------------------------------- #
# Issue #3 — is_holiday distribution
# --------------------------------------------------------------------------- #


class TestHolidayDistribution:
    """Catches corruption #3: is_holiday stuck at 1 for an extended window."""

    def test_overall_holiday_rate(self, df):
        rate = float(df["is_holiday"].mean())
        assert rate <= HOLIDAY_RATE_MAX, (
            f"Overall is_holiday rate is {rate:.3f}, exceeds {HOLIDAY_RATE_MAX}. "
            "Real holidays occur ~3% of days; a rate this high suggests stuck flags."
        )

    def test_no_long_holiday_run(self, df):
        daily = df.groupby(df["time_bucket"].dt.normalize())["is_holiday"].mean()
        stuck = daily[daily == 1.0].sort_index()
        if stuck.empty:
            return  # trivially passes
        dates = pd.Series(stuck.index)
        run_id = (dates.diff().dt.days.fillna(1) > 1).cumsum()
        runs = (
            pd.DataFrame({"date": dates, "run": run_id})
            .groupby("run")
            .agg(start=("date", "min"), end=("date", "max"), n=("date", "count"))
        )
        longest = int(runs["n"].max())
        worst = runs.loc[runs["n"].idxmax()]
        assert longest <= HOLIDAY_RUN_MAX, (
            f"Found a {longest}-day run of fully-flagged holidays "
            f"({worst['start'].date()} → {worst['end'].date()}); "
            f"max allowed = {HOLIDAY_RUN_MAX} days."
        )


# --------------------------------------------------------------------------- #
# Issue #4 — lag_1week feature integrity
# --------------------------------------------------------------------------- #


class TestLag1WeekIntegrity:
    """Catches corruption #4: lag_1week values cross-contaminated from another zone."""

    def test_lag_1week_correlates_with_target(self, df_hist):
        """In healthy data lag_1week and trip_count correlate ~0.85 globally."""
        sub = df_hist[df_hist["lag_1week"].notna()]
        global_corr = sub["lag_1week"].corr(sub["trip_count"])
        assert global_corr >= 0.50, (
            f"Global lag_1week ↔ trip_count correlation is {global_corr:.3f}, "
            "expected ≥ 0.50 in healthy historical data."
        )

    def test_per_zone_correlation_does_not_collapse(self, df_hist, df_new):
        """No zone's per-zone correlation should collapse from historical → new."""

        def per_zone(frame):
            sub = frame[frame["lag_1week"].notna()]
            return (
                sub.groupby("PULocationID")
                .apply(
                    lambda g: g["lag_1week"].corr(g["trip_count"]), include_groups=False
                )
                .dropna()
            )

        hc = per_zone(df_hist)
        nc = per_zone(df_new)
        common = hc.index.intersection(nc.index)
        drops = hc.loc[common] - nc.loc[common]

        # A zone is "collapsed" only if BOTH conditions hold
        broken_mask = (nc.loc[common] < LAG1W_NEW_MIN) & (drops >= LAG1W_DROP_MAX)
        broken_zones = common[broken_mask].tolist()

        assert not broken_zones, (
            f"lag_1week correlation collapsed in {len(broken_zones)} zones: "
            f"{broken_zones}. "
            f"(threshold: new < {LAG1W_NEW_MIN} AND drop ≥ {LAG1W_DROP_MAX})"
        )
