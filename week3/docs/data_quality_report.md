# Week 3 — Data Quality Report

**Dataset:** `data/processed/demand_enriched.parquet` (6,330,245 rows × 32 columns)  
**Baseline period:** 2023-01-01 → 2026-01-15 (clean)  
**Suspect period:** 2026-01-16 → 2026-02-28 (newly arrived)  
**Investigation notebook:** `week3/notebooks/01_data_quality_investigation.ipynb`

## Summary

Four data quality issues were identified by comparing the new period against the historical baseline. Three are hard correctness violations; one is a semantic feature corruption.

| # | Issue | Severity | Rows affected | Detection method |
|---|---|---|---|---|
| 1 | Duplicate `(zone, time_bucket)` rows | Error | 20,170 (10,085 dup pairs) | `df.duplicated(subset=['PULocationID','time_bucket'])` |
| 2 | Out-of-range `trip_count` | Error | 850 | range check `[0, 5000]` |
| 3 | `is_holiday` stuck at 1 | Error | 82,080 | longest run of 100%-holiday days |
| 4 | `lag_1week` cross-contamination | Warning | ~3 zones | per-zone correlation drop, hist vs new |

---

## Issue 1 — Duplicate rows on natural key

**What.** 10,085 `(PULocationID, time_bucket)` pairs appear twice in the dataset, producing 20,170 total duplicate rows.

**Where.** Five zones: **{4, 43, 87, 107, 229}**, each with exactly 2,017 duplicate timestamps. Period: 2026-02-07 → 2026-02-28 (last 21 days of the new window).

**Root cause hypothesis.** A re-ingestion of the same upstream batch without de-duplication on the natural key. Each affected zone's last 3 weeks were appended twice.

**Impact.**
- Aggregate demand is inflated by 2× for the affected zones over the duplicated period, distorting the `_profile` lookup that powers `/heatmap` and `/kpis`.
- Lag features computed downstream pull from the duplicated rows, propagating noise to forecasts.
- Without intervention, the LightGBM model would over-train on these zones.

**Fix at the loader.** `df.drop_duplicates(subset=['PULocationID','time_bucket'], keep='last')`. Logged as `[DQ] WARNING dropped 10085 duplicate rows`.

---

## Issue 2 — Out-of-range `trip_count`

**What.** 850 rows contain `trip_count` values from the set `{-10, -5, -1, 9999, 99999}`, all of which violate the natural domain (non-negative integer trip counts; per-zone-per-15-min realistic ceiling well under 1000).

**Where.** Distributed across multiple zones in the new period. Min observed = -10, max = 99999.

**Root cause hypothesis.** Sentinel values from upstream signaling errors (`-1` = unavailable, `9999`/`99999` = overflow) that were not stripped before merge.

**Impact.**
- Negative counts make no domain sense and break any sum/mean aggregation.
- Outliers like 99999 dominate group statistics; a single such row inflates an hourly mean by ~250×.
- Lag features inherit these values, contaminating future predictions for the same zone.

**Fix at the loader.** Clip to `[0, 5000]`. The upper bound is generous (real-world max ≪ 5000) so legitimate spikes survive. Logged with the count clipped.

---

## Issue 3 — `is_holiday` stuck at 1

**What.** Every row in the window **2026-01-07 → 2026-01-21** (15 consecutive days) is flagged `is_holiday = 1`. In the baseline period, real US federal holidays produce single-day or short-run flags totaling ~3% of days; runs longer than 3 days do not occur.

**Where.** All 57 zones, spanning 15 days × 96 slots × 57 zones = 82,080 rows.

**Root cause hypothesis.** A holiday-tagging job stuck in an "on" state, or a backfill that applied a default holiday flag without checking the calendar.

**Impact.**
- The model learned that holidays correlate with depressed demand. Treating ordinary January weekdays as holidays causes the model to systematically under-predict demand for those 15 days.
- Operator-facing KPIs (e.g., "Top zone today") will pull from the holiday-specific profile incorrectly when this date is queried.

**Fix at the loader.** Detect runs of `daily is_holiday rate == 1.0` longer than 3 days and override `is_holiday = 0` for those rows. Falling back to "not a holiday" is safer than the corrupted "always holiday" value.

---

## Issue 4 — `lag_1week` cross-contamination

**What.** For a small set of zones, `lag_1week` values in the new period appear semantically wrong: they are in-range and non-null (so all schema checks pass), but per-zone correlation between `lag_1week` and `trip_count` collapses from ~0.85 (historical) to ~0.0 (new). Pattern is consistent with the column being populated from a different zone's series.

**Where.** Detection via `corr(lag_1week, trip_count)` per zone, comparing historical and new windows. Zones flagged: **see `data_quality_report.json` for the current run.** *Note: detection threshold tuning is ongoing — see design document and the validator's `LAG1W_NEW_CORR_MIN` and `LAG1W_DROP_MIN` config.*

**Root cause hypothesis.** A merge join on the wrong key (e.g., joining on row position instead of zone × timestamp), causing a few zones to receive lag values from another zone's column.

**Impact.**
- `lag_1week` is the second-most important feature in the production LightGBM model. For affected zones, this feature becomes noise.
- Forecast MAE is expected to degrade 40–60% on those zones based on the feature's training-time importance.
- Most subtle of the four issues: schema, range, and null checks all pass.

**Fix at the loader.** For affected zones, replace `lag_1week` with `zone_slot_baseline` (the long-run average for that zone × time-of-day bucket). The baseline is a worse predictor than a clean lag, but a far better predictor than corrupted noise.

---

## Validation evidence

The same checks live in three places, layered for defense in depth:

1. **CI gate** — `week3/scripts/validate_data.py` returns exit code 1 on any error-severity issue. Run automatically in `.github/workflows/ci.yml`.
2. **Runtime safety net** — `app/backend/data.py` applies the same fixes at load time and never crashes. Status exposed via `get_data_quality_status()`.
3. **Regression suite** — `week3/tests/test_data_quality.py` (16 tests) verifies the parquet against schema, ranges, uniqueness, holiday distribution, and per-zone correlation.

Live report: `week3/data_quality_report.json`.