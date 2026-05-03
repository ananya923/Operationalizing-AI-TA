# Week 3 TA Notes — Data Quality Issues

**Do NOT distribute this file to students.**

## Quick Reference

**Baseline:** Jan 1, 2023 – Jan 15, 2026 (clean training data)
**Corrupted period:** Jan 16 – Feb 1, 2026 (4 data quality issues)

---

## Key Points for TAs

**Student mindset:** They discover issues, don't know they exist.

**Your role:** Guide, don't give answers.

- Don't: "There are duplicates in zones 4, 43, 87..."
- Do: "Have you checked for duplicates? How would you find them?"

**Common questions:**

- "I can't find anything!" → "Try `.describe()` on recent vs historical. Which columns look different?"
- "How do I validate?" → "What checks would catch these issues? Test them on corrupted data."
- "Which approach?" → "What are the tradeoffs? CI blocking vs API warnings?"

**Grading focus:** 4 issues identified with evidence? Tests work? System doesn't crash?

---

## The 4 Corruptions

| # | Name                                                       | Detection                                                        | Difficulty |
| - | ---------------------------------------------------------- | ---------------------------------------------------------------- | ---------- |
| 1 | Duplicate rows (zones 4,43,87,107,152,229, Jan 12-Feb 1)   | `.duplicated().sum()` → ~12K                                  | Easy       |
| 2 | Out-of-range trip_count (850 rows: -10 to 99999)           | `.describe()` → min=-10, max=99999                            | Very easy  |
| 3 | is_holiday stuck at 1 (Jan 7-21, all 4,464 rows)           | `groupby(date)['is_holiday'].mean()` → 100% for window        | Medium     |
| 4 | lag_1week from wrong zone (zones 161,162,186 get from 237) | `corr(lag_1week, trip_count)` per zone → 0.03 instead of 0.85 | Hard       |

---

## Corruption Details

### #1: Duplicates

- **What:** 6 zones have 2× rows for last 21 days
- **Impact:** Demand inflated 2×, lag features poisoned
- **Validation:** `df.drop_duplicates(subset=['PULocationID', 'time_bucket'])`

### #2: Out-of-Range trip_count

- **What:** 850 rows with -10, -5, -1, 9999, 99999 values (seed=42)
- **Impact:** Negative demand (invalid), lag features inherit wrong values
- **Validation:** `assert (df['trip_count'] >= 0).all()`

### #3: is_holiday Stuck

- **What:** Jan 7-21 all marked as holiday (100% rate vs 3% normal)
- **Impact:** Model under-predicts demand (learned holidays = low demand)
- **Validation:** Monitor is_holiday rate by week; flag if >10%

### #4: lag_1week Cross-Contamination

- **What:** Zones 161, 162, 186 have lag_1week values from zone 237
- **Why subtle:** Values are plausible (in-range, non-null), but semantically wrong
- **Impact:** #2 most important feature becomes noise for 3 zones; 40-60% worse MAE
- **Detection:** Per-zone correlation analysis; `corr(lag_1week, trip_count)` drops from 0.85 → 0.03

---

## Grading

| Criterion                                  | Points         |
| ------------------------------------------ | -------------- |
| Finds corruptions 1 & 2                    | 60%            |
| + Finds corruption 3                       | +15%           |
| + Finds corruption 4                       | +15%           |
| Implements validation (CI/startup/runtime) | +10%           |
| Tests that catch these issues              | +10%           |
| **Total**                            | **110%** |

---

## Common Mistakes

| Mistake               | What to Look For               | Feedback                                                                |
| --------------------- | ------------------------------ | ----------------------------------------------------------------------- |
| Only CI validation    | Doesn't catch live degradation | "What about issues after deployment?"                                   |
| Validates but crashes | API errors on bad data         | "Users need graceful degradation. Design fallbacks."                    |
| Misses #4 entirely    | Only found 3/4                 | "Try per-zone correlation: feature should correlate ~0.85 with target." |

---

## TA Workflow

1. **Before class:**

   ```bash
   python week3/ta_scripts/simulate_week3.py \
     --input data/processed/demand_enriched.parquet \
     --output data/processed/demand_enriched_week3.parquet
   ```
2. **Distribute:** Upload parquet to GCS. Tell students: "New data arrived. Replace your parquet." Don't share TA notes or script.
3. **Grading checklist:**

   - [ ] 4 issues identified with zones/dates/counts
   - [ ] Validation code works on corrupted data
   - [ ] API doesn't crash on bad data
   - [ ] Tests in `tests/test_data_quality.py`
4. **Sample feedback:**

   ```
   Issues found: 3/4
   Duplicates: 12K rows, zones 4,43,87,107,152,229
   Out-of-range: 850 rows, values -10 to 99999
   is_holiday: Jan 7-21 all marked as holiday
   lag_1week: Not found

   Missing: Try correlation analysis per zone.
   lag_1week should correlate ~0.85 with trip_count,
   but drops to ~0 for zones 161, 162, 186.
   ```

---

## If Students Ask

**Q: "How do I find what's wrong?"**
A: `.describe()`, `.isnull().sum()`, `.duplicated().sum()`. Compare recent vs historical distributions.

**Q: "My API is crashing."**
A: Wrap data loading in try/except. Log errors. Gracefully degrade (fallbacks instead of raw data).

**Q: "I found 3 issues. How do I know if I'm missing something?"**
A: Compute `Corr(each_feature, trip_count)` per zone. If any correlation drops unexpectedly, investigate.

---

## Optional Extensions

- Add 5th corruption: missing zones completely absent from new data
- Add 6th corruption: timezone shift (data off by ±6 hours)
- Require Prometheus/Grafana dashboard for metrics
- Add SLO: alert if data quality drops below X%
