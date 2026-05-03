# Week 3 — Data Quality Validation & Graceful Degradation

## Assignment

Your deployed API runs against data from an upstream pipeline. New data arrives January 16 – February 1, 2026. This data contains quality issues. Your tasks:

1. Identify the data quality issues
2. Design and implement a validation framework (decide where/when validation runs)
3. Ensure the system continues to operate when data is bad (graceful degradation)
4. Write tests that catch similar issues in future data

**Deliverables:**
- Data quality report (2-3 pages): issues identified, root causes, impact
- Validation code: `scripts/validate_data.py`, updated `app/backend/data.py`, updated `.github/workflows/ci.yml`
- Tests: `tests/test_data_quality.py`
- Design document (1 page): validation placement decisions

---

## Part 1: Identify the Issues

Load the corrupted parquet and compare to the known-good historical baseline (Jan 1, 2023 – Jan 15, 2026):

```python
import pandas as pd
import numpy as np

# Load the corrupted data
df_corrupted = pd.read_parquet('data/processed/demand_enriched.parquet')

# Filter to only the NEW data (after Jan 15)
df_new = df_corrupted[df_corrupted['time_bucket'] >= '2026-01-16']
df_historical = df_corrupted[df_corrupted['time_bucket'] < '2026-01-16']

# Explore
print("Historical data shape:", df_historical.shape)
print("New data shape:", df_new.shape)
print("\nNew data columns and dtypes:")
print(df_new.dtypes)
print("\nNew data describe():")
print(df_new.describe())
print("\nNull counts in new data:")
print(df_new.isnull().sum())
```

Look for:
- Null rates by column
- Out-of-range values (e.g., trip_count < 0)
- Duplicate rows (by PULocationID + time_bucket)
- Distribution shifts (especially is_holiday rate, lag feature correlations)
- Broken feature-target correlations per zone

Document each issue: what it is, where it appears, and its impact.

---

## Part 2: Build a Data Validation Framework

Decide where validation runs:
- **CI pipeline:** Block deployment if issues found
- **API startup:** Log warnings but continue
- **Runtime:** Expose metrics endpoint

Implement using Great Expectations (pip install great_expectations) or custom functions. Your validation should catch schema errors, out-of-range values, duplicates, and distribution shifts.

---

## Part 3: Graceful Degradation

When data is invalid, the system must continue to operate. Design fallbacks for:
- Missing lag features
- Out-of-range values
- Duplicates
- Any other issues you identify

The API should not crash; log issues and degrade gracefully.

---

## Part 4: Write Tests

Automated tests must catch the 4 data quality issues in future data. Tests should verify:
- Schema and data types
- Range constraints
- Uniqueness
- Distribution properties (e.g., is_holiday rate)
- Feature-target correlations per zone

---

## Grading

| Criterion | Weight |
|-----------|--------|
| Issues identified (4 with specific details: zones, periods, counts) | 30% |
| Validation framework (catches issues at CI, API, or runtime) | 25% |
| Graceful degradation (system continues to serve, logs warnings) | 20% |
| Tests (catch the issues in future data) | 15% |
| Documentation (clear design decisions) | 10% |
