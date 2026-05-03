# Week 3 — Validation Design Document

## Three layers, three jobs

Data quality is enforced at three points in the system, each with a different responsibility. None of them are sufficient on their own.

| Layer | Where | When it runs | What happens on failure |
|---|---|---|---|
| **Pre-deployment gate** | `week3/scripts/validate_data.py` invoked by `.github/workflows/ci.yml` | On every push and pull request | Exits non-zero. CI fails. Deploy blocked. |
| **Runtime safety net** | `_apply_data_quality_fixes()` in `app/backend/data.py` | At every API process startup | Logs `[DQ] WARNING`, repairs in place, continues. API never crashes. |
| **Regression suite** | `week3/tests/test_data_quality.py` invoked by `pytest` in CI | On every push and pull request | One assertion per known issue family. Failure surfaces a precise message (zones, counts, dates). |

The validator and the test suite share thresholds (e.g., `LAG1W_NEW_CORR_MIN = 0.20`) but live in separate files because they have different audiences. The validator is for operators reading a CI log; the tests are for engineers reading a stack trace.

## Why graceful degradation lives in the loader, not the API handler

A request handler that called `validate_then_serve()` would either reject all traffic when data is bad (unacceptable: real users are mid-ride) or duplicate the entire validation logic per endpoint (unmaintainable). Loading happens once at process startup, so the safety net there is paid for once and applies to every subsequent request.

The loader's repair philosophy is "fail safe, log loud":
- **Duplicates** → drop, keep last. (Last-write-wins is safe; aggregate inflation is not.)
- **OOR `trip_count`** → clip to `[0, 5000]`. (Clipping a sentinel back into range is a smaller error than sentinel propagation.)
- **`is_holiday` stuck** → override to 0. (False negative is safer than false positive: the model under-weights non-holiday signal modestly, vs. systematically under-predicting demand for two weeks.)
- **`lag_1week` broken** → substitute `zone_slot_baseline`. (A deterministic baseline is a worse predictor than a healthy lag, but a much better predictor than corrupted noise.)

Every repair updates `DATA_QUALITY_STATUS`, accessible via `get_data_quality_status()`. This is the surface a `/health/data-quality` endpoint can read; the same dict could be scraped by a Prometheus exporter without changing the loader.

## Why CI runs against a synthetic fixture, not the real parquet

The full upstream parquet is 70 MB and not committed to the repo. CI generates a small (~600 KB) deterministic fixture via `week3/scripts/generate_test_fixture.py` that bakes in all four known corruption patterns. Two consequences:

1. **The CI assertion is inverted.** A passing CI run means the validator and tests successfully *failed* on the corrupted fixture — i.e., the safety net is working. If the validator suddenly passed against the fixture, that would itself be a bug.
2. **The fixture is the regression contract.** It records what corruption patterns we currently defend against. Adding a fifth corruption type means extending both the fixture and the test suite together.

For a production deployment that runs against trusted upstream data, a second CI job would run validator + pytest with the inverted expectation (passing = good). That job is out of scope for this assignment but the path is clear.

## Open issues

- **`lag_1week` detection is over-eager.** Current thresholds flag ~50 zones on the real parquet; the actual corruption affects a smaller subset (estimated 3 zones based on per-zone correlation drops). The natural noise in the new period (16 days) overlaps the corruption signal. Mitigation paths: tighten thresholds further, switch to a per-(zone, hour, dow) cell-level distribution comparison, or accept the over-flag as an acceptable false-positive rate at the warning severity. Pending discussion with course staff.
- **`requests` is imported by `app/backend/data.py` but is missing from `requirements.txt`.** Pre-existing; not in scope for this assignment but worth flagging.