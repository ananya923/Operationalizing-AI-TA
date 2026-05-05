"""
test_monitoring.py — Week 7: Tests for monitoring.py

Verifies:
  1. record_query accumulates data without errors (Week 6 + Week 7 signatures)
  2. get_metrics returns all 15 expected metric keys
  3. Each Week 7 alert fires when its threshold is crossed
  4. No alerts fire when all metrics are healthy
  5. check_pii detects known PII patterns and ignores clean text
  6. Alerting list in get_metrics matches individual metric statuses
  7. cost_per_query alert: fires above 3× baseline
  8. accuracy_rate alert: fires below 60%
  9. feedback_correction_rate alert: fires above 20%
  10. system_health alert: fires on error_rate > 5% or timeout_rate > 2%
  11. model_cost_latency_corr alert: fires when correlation > 0.75
"""

import importlib
import os
import sys
import json
import tempfile
import time
import pytest

# ── path setup ────────────────────────────────────────────────────────────────
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
WEEK7_APP = os.path.join(REPO_ROOT, "week7", "app")
sys.path.insert(0, WEEK7_APP)


# ── helper: fresh monitoring module ──────────────────────────────────────────
def fresh_mon():
    """
    Return a freshly reloaded monitoring module with empty in-memory state.
    Uses importlib.reload so each test starts from a clean slate.
    """
    import monitoring as mon
    # Reset all module-level state
    mon._query_records.clear()
    mon._consistency_map.clear()
    mon._error_count   = 0
    mon._timeout_count = 0
    return mon


# ── fixtures ──────────────────────────────────────────────────────────────────
_BASE_RECORD = dict(
    role="engineer",
    latency_ms=400.0,
    docs_retrieved=3,
    docs_denied=0,
    answer="The remote work policy allows three days from home.",
    query="What is the remote work policy?",
    had_pii=False,
    cost_usd=0.00027,
    tokens_input=120,
    correct=True,
    is_error=False,
    is_timeout=False,
)


# ── 1. record_query accepts both old and new signatures ──────────────────────

def test_record_query_week6_signature():
    """Week 6 callers pass positional args only (no cost/tokens). Must not raise."""
    mon = fresh_mon()
    mon.record_query(
        role="hr",
        latency_ms=500.0,
        docs_retrieved=2,
        docs_denied=1,
        answer="Benefits include health and dental.",
        query="What benefits do we offer?",
        had_pii=False,
    )
    assert len(mon._query_records) == 1


def test_record_query_week7_signature():
    """Week 7 callers pass all extended kwargs. Must not raise."""
    mon = fresh_mon()
    mon.record_query(**_BASE_RECORD)
    assert len(mon._query_records) == 1


def test_record_query_accumulates_multiple():
    """Multiple calls accumulate correctly in _query_records."""
    mon = fresh_mon()
    for i in range(5):
        mon.record_query(**{**_BASE_RECORD, "latency_ms": 300.0 + i * 10})
    assert len(mon._query_records) == 5


# ── 2. get_metrics returns all 15 metric keys ─────────────────────────────────

EXPECTED_METRICS = {
    # Week 6
    "answer_completeness",
    "access_denials",          # actual key in monitoring.py
    "response_latency",        # actual key in monitoring.py
    "doc_retrieval_coverage",
    "access_denial_rate",
    "audit_log_volume_mb",
    "answer_consistency",
    "pii_exposure_rate",
    # Week 7
    "cost_per_query",
    "latency_percentiles_w7",
    "accuracy_rate",
    "feedback_correction_rate",
    "corpus_freshness",
    "model_cost_latency_corr",
    "system_health",
}

def test_get_metrics_returns_all_keys():
    """get_metrics must include all 15 metric keys."""
    mon = fresh_mon()
    result = mon.get_metrics()
    assert "metrics" in result
    returned_keys = set(result["metrics"].keys())
    missing = EXPECTED_METRICS - returned_keys
    assert not missing, f"Missing metric keys: {missing}"


def test_get_metrics_structure():
    """Each metric entry must have 'value' and 'status' keys."""
    mon = fresh_mon()
    result = mon.get_metrics()
    for name, metric in result["metrics"].items():
        assert "value"  in metric, f"Metric '{name}' missing 'value' key"
        assert "status" in metric, f"Metric '{name}' missing 'status' key"
        assert metric["status"] in ("ok", "alert", "no_data"), (
            f"Metric '{name}' has invalid status '{metric['status']}'"
        )


def test_get_metrics_top_level_keys():
    """Top-level response must have 'status', 'metrics', and 'alerting'."""
    mon = fresh_mon()
    result = mon.get_metrics()
    for key in ("status", "metrics", "alerting"):
        assert key in result, f"Missing top-level key '{key}'"
    assert result["status"] in ("ok", "alert")
    assert isinstance(result["alerting"], list)


# ── 3+4. Alert fires / doesn't fire ─────────────────────────────────────────

def test_no_alerts_when_healthy():
    """With low-cost, fast, accurate records, no Week 7 alerts should fire."""
    mon = fresh_mon()
    for _ in range(10):
        mon.record_query(**_BASE_RECORD)
    result = mon.get_metrics()
    w7_alert_metrics = {
        "cost_per_query", "latency_percentiles_w7", "accuracy_rate",
        "model_cost_latency_corr", "system_health",
    }
    firing = [a for a in result["alerting"] if a in w7_alert_metrics]
    assert not firing, f"Unexpected alerts firing on healthy data: {firing}"


# ── 7. cost_per_query alert ───────────────────────────────────────────────────

def test_cost_per_query_alert_fires():
    """cost_per_query alert fires when average cost exceeds $0.00081."""
    mon = fresh_mon()
    for _ in range(5):
        mon.record_query(**{**_BASE_RECORD, "cost_usd": 0.002})  # 7× baseline
    result = mon.get_metrics()
    assert result["metrics"]["cost_per_query"]["status"] == "alert", (
        f"cost_per_query did not alert. value={result['metrics']['cost_per_query']['value']}"
    )
    assert "cost_per_query" in result["alerting"]


def test_cost_per_query_ok_at_baseline():
    """cost_per_query stays OK at baseline cost."""
    mon = fresh_mon()
    for _ in range(5):
        mon.record_query(**{**_BASE_RECORD, "cost_usd": 0.00027})
    result = mon.get_metrics()
    assert result["metrics"]["cost_per_query"]["status"] == "ok"


# ── 8. accuracy_rate alert ────────────────────────────────────────────────────

def test_accuracy_rate_alert_fires():
    """accuracy_rate alert fires when fewer than 60% of answers are correct."""
    mon = fresh_mon()
    for i in range(10):
        mon.record_query(**{**_BASE_RECORD, "correct": (i < 4)})  # 40% correct
    result = mon.get_metrics()
    assert result["metrics"]["accuracy_rate"]["status"] == "alert", (
        f"accuracy_rate did not alert. value={result['metrics']['accuracy_rate']['value']}"
    )
    assert "accuracy_rate" in result["alerting"]


def test_accuracy_rate_ok_when_above_threshold():
    """accuracy_rate stays OK when ≥80% of answers are correct."""
    mon = fresh_mon()
    for i in range(10):
        mon.record_query(**{**_BASE_RECORD, "correct": (i < 9)})  # 90% correct
    result = mon.get_metrics()
    assert result["metrics"]["accuracy_rate"]["status"] == "ok"


# ── 9. feedback_correction_rate alert ────────────────────────────────────────

def test_feedback_correction_rate_alert_fires():
    """
    feedback_correction_rate alert fires when correction rate > 20%.
    We mock feedback.get_correction_rate to return 0.35 (35%) without needing
    a live feedback log — avoids module-level FEEDBACK_LOG_PATH caching issues.
    """
    from unittest.mock import patch
    mon = fresh_mon()
    # Patch feedback.get_correction_rate inside monitoring's import scope
    with patch("feedback.get_correction_rate", return_value=0.35):
        result = mon.get_metrics()
    assert result["metrics"]["feedback_correction_rate"]["status"] == "alert", (
        f"feedback_correction_rate did not alert. "
        f"value={result['metrics']['feedback_correction_rate']['value']}"
    )
    assert "feedback_correction_rate" in result["alerting"]


# ── 10. system_health alert ───────────────────────────────────────────────────

def test_system_health_alert_on_error_rate():
    """system_health alert fires when error_rate > 5%."""
    mon = fresh_mon()
    # 4 errors out of 10 queries = 40% error rate
    for i in range(10):
        mon.record_query(**{**_BASE_RECORD, "is_error": (i < 4)})
    result = mon.get_metrics()
    health = result["metrics"]["system_health"]
    assert health["status"] == "alert", (
        f"system_health did not alert on error_rate. value={health['value']}"
    )


def test_system_health_alert_on_timeout_rate():
    """system_health alert fires when timeout_rate > 2%."""
    mon = fresh_mon()
    # 3 timeouts out of 10 = 30% timeout rate
    for i in range(10):
        mon.record_query(**{**_BASE_RECORD, "is_timeout": (i < 3)})
    result = mon.get_metrics()
    health = result["metrics"]["system_health"]
    assert health["status"] == "alert", (
        f"system_health did not alert on timeout_rate. value={health['value']}"
    )


def test_system_health_ok_when_clean():
    """system_health stays OK with zero errors and timeouts."""
    mon = fresh_mon()
    for _ in range(10):
        mon.record_query(**_BASE_RECORD)
    result = mon.get_metrics()
    assert result["metrics"]["system_health"]["status"] == "ok"


# ── 11. model_cost_latency_corr alert ────────────────────────────────────────

def test_cost_latency_correlation_alert_fires():
    """model_cost_latency_corr alert fires when Pearson r > 0.75."""
    mon = fresh_mon()
    # Inject records where latency grows linearly with token count → r ≈ 1.0
    for i in range(1, 21):
        mon.record_query(**{
            **_BASE_RECORD,
            "tokens_input": i * 100,
            "latency_ms":   i * 150.0 + 50,   # perfectly correlated
            "cost_usd":     i * 0.00005,
        })
    result = mon.get_metrics()
    corr_metric = result["metrics"]["model_cost_latency_corr"]
    assert corr_metric["status"] == "alert", (
        f"model_cost_latency_corr did not alert. value={corr_metric['value']}"
    )
    assert "model_cost_latency_corr" in result["alerting"]


def test_cost_latency_correlation_ok_when_uncorrelated():
    """model_cost_latency_corr stays OK when token count and latency are unrelated."""
    mon = fresh_mon()
    latencies = [200, 800, 300, 700, 400, 600, 250, 750, 350, 650]
    tokens    = [100, 100, 100, 100, 100, 100, 100, 100, 100, 100]  # constant tokens
    for lat, tok in zip(latencies, tokens):
        mon.record_query(**{
            **_BASE_RECORD,
            "tokens_input": tok,
            "latency_ms":   lat,
        })
    result = mon.get_metrics()
    assert result["metrics"]["model_cost_latency_corr"]["status"] == "ok"


# ── 5. check_pii ──────────────────────────────────────────────────────────────

def test_check_pii_detects_ssn():
    """check_pii should detect SSN patterns."""
    import monitoring as mon
    result = mon.check_pii("Employee SSN is 123-45-6789 per HR records.")
    assert len(result) > 0, "SSN not detected by check_pii"


def test_check_pii_detects_salary():
    """check_pii should detect salary patterns."""
    import monitoring as mon
    result = mon.check_pii("Annual salary: $145,000 as of last review.")
    assert len(result) > 0, "Salary not detected by check_pii"


def test_check_pii_clean_text_returns_empty():
    """check_pii should return empty list for clean text."""
    import monitoring as mon
    result = mon.check_pii("The remote work policy allows three days per week from home.")
    assert result == [], f"check_pii falsely flagged clean text: {result}"


# ── 6. alerting list matches individual metric statuses ──────────────────────

def test_alerting_list_matches_metric_statuses():
    """
    The top-level 'alerting' list must exactly match metrics whose status='alert'.
    """
    mon = fresh_mon()
    # Inject high-cost data to trigger at least one alert
    for _ in range(5):
        mon.record_query(**{**_BASE_RECORD, "cost_usd": 0.005})
    result = mon.get_metrics()

    expected_alerting = {
        name for name, m in result["metrics"].items()
        if m.get("status") == "alert"
    }
    actual_alerting = set(result["alerting"])
    assert expected_alerting == actual_alerting, (
        f"Alerting list mismatch.\n"
        f"  Expected from statuses: {expected_alerting}\n"
        f"  Actual alerting list:   {actual_alerting}"
    )
