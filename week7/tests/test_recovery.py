"""
test_recovery.py — Week 7: Tests for auto_recovery.py

Verifies:
  1. No recovery actions trigger when all metrics are healthy
  2. corpus_rearchival triggers on cost spike
  3. corpus_rearchival triggers on accuracy drop
  4. corpus_rearchival triggers on feedback spike
  5. corpus_rearchival triggers on high cost-latency correlation
  6. Stricter threshold (0.6) used when both cost AND accuracy alert simultaneously
  7. fallback_mode triggers on latency p95 spike
  8. fallback_mode triggers on high timeout rate
  9. fallback_mode disabled when latency recovers (existing config removed)
  10. rate_limiting triggers on high error rate
  11. notify fires for corpus_freshness (always requires human review)
  12. fallback_config.json has correct schema (top_k, expires_at, fallback_mode)
  13. rate_limit_config.json has correct schema
  14. All actions are idempotent (re-running while active is a no-op)
  15. Dry-run mode prints actions without writing files
"""

import json
import os
import sys
import tempfile
import time
import pytest

# ── path setup ────────────────────────────────────────────────────────────────
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
WEEK7_APP = os.path.join(REPO_ROOT, "week7", "app")
WEEK7_SCRIPTS = os.path.join(REPO_ROOT, "week7", "scripts")
sys.path.insert(0, WEEK7_APP)
sys.path.insert(0, WEEK7_SCRIPTS)

import auto_recovery as ar

# ── helpers ───────────────────────────────────────────────────────────────────

def _healthy_metrics() -> dict:
    """Metrics dict with all values well inside thresholds — no alerts."""
    return {
        "status": "ok",
        "alerting": [],
        "metrics": {
            "cost_per_query":           {"value": 0.00027,  "status": "ok"},
            "accuracy_rate":            {"value": 0.85,     "status": "ok"},
            "feedback_correction_rate": {"value": 0.05,     "status": "ok"},
            "model_cost_latency_corr":  {"value": 0.30,     "status": "ok"},
            "latency_percentiles_w7":   {"value": {"p50_ms": 400, "p95_ms": 1500, "p99_ms": 3000}, "status": "ok"},
            "system_health":            {"value": {"error_rate": 0.01, "timeout_rate": 0.00}, "status": "ok"},
            "corpus_freshness":         {"value": {"avg_age_days": 45, "stale_count": 3}, "status": "ok"},
        },
    }


def _alert_metrics(alerting: list[str], overrides: dict) -> dict:
    """Build a metrics dict with specified alerts firing."""
    m = _healthy_metrics()
    m["alerting"] = alerting
    m["status"]   = "alert"
    for key, val in overrides.items():
        if key in m["metrics"]:
            m["metrics"][key]["value"]  = val
            m["metrics"][key]["status"] = "alert"
    return m


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clean_config_files(tmp_path, monkeypatch):
    """
    Redirect config/log paths to a temp directory so tests don't pollute
    the real week7/ folder, and don't interfere with each other.
    """
    monkeypatch.setattr(ar, "FALLBACK_CONFIG",   str(tmp_path / "fallback_config.json"))
    monkeypatch.setattr(ar, "RATE_LIMIT_CONFIG", str(tmp_path / "rate_limit_config.json"))
    monkeypatch.setattr(ar, "RECOVERY_LOG_PATH", str(tmp_path / "recovery.log"))
    monkeypatch.setattr(ar, "ALERTS_LOG_PATH",   str(tmp_path / "alerts.log"))
    monkeypatch.setattr(ar, "BLOATED_CORPUS",    str(tmp_path / "bloated.json"))
    monkeypatch.setattr(ar, "CLEANED_CORPUS",    str(tmp_path / "cleaned.json"))
    monkeypatch.setattr(ar, "ARCHIVED_CORPUS",   str(tmp_path / "archived.json"))
    yield tmp_path


# ── 1. No actions when healthy ────────────────────────────────────────────────

def test_no_actions_when_healthy():
    """decide_and_act returns empty dict when no metrics are alerting."""
    results = ar.decide_and_act(_healthy_metrics(), dry_run=True)
    assert results == {}, f"Expected no actions, got: {results}"


# ── 2-5. corpus_rearchival triggers ──────────────────────────────────────────

def test_rearchival_on_cost_spike():
    """corpus_rearchival triggers when cost_per_query > $0.00081."""
    metrics = _alert_metrics(
        alerting=["cost_per_query"],
        overrides={"cost_per_query": 0.002},
    )
    results = ar.decide_and_act(metrics, dry_run=True)
    assert "corpus_rearchival" in results
    assert results["corpus_rearchival"] is True


def test_rearchival_on_accuracy_drop():
    """corpus_rearchival triggers when accuracy_rate < 60%."""
    metrics = _alert_metrics(
        alerting=["accuracy_rate"],
        overrides={"accuracy_rate": 0.45},
    )
    results = ar.decide_and_act(metrics, dry_run=True)
    assert "corpus_rearchival" in results


def test_rearchival_on_feedback_spike():
    """corpus_rearchival triggers when feedback_correction_rate > 20%."""
    metrics = _alert_metrics(
        alerting=["feedback_correction_rate"],
        overrides={"feedback_correction_rate": 0.35},
    )
    results = ar.decide_and_act(metrics, dry_run=True)
    assert "corpus_rearchival" in results


def test_rearchival_on_high_correlation():
    """corpus_rearchival triggers when model_cost_latency_corr > 0.75."""
    metrics = _alert_metrics(
        alerting=["model_cost_latency_corr"],
        overrides={"model_cost_latency_corr": 0.90},
    )
    results = ar.decide_and_act(metrics, dry_run=True)
    assert "corpus_rearchival" in results


# ── 6. Stricter threshold when both cost AND accuracy alert ───────────────────

def test_stricter_threshold_when_dual_alert(tmp_path, monkeypatch, caplog):
    """
    When both cost_per_query and accuracy_rate are alerting,
    action_rearchive_corpus is called with stricter=True (threshold 0.6).
    Verify via dry-run log message.
    """
    import logging
    metrics = _alert_metrics(
        alerting=["cost_per_query", "accuracy_rate"],
        overrides={"cost_per_query": 0.002, "accuracy_rate": 0.45},
    )
    with caplog.at_level(logging.INFO, logger="auto_recovery"):
        results = ar.decide_and_act(metrics, dry_run=True)

    assert "corpus_rearchival" in results
    # Dry-run log should mention threshold 0.6
    assert any("0.6" in msg for msg in caplog.messages), (
        "Expected stricter threshold 0.6 in log, got: " + str(caplog.messages)
    )


def test_normal_threshold_on_single_alert(caplog):
    """When only cost alerts (not accuracy), threshold should be 0.5."""
    import logging
    metrics = _alert_metrics(
        alerting=["cost_per_query"],
        overrides={"cost_per_query": 0.002},
    )
    with caplog.at_level(logging.INFO, logger="auto_recovery"):
        ar.decide_and_act(metrics, dry_run=True)

    # Should NOT mention threshold 0.6
    assert not any("stricter=True" in msg and "0.6" in msg for msg in caplog.messages
                   if "0.5" not in msg), \
        "Stricter threshold used even though only cost is alerting"


# ── 7-8. fallback_mode triggers ───────────────────────────────────────────────

def test_fallback_on_latency_spike(clean_config_files):
    """fallback_mode triggers and writes fallback_config.json on p95 > 3000ms."""
    metrics = _alert_metrics(
        alerting=["latency_percentiles_w7"],
        overrides={"latency_percentiles_w7": {"p50_ms": 800, "p95_ms": 4500, "p99_ms": 7000}},
    )
    results = ar.decide_and_act(metrics, dry_run=False)
    assert "fallback_mode" in results
    assert results["fallback_mode"] is True
    assert os.path.exists(ar.FALLBACK_CONFIG)


def test_fallback_on_timeout_rate(clean_config_files):
    """fallback_mode triggers on timeout_rate > 2%."""
    metrics = _alert_metrics(
        alerting=["system_health"],
        overrides={"system_health": {"error_rate": 0.01, "timeout_rate": 0.05}},
    )
    results = ar.decide_and_act(metrics, dry_run=False)
    assert "fallback_mode" in results


# ── 9. fallback_mode disabled on recovery ────────────────────────────────────

def test_fallback_disabled_when_latency_recovers(clean_config_files):
    """
    If fallback_config.json exists but latency is no longer alerting,
    decide_and_act should remove the file (fallback_disabled action).

    Note: the disable path only runs when at least one OTHER alert is firing
    (the early-return guard skips it when all metrics are healthy). We simulate
    a corpus_freshness alert — which has no fallback trigger — so the else
    branch of the fallback logic fires.
    """
    # Write a fallback config (simulate a previous latency incident)
    config = {
        "fallback_mode": True,
        "top_k":         1,
        "reason":        "previous latency spike",
        "expires_at":    time.time() + 600,
    }
    with open(ar.FALLBACK_CONFIG, "w") as f:
        json.dump(config, f)

    # Trigger a non-latency alert (corpus_freshness) so decide_and_act proceeds
    # past the early-return guard, but latency is healthy → fallback_disabled fires
    metrics = _alert_metrics(
        alerting=["corpus_freshness"],
        overrides={"corpus_freshness": {"avg_age_days": 200, "stale_count": 30}},
    )
    results = ar.decide_and_act(metrics, dry_run=False)

    assert "fallback_disabled" in results, (
        f"Expected fallback_disabled action, got: {list(results.keys())}"
    )
    assert not os.path.exists(ar.FALLBACK_CONFIG), (
        "fallback_config.json should have been removed when latency recovered"
    )


# ── 10. rate_limiting triggers ────────────────────────────────────────────────

def test_rate_limiting_on_high_error_rate(clean_config_files):
    """rate_limiting triggers and writes rate_limit_config.json on error_rate > 5%."""
    metrics = _alert_metrics(
        alerting=["system_health"],
        overrides={"system_health": {"error_rate": 0.12, "timeout_rate": 0.00}},
    )
    results = ar.decide_and_act(metrics, dry_run=False)
    assert "rate_limiting" in results
    assert results["rate_limiting"] is True
    assert os.path.exists(ar.RATE_LIMIT_CONFIG)


# ── 11. notify always fires for corpus_freshness ─────────────────────────────

def test_notify_fires_for_corpus_freshness():
    """corpus_freshness always routes to notify (requires human review)."""
    metrics = _alert_metrics(
        alerting=["corpus_freshness"],
        overrides={"corpus_freshness": {"avg_age_days": 200, "stale_count": 30}},
    )
    results = ar.decide_and_act(metrics, dry_run=True)
    assert "notify_corpus_freshness" in results


# ── 12. fallback_config.json schema ──────────────────────────────────────────

def test_fallback_config_schema(clean_config_files):
    """fallback_config.json must contain required fields with correct types."""
    ar.action_enable_fallback("test latency spike", dry_run=False)
    assert os.path.exists(ar.FALLBACK_CONFIG)

    with open(ar.FALLBACK_CONFIG) as f:
        cfg = json.load(f)

    assert cfg.get("fallback_mode") is True
    assert isinstance(cfg.get("top_k"), int)
    assert cfg["top_k"] == 1, f"Expected top_k=1, got {cfg['top_k']}"
    assert "expires_at" in cfg
    assert cfg["expires_at"] > time.time(), "Expiry time should be in the future"
    assert "activated_at" in cfg
    assert "reason" in cfg


# ── 13. rate_limit_config.json schema ────────────────────────────────────────

def test_rate_limit_config_schema(clean_config_files):
    """rate_limit_config.json must contain required fields with correct types."""
    ar.action_enable_rate_limiting("high error rate test", dry_run=False)
    assert os.path.exists(ar.RATE_LIMIT_CONFIG)

    with open(ar.RATE_LIMIT_CONFIG) as f:
        cfg = json.load(f)

    assert cfg.get("rate_limiting_enabled") is True
    assert isinstance(cfg.get("max_queries_per_minute"), int)
    assert cfg["max_queries_per_minute"] <= 10, (
        f"Rate limit should be ≤10 (halved from default), got {cfg['max_queries_per_minute']}"
    )
    assert "expires_at" in cfg
    assert cfg["expires_at"] > time.time()


# ── 14. Idempotency ───────────────────────────────────────────────────────────

def test_fallback_idempotent(clean_config_files):
    """Calling action_enable_fallback twice returns True both times, writes once."""
    result1 = ar.action_enable_fallback("first call", dry_run=False)
    mtime1  = os.path.getmtime(ar.FALLBACK_CONFIG)

    time.sleep(0.05)
    result2 = ar.action_enable_fallback("second call", dry_run=False)
    mtime2  = os.path.getmtime(ar.FALLBACK_CONFIG)

    assert result1 is True
    assert result2 is True
    assert mtime1 == mtime2, "File should not be rewritten on second idempotent call"


def test_rate_limit_idempotent(clean_config_files):
    """Calling action_enable_rate_limiting twice returns True both times."""
    result1 = ar.action_enable_rate_limiting("first", dry_run=False)
    mtime1  = os.path.getmtime(ar.RATE_LIMIT_CONFIG)

    time.sleep(0.05)
    result2 = ar.action_enable_rate_limiting("second", dry_run=False)
    mtime2  = os.path.getmtime(ar.RATE_LIMIT_CONFIG)

    assert result1 is True
    assert result2 is True
    assert mtime1 == mtime2, "File should not be rewritten on second idempotent call"


# ── 15. Dry-run writes no files ───────────────────────────────────────────────

def test_dry_run_no_files_written(clean_config_files):
    """In dry_run=True mode, no config files should be created."""
    metrics = _alert_metrics(
        alerting=["latency_percentiles_w7", "system_health", "corpus_freshness"],
        overrides={
            "latency_percentiles_w7": {"p50_ms": 800, "p95_ms": 4500, "p99_ms": 7000},
            "system_health":          {"error_rate": 0.10, "timeout_rate": 0.03},
            "corpus_freshness":       {"avg_age_days": 200, "stale_count": 30},
        },
    )
    ar.decide_and_act(metrics, dry_run=True)

    assert not os.path.exists(ar.FALLBACK_CONFIG),   "fallback_config.json written in dry-run"
    assert not os.path.exists(ar.RATE_LIMIT_CONFIG), "rate_limit_config.json written in dry-run"
