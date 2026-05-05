"""
alert_runbook.py — Week 7: Simulate metric threshold breaches and print runbooks.

For each of the 8 Week 7 metrics, this script:
  1. Simulates a threshold breach by injecting bad query records
  2. Calls get_metrics() to confirm the alert fires
  3. Prints the step-by-step diagnostic runbook for that alert

Run this to verify alerts fire correctly AND as a reference doc for
on-call engineers — it tells them exactly what to do when each alert fires.

Usage:
    python week7/scripts/alert_runbook.py

    # Run only specific alerts:
    python week7/scripts/alert_runbook.py --alerts cost_per_query system_health
"""

import sys
import os
import argparse
import tempfile

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
WEEK7_APP = os.path.join(REPO_ROOT, "week7", "app")
sys.path.insert(0, WEEK7_APP)

os.environ["DOCS_PATH"] = os.path.join(
    REPO_ROOT, "data", "raw", "techcorp", "documents_week7_cleaned.json"
)

# ---------------------------------------------------------------------------
# Runbook definitions
# Each entry maps a metric name → {trigger, steps, auto_recovery}
# ---------------------------------------------------------------------------

RUNBOOKS: dict[str, dict] = {

    "cost_per_query": {
        "trigger":  "avg cost/query > $0.00081 (3x baseline of $0.00027)",
        "severity": "HIGH — directly maps to CFO complaint",
        "steps": [
            "1. Pull /metrics and confirm cost_per_query.value and avg_tokens are elevated.",
            "2. Run scripts/diagnose_degradation.py — compare token counts clean vs live corpus.",
            "3. If avg_tokens > 2x baseline: corpus bloat is the cause → go to step 4.",
            "   If avg_tokens are normal: check if a new high-cost query type appeared (intent breakdown).",
            "4. Run app/cost_optimization.py --input <live corpus> to score and archive junk docs.",
            "5. Restart agent with DOCS_PATH pointing at cleaned corpus.",
            "6. Re-run /metrics — confirm cost_per_query drops below threshold.",
            "7. If cost stays high after cleanup: check if top_k was increased; revert to top_k=3.",
            "8. File incident report with before/after cost figures.",
        ],
        "auto_recovery": "auto_recovery.py triggers corpus re-archival when this alert fires.",
    },

    "latency_percentiles_w7": {
        "trigger":  "latency p95 > 3000ms",
        "severity": "MEDIUM — users timing out",
        "steps": [
            "1. Check /metrics latency_percentiles_w7.value.p95_ms.",
            "2. Check model_cost_latency_corr — if correlation > 0.75, latency is context-driven.",
            "3. If context-driven: corpus bloat inflating retrieved doc size.",
            "   → Run scripts/diagnose_degradation.py to confirm token count increase.",
            "   → Run app/cost_optimization.py to clean corpus.",
            "4. If NOT context-driven: check Ollama response times (curl http://localhost:11434/api/tags).",
            "5. Check system_health for timeout_rate — if >2%, Ollama may be overloaded.",
            "6. Temporarily reduce top_k from 3 to 1 (fallback mode) via FALLBACK_TOP_K=1 env var.",
            "7. After fix: verify p95 drops below 3000ms over next 10 queries.",
        ],
        "auto_recovery": "auto_recovery.py reduces top_k to 1 when p95 > 3000ms.",
    },

    "accuracy_rate": {
        "trigger":  "accuracy < 60% (keyword-hit proxy drops below threshold)",
        "severity": "HIGH — users getting wrong answers",
        "steps": [
            "1. Check /metrics accuracy_rate.value and feedback_correction_rate.",
            "2. If feedback_correction_rate is also elevated: users are actively correcting → systematic issue.",
            "3. Run scripts/diagnose_degradation.py — check per-query accuracy delta.",
            "4. Identify which intents are failing most (policy vs employee vs expense etc.).",
            "5. For policy failures: likely misleading or contradicting docs in corpus.",
            "   → Run app/cost_optimization.py with --threshold 0.6 (stricter) to remove more junk.",
            "6. For non-policy failures: check database.py queries returning stale data.",
            "7. Review corpus_freshness metric — stale docs may be the source.",
            "8. After cleanup: re-run scripts/measure_optimization_impact.py to confirm recovery.",
        ],
        "auto_recovery": "auto_recovery.py triggers corpus re-archival when correction rate spikes.",
    },

    "feedback_correction_rate": {
        "trigger":  "correction rate > 20% in last 60 minutes",
        "severity": "HIGH — users actively correcting answers at scale",
        "steps": [
            "1. Run scripts/feedback_metrics.py to get full breakdown.",
            "2. Check by_intent — which domain is being corrected most?",
            "3. Check top_corrected_queries — is one query being corrected repeatedly?",
            "4. If one query dominates: spot-fix by adding a quality document for that topic.",
            "5. If broad correction pattern (multiple intents): corpus-wide issue.",
            "   → Run scripts/diagnose_degradation.py to confirm accuracy drop.",
            "   → Run app/cost_optimization.py with stricter threshold.",
            "6. Check correction velocity trend — is rate increasing or stabilising?",
            "7. If rate > 40%: consider enabling fallback mode (return raw tool output, skip LLM).",
            "8. After fix: monitor correction rate over next hour — should drop below 10%.",
        ],
        "auto_recovery": "auto_recovery.py triggers corpus re-archival when correction rate > 20%.",
    },

    "corpus_freshness": {
        "trigger":  "avg doc age > 180 days OR stale doc count > 20",
        "severity": "MEDIUM — answers may reference outdated policies",
        "steps": [
            "1. Check /metrics corpus_freshness.value — note avg_age_days and stale_count.",
            "2. Identify which categories have the oldest docs:",
            "   python3 -c \"import json; docs=json.load(open('<corpus>')); "
            "[print(d['id'], d.get('last_updated','')) for d in docs if d.get('category')=='HR']\"",
            "3. For policy docs older than 180 days: contact doc owners to confirm if still current.",
            "4. Outdated confirmed docs: archive manually or add last_updated refresh.",
            "5. For auto-generated summaries with old dates: re-run corpus_bloat cleanup.",
            "6. Update DOCS_PATH to point at refreshed corpus and restart agent.",
            "7. Verify corpus_freshness avg_age_days drops below 180 in /metrics.",
        ],
        "auto_recovery": "No automatic recovery — requires human review of doc content.",
    },

    "model_cost_latency_corr": {
        "trigger":  "Pearson correlation between tokens_input and latency_ms > 0.75",
        "severity": "MEDIUM — early warning of corpus bloat before cost/latency alerts fire",
        "steps": [
            "1. A high correlation means longer contexts → longer Ollama inference times.",
            "2. This is an early-warning signal — cost_per_query and latency may not have alerted yet.",
            "3. Run scripts/diagnose_degradation.py — confirm avg_tokens has increased vs baseline.",
            "4. Check corpus size: wc -l <corpus.json> vs baseline (74 quality docs).",
            "5. If corpus has grown: run app/cost_optimization.py proactively.",
            "6. If corpus size is unchanged: check if queries have changed (new verbose query types).",
            "7. After cleanup: correlation should drop below 0.40 (weak relationship).",
        ],
        "auto_recovery": "auto_recovery.py runs proactive corpus cleanup when correlation > 0.75.",
    },

    "system_health": {
        "trigger":  "error_rate > 5% OR timeout_rate > 2%",
        "severity": "CRITICAL — agent returning failures to users",
        "steps": [
            "1. Check /metrics system_health.value — note error_rate and timeout_rate separately.",
            "2. For high error_rate (>5%):",
            "   a. Check agent logs: tail -f backend.log | grep ERROR",
            "   b. Common causes: database connection failure, access_control.json missing, bad imports.",
            "   c. Check /health endpoint — if it returns error, restart the agent process.",
            "3. For high timeout_rate (>2%):",
            "   a. Ollama likely overloaded or corpus context too long.",
            "   b. Check Ollama: curl http://localhost:11434/api/tags",
            "   c. If Ollama is down: restart with 'ollama serve'.",
            "   d. Enable fallback mode: set FALLBACK_TOP_K=1 to reduce context size.",
            "4. If both error and timeout are high: likely a resource exhaustion issue.",
            "   a. Check memory: free -h",
            "   b. Check disk: df -h (corpus files may have grown too large).",
            "5. After fix: error_rate and timeout_rate should return to 0% within 5 minutes.",
            "6. Page on-call engineer if errors persist > 10 minutes.",
        ],
        "auto_recovery": "auto_recovery.py enables rate limiting when error_rate > 5%.",
    },

    "cost_per_query_monthly": {
        "trigger":  "monthly cost projection > $1,500 (original budget)",
        "severity": "HIGH — CFO alert threshold",
        "steps": [
            "1. Monthly projection = avg_cost_usd * 30,000 queries/month.",
            "2. Check cost_per_query metric for current avg.",
            "3. If avg_cost > $0.00005: token usage has grown — follow cost_per_query runbook.",
            "4. If query volume has grown (not cost per query): review rate limiting settings.",
            "5. Check guardrails.py MAX_QUERIES_PER_MINUTE — consider lowering if traffic spiked.",
            "6. Present CFO with: current monthly projection, root cause, fix timeline.",
        ],
        "auto_recovery": "auto_recovery.py triggers corpus re-archival to reduce cost per query.",
    },
}

# ---------------------------------------------------------------------------
# Breach simulators — inject bad records to trigger each alert
# ---------------------------------------------------------------------------

def _fresh_monitoring():
    """Re-import monitoring (and feedback) with clean state for each simulation."""
    import importlib
    # Reload feedback first so get_correction_rate picks up current FEEDBACK_LOG_PATH
    if "feedback" in sys.modules:
        importlib.reload(sys.modules["feedback"])
    import monitoring
    importlib.reload(monitoring)
    return monitoring


def simulate_cost_breach(mon):
    """Inject queries with 5x baseline cost."""
    for _ in range(5):
        mon.record_query("engineer", 500, 3, 0, "answer", "query", False,
                         cost_usd=0.00150, tokens_input=5000)


def simulate_latency_breach(mon):
    """Inject queries with very high latency."""
    for _ in range(5):
        mon.record_query("engineer", 8000, 3, 0, "answer", "query", False,
                         cost_usd=0.0003, tokens_input=400)


def simulate_accuracy_breach(mon):
    """Inject queries all marked incorrect."""
    for _ in range(10):
        mon.record_query("engineer", 400, 3, 0, "answer", "query", False,
                         cost_usd=0.0003, tokens_input=400, correct=False)


def simulate_feedback_breach(_mon):
    """Write a high-volume accepted corrections file."""
    import json, time
    from datetime import datetime, timezone, timedelta
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False)
    now = datetime.now(timezone.utc)
    # 3 accepted + 1 total in last hour = 75% correction rate
    for i in range(3):
        tmp.write(json.dumps({
            "feedback_id": f"sim_{i}", "accepted": True,
            "timestamp": (now - timedelta(minutes=10 * i)).isoformat(),
            "user_role": "hr", "intent": "benefits",
            "query": "How much PTO?", "wrong_answer": "10", "correct_answer": "20",
        }) + "\n")
    tmp.write(json.dumps({
        "feedback_id": "sim_total", "accepted": False,
        "timestamp": now.isoformat(),
        "user_role": "engineer", "intent": "policy",
        "query": "Travel limit?", "wrong_answer": "100", "correct_answer": "200",
        "rejection_reason": "not permitted",
    }) + "\n")
    tmp.close()
    os.environ["FEEDBACK_LOG_PATH"] = tmp.name
    return tmp.name


def simulate_corpus_freshness_breach(mon):
    """Point DOCS_PATH at bloated corpus (has many old docs)."""
    os.environ["DOCS_PATH"] = os.path.join(
        REPO_ROOT, "data", "raw", "techcorp", "documents_week7_bloated.json"
    )


def simulate_correlation_breach(mon):
    """Inject queries where high token count correlates strongly with high latency."""
    pairs = [(200, 100), (400, 300), (600, 500), (800, 700), (1200, 1100), (2000, 1900)]
    for tokens, latency in pairs:
        mon.record_query("engineer", latency, 3, 0, "answer", "query", False,
                         cost_usd=0.0003, tokens_input=tokens)


def simulate_system_health_breach(mon):
    """Inject a mix of errors and timeouts."""
    for _ in range(5):
        mon.record_query("engineer", 400, 3, 0, "answer", "query", False,
                         cost_usd=0.0003, tokens_input=400)
    for _ in range(3):
        mon.record_query("engineer", 500, 0, 0, "an error occurred", "query", False,
                         cost_usd=0.0, tokens_input=0, is_error=True)
    for _ in range(2):
        mon.record_query("engineer", 30000, 2, 0, "unable to connect", "query", False,
                         cost_usd=0.0, tokens_input=200, is_timeout=True)


SIMULATORS = {
    "cost_per_query":           simulate_cost_breach,
    "latency_percentiles_w7":   simulate_latency_breach,
    "accuracy_rate":            simulate_accuracy_breach,
    "feedback_correction_rate": simulate_feedback_breach,
    "corpus_freshness":         simulate_corpus_freshness_breach,
    "model_cost_latency_corr":  simulate_correlation_breach,
    "system_health":            simulate_system_health_breach,
}

# ---------------------------------------------------------------------------
# Run one runbook
# ---------------------------------------------------------------------------

def run_runbook(alert_name: str, verbose: bool = True) -> bool:
    """
    Simulate a breach for alert_name, fire get_metrics(), verify alert fires,
    print the runbook. Returns True if alert fired as expected.
    """
    if alert_name not in RUNBOOKS:
        print(f"  Unknown alert: {alert_name}")
        return False

    runbook = RUNBOOKS[alert_name]
    simulate_fn = SIMULATORS.get(alert_name)

    print(f"\n{'='*68}")
    print(f"  ALERT: {alert_name.upper()}")
    print(f"  Trigger:  {runbook['trigger']}")
    print(f"  Severity: {runbook['severity']}")
    print(f"{'='*68}")

    # Set up temp files
    tmp_audit = tempfile.NamedTemporaryFile(suffix=".log", delete=False)
    os.environ["AUDIT_LOG_PATH"] = tmp_audit.name
    tmp_feedback_path = None

    try:
        # For feedback alert: write temp log BEFORE reloading monitoring
        # so get_correction_rate() picks up the right path on first import
        if alert_name == "feedback_correction_rate":
            tmp_feedback_path = simulate_feedback_breach(None)

        mon = _fresh_monitoring()

        if alert_name != "feedback_correction_rate" and simulate_fn:
            simulate_fn(mon)

        metrics  = mon.get_metrics()
        alerting = metrics.get("alerting", [])

        # For monthly cost, check the cost_per_query metric value instead
        if alert_name == "cost_per_query_monthly":
            m = metrics["metrics"].get("cost_per_query", {})
            fired = (m.get("monthly_projection", 0) or 0) > 1500
        else:
            fired = alert_name in alerting

        status_tag = "✓ FIRED" if fired else "✗ DID NOT FIRE"
        print(f"\n  Alert status: {status_tag}")
        if alert_name in metrics.get("metrics", {}):
            val = metrics["metrics"][alert_name].get("value")
            print(f"  Metric value: {val}")

        print(f"\n  RUNBOOK — Diagnostic Steps:")
        for step in runbook["steps"]:
            print(f"    {step}")

        print(f"\n  Auto-recovery: {runbook['auto_recovery']}")

        return fired

    finally:
        os.unlink(tmp_audit.name)
        if tmp_feedback_path and os.path.exists(tmp_feedback_path):
            os.unlink(tmp_feedback_path)
        # Reset env vars
        os.environ.pop("AUDIT_LOG_PATH", None)
        os.environ["DOCS_PATH"] = os.path.join(
            REPO_ROOT, "data", "raw", "techcorp", "documents_week7_cleaned.json"
        )
        os.environ.pop("FEEDBACK_LOG_PATH", None)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate alerts and print runbooks")
    parser.add_argument(
        "--alerts", nargs="+",
        default=list(SIMULATORS.keys()),
        help="Alert names to simulate (default: all)",
    )
    args = parser.parse_args()

    print("TechCorp Week 7 — Alert Runbook Simulation")
    print(f"Simulating {len(args.alerts)} alert(s)...\n")

    results = {}
    for alert in args.alerts:
        fired = run_runbook(alert)
        results[alert] = fired

    print(f"\n{'='*68}")
    print("  SUMMARY")
    print(f"{'='*68}")
    for alert, fired in results.items():
        tag = "✓ fired" if fired else "✗ missed"
        print(f"  {tag}  {alert}")

    all_fired = all(results.values())
    print(f"\n  Result: {'ALL ALERTS FIRED ✓' if all_fired else 'SOME ALERTS DID NOT FIRE ✗'}")
    print(f"{'='*68}")

    if not all_fired:
        sys.exit(1)
