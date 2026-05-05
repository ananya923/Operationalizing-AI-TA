"""
auto_recovery.py — Week 7: Automated recovery actions triggered by metric alerts.

Polls the /metrics endpoint (or runs metrics directly offline) and executes
one of four recovery actions when thresholds are breached:

  1. CORPUS RE-ARCHIVAL    — triggered by: cost spike, accuracy drop, feedback spike,
                             or high cost-latency correlation
                             Action: re-runs cost_optimization.py with stricter threshold,
                             writes cleaned corpus, updates DOCS_PATH symlink

  2. FALLBACK MODE         — triggered by: latency p95 spike (> 3000ms)
                             Action: writes a fallback config that reduces top_k to 1
                             so the agent retrieves fewer docs per query

  3. RATE LIMITING         — triggered by: system error rate > 5%
                             Action: writes a rate limit config that halves
                             MAX_QUERIES_PER_MINUTE for 10 minutes

  4. ALERT NOTIFICATION    — triggered by: any alert not covered above
                             Action: appends to an alerts log file so CI/CD or
                             an on-call script can page the team

Each action is idempotent — re-running when already recovered is a no-op.
Each action is logged to auto_recovery.log with timestamp and reason.

Usage:
    # Offline (reads metrics directly — no running agent needed):
    python week7/scripts/auto_recovery.py

    # Against a live agent:
    python week7/scripts/auto_recovery.py --mode online --base-url http://localhost:8001

    # Dry run (print actions without executing):
    python week7/scripts/auto_recovery.py --dry-run
"""

import sys
import os
import json
import time
import argparse
import subprocess
import logging
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
WEEK7_APP = os.path.join(REPO_ROOT, "week7", "app")
sys.path.insert(0, WEEK7_APP)

DEFAULT_BASE_URL    = "http://localhost:8001"
RECOVERY_LOG_PATH   = os.path.join(REPO_ROOT, "week7", "auto_recovery.log")
ALERTS_LOG_PATH     = os.path.join(REPO_ROOT, "week7", "alerts.log")
FALLBACK_CONFIG     = os.path.join(REPO_ROOT, "week7", "fallback_config.json")
RATE_LIMIT_CONFIG   = os.path.join(REPO_ROOT, "week7", "rate_limit_config.json")

BLOATED_CORPUS  = os.path.join(REPO_ROOT, "data", "raw", "techcorp", "documents_week7_bloated.json")
CLEANED_CORPUS  = os.path.join(REPO_ROOT, "data", "raw", "techcorp", "documents_week7_cleaned.json")
ARCHIVED_CORPUS = os.path.join(REPO_ROOT, "data", "raw", "techcorp", "documents_week7_archived.json")

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(RECOVERY_LOG_PATH),
    ],
)
logger = logging.getLogger("auto_recovery")

# ---------------------------------------------------------------------------
# Thresholds that trigger each recovery action
# (mirrors METRIC_DEFINITIONS in monitoring.py)
# ---------------------------------------------------------------------------

THRESHOLDS = {
    "cost_per_query":           0.00081,   # > 3x baseline → re-archival
    "latency_p95_ms":           3000,      # > 3s p95 → fallback mode
    "accuracy_rate":            0.60,      # < 60% → re-archival
    "feedback_correction_rate": 0.20,      # > 20% → re-archival
    "model_cost_latency_corr":  0.75,      # > 0.75 → proactive re-archival
    "error_rate":               0.05,      # > 5% → rate limiting
    "timeout_rate":             0.02,      # > 2% → fallback mode
}

# ---------------------------------------------------------------------------
# Fetch metrics
# ---------------------------------------------------------------------------

def fetch_metrics_online(base_url: str) -> dict:
    """Fetch /metrics from a live agent."""
    try:
        import httpx
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(f"{base_url}/metrics")
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.error(f"Could not reach agent at {base_url}: {e}")
        return {}


def fetch_metrics_offline() -> dict:
    """Compute metrics directly without a running agent."""
    os.environ.setdefault(
        "DOCS_PATH",
        CLEANED_CORPUS if os.path.exists(CLEANED_CORPUS) else BLOATED_CORPUS,
    )
    try:
        import monitoring as mon
        return mon.get_metrics()
    except Exception as e:
        logger.error(f"Could not compute offline metrics: {e}")
        return {}


# ---------------------------------------------------------------------------
# Extract scalar values from nested metric response
# ---------------------------------------------------------------------------

def _extract(metrics: dict, *keys):
    """Safely traverse nested dict. Returns None if any key is missing."""
    node = metrics
    for k in keys:
        if not isinstance(node, dict):
            return None
        node = node.get(k)
    return node


def _alerting_metrics(metrics: dict) -> list[str]:
    return metrics.get("alerting", [])


# ---------------------------------------------------------------------------
# Recovery action 1: Corpus re-archival
# ---------------------------------------------------------------------------

def action_rearchive_corpus(reason: str, dry_run: bool, stricter: bool = False) -> bool:
    """
    Re-run cost_optimization.py with threshold 0.5 (or 0.6 if stricter=True).
    Returns True if action was taken.
    """
    threshold = 0.6 if stricter else 0.5
    logger.info(f"[RECOVERY] Corpus re-archival triggered — reason: {reason}")
    logger.info(f"           Threshold: {threshold} | stricter={stricter}")

    if dry_run:
        logger.info("[DRY RUN] Would run: python week7/app/cost_optimization.py "
                    f"--threshold {threshold}")
        return True

    # Check input corpus exists
    input_corpus = BLOATED_CORPUS if os.path.exists(BLOATED_CORPUS) else CLEANED_CORPUS
    if not os.path.exists(input_corpus):
        logger.error(f"Input corpus not found: {input_corpus}")
        return False

    result = subprocess.run(
        [
            sys.executable,
            os.path.join(WEEK7_APP, "cost_optimization.py"),
            "--input",     input_corpus,
            "--output",    CLEANED_CORPUS,
            "--archive",   ARCHIVED_CORPUS,
            "--threshold", str(threshold),
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        logger.info(f"[RECOVERY] ✓ Corpus re-archival complete → {CLEANED_CORPUS}")
        # Update DOCS_PATH so future metric reads use the fresh corpus
        os.environ["DOCS_PATH"] = CLEANED_CORPUS
        _log_alert_notification("corpus_rearchival", reason, "completed")
        return True
    else:
        logger.error(f"[RECOVERY] ✗ Corpus re-archival failed:\n{result.stderr[:500]}")
        return False


# ---------------------------------------------------------------------------
# Recovery action 2: Fallback mode (reduce top_k)
# ---------------------------------------------------------------------------

def action_enable_fallback(reason: str, dry_run: bool) -> bool:
    """
    Write fallback_config.json with top_k=1 and a 10-minute expiry.
    The agent checks this file on startup (or via a hot-reload mechanism).
    Returns True if action was taken or already active.
    """
    logger.info(f"[RECOVERY] Fallback mode triggered — reason: {reason}")

    # Check if already active and not expired
    if os.path.exists(FALLBACK_CONFIG):
        try:
            with open(FALLBACK_CONFIG) as f:
                cfg = json.load(f)
            expiry = cfg.get("expires_at", 0)
            if expiry > time.time():
                logger.info(f"[RECOVERY] Fallback mode already active "
                            f"(expires {datetime.fromtimestamp(expiry).isoformat()})")
                return True
        except Exception:
            pass

    config = {
        "fallback_mode":    True,
        "top_k":            1,
        "reason":           reason,
        "activated_at":     datetime.now(timezone.utc).isoformat(),
        "expires_at":       time.time() + 600,  # 10 minutes
        "expires_at_human": (datetime.now(timezone.utc).strftime("%H:%M:%S") + " + 10min"),
    }

    if dry_run:
        logger.info(f"[DRY RUN] Would write fallback config: {json.dumps(config, indent=2)}")
        return True

    os.makedirs(os.path.dirname(os.path.abspath(FALLBACK_CONFIG)), exist_ok=True)
    with open(FALLBACK_CONFIG, "w") as f:
        json.dump(config, f, indent=2)

    logger.info(f"[RECOVERY] ✓ Fallback config written → {FALLBACK_CONFIG}")
    _log_alert_notification("fallback_mode", reason, "enabled")
    return True


def action_disable_fallback(dry_run: bool) -> bool:
    """Remove fallback config once recovery is confirmed."""
    if not os.path.exists(FALLBACK_CONFIG):
        return True  # already disabled

    if dry_run:
        logger.info(f"[DRY RUN] Would remove fallback config: {FALLBACK_CONFIG}")
        return True

    os.remove(FALLBACK_CONFIG)
    logger.info(f"[RECOVERY] ✓ Fallback mode disabled")
    return True


# ---------------------------------------------------------------------------
# Recovery action 3: Rate limiting
# ---------------------------------------------------------------------------

def action_enable_rate_limiting(reason: str, dry_run: bool) -> bool:
    """
    Write rate_limit_config.json halving MAX_QUERIES_PER_MINUTE for 10 min.
    Returns True if action was taken.
    """
    logger.info(f"[RECOVERY] Rate limiting triggered — reason: {reason}")

    # Check if already active
    if os.path.exists(RATE_LIMIT_CONFIG):
        try:
            with open(RATE_LIMIT_CONFIG) as f:
                cfg = json.load(f)
            if cfg.get("expires_at", 0) > time.time():
                logger.info("[RECOVERY] Rate limiting already active")
                return True
        except Exception:
            pass

    config = {
        "rate_limiting_enabled":     True,
        "max_queries_per_minute":    5,    # halved from default 10
        "reason":                    reason,
        "activated_at":              datetime.now(timezone.utc).isoformat(),
        "expires_at":                time.time() + 600,
        "expires_at_human":          (datetime.now(timezone.utc).strftime("%H:%M:%S") + " + 10min"),
    }

    if dry_run:
        logger.info(f"[DRY RUN] Would write rate limit config: {json.dumps(config, indent=2)}")
        return True

    os.makedirs(os.path.dirname(os.path.abspath(RATE_LIMIT_CONFIG)), exist_ok=True)
    with open(RATE_LIMIT_CONFIG, "w") as f:
        json.dump(config, f, indent=2)

    logger.info(f"[RECOVERY] ✓ Rate limit config written → {RATE_LIMIT_CONFIG}")
    _log_alert_notification("rate_limiting", reason, "enabled")
    return True


# ---------------------------------------------------------------------------
# Recovery action 4: Alert notification (catch-all)
# ---------------------------------------------------------------------------

def _log_alert_notification(alert_name: str, reason: str, action: str):
    """Append an alert record to alerts.log for CI/CD or paging systems to consume."""
    record = {
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "alert":      alert_name,
        "reason":     reason,
        "action":     action,
    }
    os.makedirs(os.path.dirname(os.path.abspath(ALERTS_LOG_PATH)), exist_ok=True)
    with open(ALERTS_LOG_PATH, "a") as f:
        f.write(json.dumps(record) + "\n")


def action_notify(alert_name: str, reason: str, dry_run: bool) -> bool:
    """Log the alert for paging / CI. Always succeeds."""
    logger.info(f"[NOTIFY] Alert: {alert_name} — {reason}")
    if not dry_run:
        _log_alert_notification(alert_name, reason, "notified")
    else:
        logger.info(f"[DRY RUN] Would write to alerts.log: {alert_name}")
    return True


# ---------------------------------------------------------------------------
# Decision engine — map alerting metrics to recovery actions
# ---------------------------------------------------------------------------

def decide_and_act(metrics: dict, dry_run: bool) -> dict[str, bool]:
    """
    Inspect metrics, decide which recovery actions to trigger, execute them.
    Returns dict of {action_name: success}.
    """
    results: dict[str, bool] = {}
    alerting = _alerting_metrics(metrics)
    m = metrics.get("metrics", {})

    if not alerting:
        logger.info("All metrics OK — no recovery actions needed.")
        return results

    logger.info(f"Alerting metrics: {alerting}")

    # ── 1. Corpus re-archival ────────────────────────────────────────────────
    rearchive_triggers = []

    cost = _extract(m, "cost_per_query", "value") or 0
    if cost > THRESHOLDS["cost_per_query"]:
        rearchive_triggers.append(f"cost_per_query={cost:.5f} > {THRESHOLDS['cost_per_query']}")

    acc = _extract(m, "accuracy_rate", "value") or 1.0
    if acc < THRESHOLDS["accuracy_rate"]:
        rearchive_triggers.append(f"accuracy_rate={acc:.2f} < {THRESHOLDS['accuracy_rate']}")

    fbk = _extract(m, "feedback_correction_rate", "value") or 0
    if fbk > THRESHOLDS["feedback_correction_rate"]:
        rearchive_triggers.append(
            f"feedback_correction_rate={fbk:.2f} > {THRESHOLDS['feedback_correction_rate']}"
        )

    corr = _extract(m, "model_cost_latency_corr", "value") or 0
    if corr > THRESHOLDS["model_cost_latency_corr"]:
        rearchive_triggers.append(
            f"model_cost_latency_corr={corr:.2f} > {THRESHOLDS['model_cost_latency_corr']}"
        )

    if rearchive_triggers:
        # Use stricter threshold if both cost AND accuracy are alerting
        stricter = (cost > THRESHOLDS["cost_per_query"]) and (acc < THRESHOLDS["accuracy_rate"])
        reason = "; ".join(rearchive_triggers)
        results["corpus_rearchival"] = action_rearchive_corpus(reason, dry_run, stricter)

    # ── 2. Fallback mode ─────────────────────────────────────────────────────
    fallback_triggers = []

    lat_val = _extract(m, "latency_percentiles_w7", "value") or {}
    p95 = lat_val.get("p95_ms", 0) if isinstance(lat_val, dict) else 0
    if p95 > THRESHOLDS["latency_p95_ms"]:
        fallback_triggers.append(f"latency_p95={p95:.0f}ms > {THRESHOLDS['latency_p95_ms']}ms")

    health_val = _extract(m, "system_health", "value") or {}
    timeout_rate = health_val.get("timeout_rate", 0) if isinstance(health_val, dict) else 0
    if timeout_rate > THRESHOLDS["timeout_rate"]:
        fallback_triggers.append(
            f"timeout_rate={timeout_rate:.2f} > {THRESHOLDS['timeout_rate']}"
        )

    if fallback_triggers:
        results["fallback_mode"] = action_enable_fallback(
            "; ".join(fallback_triggers), dry_run
        )
    else:
        # Latency recovered — disable fallback if active
        if os.path.exists(FALLBACK_CONFIG):
            results["fallback_disabled"] = action_disable_fallback(dry_run)

    # ── 3. Rate limiting ─────────────────────────────────────────────────────
    error_rate = health_val.get("error_rate", 0) if isinstance(health_val, dict) else 0
    if error_rate > THRESHOLDS["error_rate"]:
        results["rate_limiting"] = action_enable_rate_limiting(
            f"error_rate={error_rate:.2f} > {THRESHOLDS['error_rate']}", dry_run
        )

    # ── 4. Notify for any remaining unhandled alerts ─────────────────────────
    handled = {"cost_per_query", "accuracy_rate", "feedback_correction_rate",
               "model_cost_latency_corr", "latency_percentiles_w7",
               "system_health", "corpus_freshness"}
    unhandled = [a for a in alerting if a not in handled]
    for alert in unhandled:
        results[f"notify_{alert}"] = action_notify(
            alert, f"Alert fired with no automated recovery defined", dry_run
        )

    # Always notify corpus_freshness — requires human review
    if "corpus_freshness" in alerting:
        results["notify_corpus_freshness"] = action_notify(
            "corpus_freshness",
            "Stale documents detected — manual review required",
            dry_run,
        )

    return results


# ---------------------------------------------------------------------------
# Print summary
# ---------------------------------------------------------------------------

def print_summary(results: dict[str, bool]):
    print(f"\n{'='*56}")
    print("  AUTO RECOVERY SUMMARY")
    print(f"{'='*56}")
    if not results:
        print("  No actions triggered — system healthy.")
    else:
        for action, success in results.items():
            tag = "✓" if success else "✗"
            print(f"  {tag} {action}")
    print(f"{'='*56}")
    print(f"  Recovery log: {RECOVERY_LOG_PATH}")
    print(f"  Alerts log:   {ALERTS_LOG_PATH}")
    print(f"{'='*56}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Auto-recovery for Week 7 agent")
    parser.add_argument("--mode",     default="offline", choices=["offline", "online"],
                        help="offline: compute metrics directly | online: call /metrics endpoint")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL,
                        help="Agent base URL (online mode only)")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Print actions without executing them")
    args = parser.parse_args()

    print("TechCorp Week 7 — Auto Recovery")
    print(f"  Mode:    {args.mode}")
    print(f"  Dry run: {args.dry_run}")
    print()

    if args.mode == "online":
        metrics = fetch_metrics_online(args.base_url)
    else:
        os.environ.setdefault("DOCS_PATH", CLEANED_CORPUS)
        metrics = fetch_metrics_offline()

    if not metrics:
        print("ERROR: Could not fetch metrics — aborting.")
        sys.exit(1)

    alerting = _alerting_metrics(metrics)
    print(f"  Overall status: {metrics.get('status', 'unknown')}")
    print(f"  Alerting:       {alerting if alerting else 'none'}")
    print()

    results = decide_and_act(metrics, dry_run=args.dry_run)
    print_summary(results)
