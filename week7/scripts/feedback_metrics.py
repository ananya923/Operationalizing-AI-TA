"""
feedback_metrics.py — Week 7: Analyse the feedback correction log.

Reads feedback.jsonl (written by app/feedback.py) and produces:
  - Correction rate over time (accepted / total per hour)
  - Breakdown by intent: which query domains get corrected most
  - Breakdown by role: which roles are submitting corrections
  - Top offending queries: queries corrected most often
  - Correction velocity: is the rate increasing or decreasing?
  - Alert: flags if correction rate exceeds threshold (signals accuracy regression)

Usage:
    python week7/scripts/feedback_metrics.py

    # Explicit log path + output:
    python week7/scripts/feedback_metrics.py \\
        --log     week7/feedback.jsonl \\
        --output  week7/feedback_metrics_report.json
"""

import sys
import os
import json
import time
import argparse
from collections import defaultdict
from datetime import datetime, timezone, timedelta

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
WEEK7_APP = os.path.join(REPO_ROOT, "week7", "app")
sys.path.insert(0, WEEK7_APP)

DEFAULT_LOG    = os.path.join(REPO_ROOT, "week7", "feedback.jsonl")
DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), "..", "feedback_metrics_report.json")

# Alert threshold: if >20% of submissions in the last hour are accepted corrections,
# something is systematically wrong with agent answers.
CORRECTION_RATE_ALERT_THRESHOLD = 0.20

# ---------------------------------------------------------------------------
# Load log
# ---------------------------------------------------------------------------

def load_log(log_path: str) -> list[dict]:
    if not os.path.exists(log_path):
        return []
    records = []
    with open(log_path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def _iso_to_dt(iso: str) -> datetime:
    try:
        return datetime.fromisoformat(iso)
    except (ValueError, TypeError):
        return datetime.min.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Metric computations
# ---------------------------------------------------------------------------

def correction_rate_overall(records: list[dict]) -> dict:
    """Overall accepted / total correction rate."""
    total    = len(records)
    accepted = sum(1 for r in records if r.get("accepted"))
    rejected = total - accepted
    rate     = accepted / total if total else 0.0
    return {
        "total":    total,
        "accepted": accepted,
        "rejected": rejected,
        "rate":     round(rate, 4),
        "status":   "alert" if rate >= CORRECTION_RATE_ALERT_THRESHOLD else "ok",
        "threshold": CORRECTION_RATE_ALERT_THRESHOLD,
    }


def correction_rate_by_window(records: list[dict], window_hours: int = 1) -> list[dict]:
    """
    Bucket accepted corrections into hourly windows.
    Returns list of {window_start, accepted, total, rate} dicts, sorted by time.
    """
    if not records:
        return []

    # Find time range
    dts = [_iso_to_dt(r.get("timestamp", "")) for r in records]
    dts = [dt for dt in dts if dt != datetime.min.replace(tzinfo=timezone.utc)]
    if not dts:
        return []

    min_dt = min(dts).replace(minute=0, second=0, microsecond=0)
    max_dt = max(dts).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    buckets: dict[datetime, dict] = {}
    current = min_dt
    while current <= max_dt:
        buckets[current] = {"accepted": 0, "total": 0}
        current += timedelta(hours=window_hours)

    for r, dt in zip(records, [_iso_to_dt(r.get("timestamp", "")) for r in records]):
        if dt == datetime.min.replace(tzinfo=timezone.utc):
            continue
        bucket = dt.replace(minute=0, second=0, microsecond=0)
        if bucket in buckets:
            buckets[bucket]["total"] += 1
            if r.get("accepted"):
                buckets[bucket]["accepted"] += 1

    result = []
    for window_start in sorted(buckets):
        b = buckets[window_start]
        rate = b["accepted"] / b["total"] if b["total"] else 0.0
        result.append({
            "window_start": window_start.isoformat(),
            "accepted":     b["accepted"],
            "total":        b["total"],
            "rate":         round(rate, 4),
            "status":       "alert" if rate >= CORRECTION_RATE_ALERT_THRESHOLD else "ok",
        })

    return result


def breakdown_by_intent(records: list[dict]) -> dict:
    """Accepted + total corrections per intent domain."""
    by_intent: dict[str, dict] = defaultdict(lambda: {"total": 0, "accepted": 0, "queries": set()})
    for r in records:
        intent = r.get("intent", "unknown")
        by_intent[intent]["total"] += 1
        if r.get("accepted"):
            by_intent[intent]["accepted"] += 1
            by_intent[intent]["queries"].add(r.get("query", "").strip().lower())

    result = {}
    for intent, data in sorted(by_intent.items(), key=lambda x: -x[1]["accepted"]):
        result[intent] = {
            "total":              data["total"],
            "accepted":           data["accepted"],
            "unique_queries":     len(data["queries"]),
            "acceptance_rate":    round(data["accepted"] / data["total"], 3) if data["total"] else 0.0,
        }
    return result


def breakdown_by_role(records: list[dict]) -> dict:
    """Accepted + total corrections per user role."""
    by_role: dict[str, dict] = defaultdict(lambda: {"total": 0, "accepted": 0, "rejected": 0})
    for r in records:
        role = r.get("user_role", "unknown")
        by_role[role]["total"] += 1
        if r.get("accepted"):
            by_role[role]["accepted"] += 1
        else:
            by_role[role]["rejected"] += 1

    result = {}
    for role, data in sorted(by_role.items(), key=lambda x: -x[1]["total"]):
        result[role] = {
            **data,
            "acceptance_rate": round(data["accepted"] / data["total"], 3) if data["total"] else 0.0,
        }
    return result


def top_corrected_queries(records: list[dict], n: int = 10) -> list[dict]:
    """
    Queries with the most accepted corrections.
    A high correction count on the same query signals a persistent retrieval problem.
    """
    accepted = [r for r in records if r.get("accepted")]
    query_data: dict[str, dict] = defaultdict(
        lambda: {"count": 0, "intents": set(), "corrections": []}
    )
    for r in accepted:
        q = r.get("query", "").strip()
        query_data[q]["count"] += 1
        query_data[q]["intents"].add(r.get("intent", ""))
        query_data[q]["corrections"].append(r.get("correct_answer", "")[:100])

    ranked = sorted(query_data.items(), key=lambda x: -x[1]["count"])[:n]
    return [
        {
            "query":       q[:100],
            "count":       data["count"],
            "intents":     list(data["intents"]),
            "sample_corrections": data["corrections"][:3],
        }
        for q, data in ranked
    ]


def rejection_reasons(records: list[dict]) -> dict:
    """Count rejected corrections grouped by rejection reason."""
    rejected = [r for r in records if not r.get("accepted")]
    reason_counts: dict[str, int] = defaultdict(int)
    for r in rejected:
        reason = r.get("rejection_reason") or "unknown"
        # Bucket into categories
        if "not permitted" in reason.lower():
            reason_counts["role_not_permitted"] += 1
        elif "cannot correct" in reason.lower():
            reason_counts["wrong_domain"] += 1
        elif "identical" in reason.lower():
            reason_counts["duplicate_content"] += 1
        elif "already been submitted" in reason.lower():
            reason_counts["duplicate_submission"] += 1
        else:
            reason_counts["other"] += 1
    return dict(reason_counts)


def correction_velocity(windows: list[dict]) -> dict:
    """
    Is the correction rate trending up or down?
    Compares the last 3 windows vs the 3 windows before that.
    """
    if len(windows) < 2:
        return {"trend": "insufficient_data", "recent_avg": 0.0, "prior_avg": 0.0}

    rates = [w["rate"] for w in windows if w["total"] > 0]
    if len(rates) < 2:
        return {"trend": "insufficient_data", "recent_avg": 0.0, "prior_avg": 0.0}

    mid = len(rates) // 2
    prior_avg  = sum(rates[:mid]) / mid
    recent_avg = sum(rates[mid:]) / (len(rates) - mid)

    if recent_avg > prior_avg * 1.1:
        trend = "increasing"    # correction rate is going up — possible regression
    elif recent_avg < prior_avg * 0.9:
        trend = "decreasing"    # corrections are declining — agent improving
    else:
        trend = "stable"

    return {
        "trend":       trend,
        "recent_avg":  round(recent_avg, 4),
        "prior_avg":   round(prior_avg, 4),
        "delta":       round(recent_avg - prior_avg, 4),
    }


# ---------------------------------------------------------------------------
# Print report
# ---------------------------------------------------------------------------

def print_report(report: dict):
    overall  = report["overall"]
    by_intent = report["by_intent"]
    by_role   = report["by_role"]
    top_q     = report["top_corrected_queries"]
    velocity  = report["velocity"]
    reasons   = report["rejection_reasons"]

    print("\n" + "=" * 64)
    print("  FEEDBACK METRICS REPORT")
    print("=" * 64)

    status_tag = "[ALERT]" if overall["status"] == "alert" else "[OK]"
    print(f"\n  Overall correction rate: {overall['rate']:.1%}  {status_tag}")
    print(f"  Total submissions:  {overall['total']}")
    print(f"  Accepted:           {overall['accepted']}")
    print(f"  Rejected:           {overall['rejected']}")
    print(f"  Alert threshold:    {overall['threshold']:.0%}")

    print(f"\n  Trend: {velocity['trend'].upper()}")
    print(f"  Recent avg rate: {velocity['recent_avg']:.1%}  |  Prior avg rate: {velocity['prior_avg']:.1%}")

    if by_intent:
        print(f"\n  Corrections by intent:")
        print(f"  {'Intent':<12} {'Total':>7} {'Accepted':>9} {'Rate':>7}")
        print("  " + "-" * 38)
        for intent, data in by_intent.items():
            print(f"  {intent:<12} {data['total']:>7} {data['accepted']:>9} {data['acceptance_rate']:>6.1%}")

    if by_role:
        print(f"\n  Corrections by role:")
        print(f"  {'Role':<12} {'Total':>7} {'Accepted':>9} {'Rejected':>9}")
        print("  " + "-" * 42)
        for role, data in by_role.items():
            print(f"  {role:<12} {data['total']:>7} {data['accepted']:>9} {data['rejected']:>9}")

    if reasons:
        print(f"\n  Rejection reasons:")
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"    {reason:<28} {count}")

    if top_q:
        print(f"\n  Top corrected queries:")
        for i, q in enumerate(top_q[:5], 1):
            print(f"    {i}. [{q['count']}x] {q['query'][:65]}")
            if q["sample_corrections"]:
                print(f"       → {q['sample_corrections'][0][:70]}")

    print("=" * 64)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyse feedback correction log")
    parser.add_argument("--log",    default=DEFAULT_LOG,    help="Path to feedback.jsonl")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Path to save JSON report")
    args = parser.parse_args()

    print(f"Loading feedback log: {args.log}")
    records = load_log(args.log)

    if not records:
        print("No feedback records found. Submit some corrections first via POST /feedback.")
        print("Generating report with empty data for structure reference.")

    overall  = correction_rate_overall(records)
    windows  = correction_rate_by_window(records)
    velocity = correction_velocity(windows)

    report = {
        "generated":              time.strftime("%Y-%m-%dT%H:%M:%S"),
        "log_path":               args.log,
        "overall":                overall,
        "by_intent":              breakdown_by_intent(records),
        "by_role":                breakdown_by_role(records),
        "top_corrected_queries":  top_corrected_queries(records),
        "rejection_reasons":      rejection_reasons(records),
        "hourly_windows":         windows,
        "velocity":               velocity,
    }

    print_report(report)

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport saved to: {args.output}")
