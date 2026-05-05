"""
monitoring.py — Compute and expose the 8 required metrics.

Metrics:
1. Answer completeness     (% of queries agent fully answers)
2. Access denials          (count by field/category)
3. Response latency        (p50, p95, p99 by role)
4. Doc retrieval coverage  (% of relevant docs retrieved post-filtering)
5. Access denial rate      (% of requests hitting at least one denial)
6. Audit log volume        (MB/day)
7. Answer consistency      (same role + query → same answer)
8. PII exposure            (regex check for SSN/salary patterns in responses)
"""

import re
import hashlib
from collections import defaultdict
from statistics import median, quantiles

import audit

# ---------------------------------------------------------------------------
# In-memory state (accumulated across requests in this process lifetime)
# ---------------------------------------------------------------------------

# Per-query records: list of dicts with keys:
#   role, latency_ms, docs_retrieved, docs_denied, answer, query, had_pii
_query_records: list[dict] = []

# Consistency tracking: (role, query_hash) → set of answer_hashes
_consistency_map: dict[tuple, set] = defaultdict(set)

# PII pattern definitions
_PII_PATTERNS = {
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "salary": re.compile(r"\$[\d,]{4,}"),  # dollar amounts ≥ $1000
}

# ---------------------------------------------------------------------------
# Metric definitions: baseline and alert threshold
# ---------------------------------------------------------------------------

METRIC_DEFINITIONS = {
    "answer_completeness": {
        "description": "% of queries where agent returned a non-empty, non-fallback answer",
        "baseline": 0.90,
        "alert_below": 0.75,
        "unit": "ratio",
    },
    "access_denials_by_category": {
        "description": "Count of document-level denials broken down by category",
        "baseline": "varies",
        "alert_above": 50,
        "unit": "count",
    },
    "response_latency_ms": {
        "description": "p50 / p95 / p99 response latency in ms, broken down by role",
        "baseline": {"p50": 500, "p95": 2000, "p99": 5000},
        "alert_above": {"p95": 5000},
        "unit": "ms",
    },
    "doc_retrieval_coverage": {
        "description": "% of retrieved docs that were allowed through access control",
        "baseline": 0.80,
        "alert_below": 0.50,
        "unit": "ratio",
    },
    "access_denial_rate": {
        "description": "% of requests that triggered at least one document denial",
        "baseline": 0.10,
        "alert_above": 0.40,
        "unit": "ratio",
    },
    "audit_log_volume_mb": {
        "description": "Current size of audit.log in MB",
        "baseline": "< 10 MB/day",
        "alert_above": 100,
        "unit": "MB",
    },
    "answer_consistency": {
        "description": "% of (role, query) pairs that always return the same answer",
        "baseline": 1.00,
        "alert_below": 0.90,
        "unit": "ratio",
    },
    "pii_exposure_rate": {
        "description": "% of responses containing SSN or salary patterns",
        "baseline": 0.00,
        "alert_above": 0.01,
        "unit": "ratio",
    },
}

# ---------------------------------------------------------------------------
# PII check (also called from agent.py)
# ---------------------------------------------------------------------------


def check_pii(text: str) -> list[str]:
    """
    Scan text for PII patterns.
    Returns list of matched pattern names (empty if clean).
    """
    found = []
    for name, pattern in _PII_PATTERNS.items():
        if pattern.search(text):
            found.append(name)
    return found


# ---------------------------------------------------------------------------
# Record a query (called from agent.py after each request)
# ---------------------------------------------------------------------------


def record_query(
    role: str,
    latency_ms: float,
    docs_retrieved: int,
    docs_denied: int,
    answer: str,
    query: str,
    had_pii: bool,
):
    """Append a query record to in-memory state for metric computation."""
    _query_records.append(
        {
            "role": role,
            "latency_ms": latency_ms,
            "docs_retrieved": docs_retrieved,
            "docs_denied": docs_denied,
            "answer": answer,
            "query": query,
            "had_pii": had_pii,
        }
    )

    # Track consistency: hash (role, query) → hash of answer
    key = (role, hashlib.md5(query.strip().lower().encode()).hexdigest())
    answer_hash = hashlib.md5(answer.strip().encode()).hexdigest()
    _consistency_map[key].add(answer_hash)


# ---------------------------------------------------------------------------
# Compute each metric
# ---------------------------------------------------------------------------


def _metric_answer_completeness() -> dict:
    if not _query_records:
        return {"value": None, "status": "no_data"}
    fallback_phrases = [
        "unable to connect",
        "no relevant data",
        "an error occurred",
        "i don't know",
        "i cannot answer",
    ]
    complete = sum(
        1
        for r in _query_records
        if r["answer"].strip()
        and not any(p in r["answer"].lower() for p in fallback_phrases)
    )
    value = complete / len(_query_records)
    defn = METRIC_DEFINITIONS["answer_completeness"]
    return {
        "value": round(value, 4),
        "baseline": defn["baseline"],
        "alert_below": defn["alert_below"],
        "status": "ok" if value > defn["alert_below"] else "alert",
        "unit": defn["unit"],
        "description": defn["description"],
    }


def _metric_access_denials() -> dict:
    records = audit.read_audit_log()
    denials = [
        r
        for r in records
        if r.get("event") == "document_access" and r.get("decision") == "denied"
    ]
    by_category: dict[str, int] = defaultdict(int)
    for r in denials:
        by_category[r.get("doc_category", "unknown")] += 1
    total = len(denials)
    defn = METRIC_DEFINITIONS["access_denials_by_category"]
    return {
        "value": dict(by_category),
        "total": total,
        "baseline": defn["baseline"],
        "alert_above": defn["alert_above"],
        "status": "ok" if total <= defn["alert_above"] else "alert",
        "unit": defn["unit"],
        "description": defn["description"],
    }


def _metric_response_latency() -> dict:
    defn = METRIC_DEFINITIONS["response_latency_ms"]
    if not _query_records:
        return {"value": None, "status": "no_data", **defn}

    by_role: dict[str, list[float]] = defaultdict(list)
    for r in _query_records:
        by_role[r["role"]].append(r["latency_ms"])

    result = {}
    overall_status = "ok"
    for role, latencies in by_role.items():
        latencies_sorted = sorted(latencies)
        n = len(latencies_sorted)
        p50 = median(latencies_sorted)
        p95 = quantiles(latencies_sorted, n=20)[18] if n >= 2 else latencies_sorted[-1]
        p99 = quantiles(latencies_sorted, n=100)[98] if n >= 2 else latencies_sorted[-1]
        status = "alert" if p95 > defn["alert_above"]["p95"] else "ok"
        if status == "alert":
            overall_status = "alert"
        result[role] = {
            "p50_ms": round(p50, 1),
            "p95_ms": round(p95, 1),
            "p99_ms": round(p99, 1),
            "n": n,
            "status": status,
        }

    return {
        "value": result,
        "baseline": defn["baseline"],
        "alert_above": defn["alert_above"],
        "status": overall_status,
        "unit": defn["unit"],
        "description": defn["description"],
    }


def _metric_doc_retrieval_coverage() -> dict:
    defn = METRIC_DEFINITIONS["doc_retrieval_coverage"]
    if not _query_records:
        return {"value": None, "status": "no_data", **defn}

    total_retrieved = sum(r["docs_retrieved"] for r in _query_records)
    total_denied = sum(r["docs_denied"] for r in _query_records)
    total_attempted = total_retrieved + total_denied

    if total_attempted == 0:
        return {"value": None, "status": "no_data", **defn}

    value = total_retrieved / total_attempted
    return {
        "value": round(value, 4),
        "total_allowed": total_retrieved,
        "total_denied": total_denied,
        "baseline": defn["baseline"],
        "alert_below": defn["alert_below"],
        "status": "ok" if value >= defn["alert_below"] else "alert",
        "unit": defn["unit"],
        "description": defn["description"],
    }


def _metric_access_denial_rate() -> dict:
    defn = METRIC_DEFINITIONS["access_denial_rate"]
    if not _query_records:
        return {"value": None, "status": "no_data", **defn}

    with_denials = sum(1 for r in _query_records if r["docs_denied"] > 0)
    value = with_denials / len(_query_records)
    return {
        "value": round(value, 4),
        "baseline": defn["baseline"],
        "alert_above": defn["alert_above"],
        "status": "ok" if value <= defn["alert_above"] else "alert",
        "unit": defn["unit"],
        "description": defn["description"],
    }


def _metric_audit_log_volume() -> dict:
    defn = METRIC_DEFINITIONS["audit_log_volume_mb"]
    value = audit.get_log_size_mb()
    return {
        "value": round(value, 4),
        "baseline": defn["baseline"],
        "alert_above": defn["alert_above"],
        "status": "ok" if value <= defn["alert_above"] else "alert",
        "unit": defn["unit"],
        "description": defn["description"],
    }


def _metric_answer_consistency() -> dict:
    defn = METRIC_DEFINITIONS["answer_consistency"]
    if not _consistency_map:
        return {"value": None, "status": "no_data", **defn}

    # A (role, query) pair is consistent if it always produced the same answer
    consistent = sum(1 for answers in _consistency_map.values() if len(answers) == 1)
    inconsistent = sum(1 for answers in _consistency_map.values() if len(answers) > 1)
    total = consistent + inconsistent
    value = consistent / total if total else 1.0

    return {
        "value": round(value, 4),
        "consistent_pairs": consistent,
        "inconsistent_pairs": inconsistent,
        "baseline": defn["baseline"],
        "alert_below": defn["alert_below"],
        "status": "ok" if value >= defn["alert_below"] else "alert",
        "unit": defn["unit"],
        "description": defn["description"],
    }


def _metric_pii_exposure() -> dict:
    defn = METRIC_DEFINITIONS["pii_exposure_rate"]
    if not _query_records:
        return {"value": None, "status": "no_data", **defn}

    exposed = sum(1 for r in _query_records if r["had_pii"])
    value = exposed / len(_query_records)
    return {
        "value": round(value, 4),
        "exposed": exposed,
        "total": len(_query_records),
        "baseline": defn["baseline"],
        "alert_above": defn["alert_above"],
        "status": "ok" if value <= defn["alert_above"] else "alert",
        "unit": defn["unit"],
        "description": defn["description"],
    }


# ---------------------------------------------------------------------------
# Public API — called by /metrics endpoint in agent.py
# ---------------------------------------------------------------------------


def get_metrics() -> dict:
    """Compute and return all 8 metrics with baselines and alert statuses."""
    metrics = {
        "answer_completeness": _metric_answer_completeness(),
        "access_denials": _metric_access_denials(),
        "response_latency": _metric_response_latency(),
        "doc_retrieval_coverage": _metric_doc_retrieval_coverage(),
        "access_denial_rate": _metric_access_denial_rate(),
        "audit_log_volume_mb": _metric_audit_log_volume(),
        "answer_consistency": _metric_answer_consistency(),
        "pii_exposure_rate": _metric_pii_exposure(),
    }

    # Overall system status: alert if any metric is alerting
    overall = (
        "alert" if any(m.get("status") == "alert" for m in metrics.values()) else "ok"
    )

    return {
        "status": overall,
        "total_queries": len(_query_records),
        "metrics": metrics,
    }
