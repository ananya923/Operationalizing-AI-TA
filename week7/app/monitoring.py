"""
monitoring.py — Week 7: Extended monitoring with 15 metrics total.

Keeps all 8 Week 6 metrics (access control, PII, latency, consistency…)
and adds 7 new Week 7 metrics focused on cost, corpus health, and feedback:

Week 6 metrics (unchanged):
  1.  answer_completeness        % of queries fully answered
  2.  access_denials             count by category
  3.  response_latency           p50/p95/p99 by role
  4.  doc_retrieval_coverage     % of docs allowed through access control
  5.  access_denial_rate         % of requests hitting a denial
  6.  audit_log_volume_mb        current audit log size
  7.  answer_consistency         same query → same answer
  8.  pii_exposure_rate          SSN/salary in responses

Week 7 metrics (new):
  9.  cost_per_query             avg USD/query with alert threshold
  10. latency_percentiles_w7     p50/p95/p99 overall (not split by role)
  11. accuracy_rate              keyword-hit proxy for correctness
  12. feedback_correction_rate   % of queries corrected in last 60 min
  13. corpus_freshness           avg doc age + stale doc count
  14. model_cost_latency_corr    correlation between token count and latency
  15. system_health              error rate, timeout rate, retry signals
"""

import re
import os
import json
import hashlib
import time
from collections import defaultdict
from statistics import median, quantiles, mean, stdev

import audit

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

# Extended record: now includes cost_usd, tokens_input, correct (optional)
_query_records: list[dict] = []

# Consistency tracking (week 6)
_consistency_map: dict[tuple, set] = defaultdict(set)

# Error/timeout counters (week 7)
_error_count:   int = 0
_timeout_count: int = 0

# PII patterns (week 6)
_PII_PATTERNS = {
    "ssn":    re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "salary": re.compile(r"\$[\d,]{4,}"),
}

# ---------------------------------------------------------------------------
# Metric definitions — baselines and alert thresholds
# ---------------------------------------------------------------------------

METRIC_DEFINITIONS = {
    # ── Week 6 ──────────────────────────────────────────────────────────────
    "answer_completeness": {
        "description": "% of queries where agent returned a non-empty, non-fallback answer",
        "baseline": 0.90, "alert_below": 0.75, "unit": "ratio",
    },
    "access_denials_by_category": {
        "description": "Count of document-level denials broken down by category",
        "baseline": "varies", "alert_above": 50, "unit": "count",
    },
    "response_latency_ms": {
        "description": "p50/p95/p99 response latency in ms, by role",
        "baseline": {"p50": 500, "p95": 2000, "p99": 5000},
        "alert_above": {"p95": 5000}, "unit": "ms",
    },
    "doc_retrieval_coverage": {
        "description": "% of retrieved docs allowed through access control",
        "baseline": 0.80, "alert_below": 0.50, "unit": "ratio",
    },
    "access_denial_rate": {
        "description": "% of requests that triggered at least one document denial",
        "baseline": 0.10, "alert_above": 0.40, "unit": "ratio",
    },
    "audit_log_volume_mb": {
        "description": "Current size of audit.log in MB",
        "baseline": "< 10 MB/day", "alert_above": 100, "unit": "MB",
    },
    "answer_consistency": {
        "description": "% of (role, query) pairs that always return the same answer",
        "baseline": 1.00, "alert_below": 0.90, "unit": "ratio",
    },
    "pii_exposure_rate": {
        "description": "% of responses containing SSN or salary patterns",
        "baseline": 0.00, "alert_above": 0.01, "unit": "ratio",
    },
    # ── Week 7 ──────────────────────────────────────────────────────────────
    "cost_per_query": {
        "description": "Average cost in USD per query; alerts when 3x above baseline",
        "baseline": 0.00027,           # from Week 6 evaluation on clean corpus
        "alert_above": 0.00081,        # 3x baseline — matches CFO complaint trigger
        "unit": "USD",
    },
    "latency_percentiles_w7": {
        "description": "Overall p50/p95/p99 latency (not split by role); tracks corpus-driven degradation",
        "baseline": {"p50": 500, "p95": 2000, "p99": 5000},
        "alert_above": {"p95": 3000},  # tighter than week6 — catches corpus bloat earlier
        "unit": "ms",
    },
    "accuracy_rate": {
        "description": "% of queries where answer contains all expected keywords (proxy for correctness)",
        "baseline": 0.80, "alert_below": 0.60, "unit": "ratio",
    },
    "feedback_correction_rate": {
        "description": "% of queries receiving an accepted user correction in the last 60 min",
        "baseline": 0.00, "alert_above": 0.20, "unit": "ratio",
    },
    "corpus_freshness": {
        "description": "Avg document age (days) and stale doc count (>180 days old)",
        "baseline": {"avg_age_days": 60, "stale_count": 5},
        "alert_above": {"avg_age_days": 180, "stale_count": 20},
        "unit": "days / count",
    },
    "model_cost_latency_corr": {
        "description": "Pearson correlation between input token count and latency — high correlation signals retrieval bloat",
        "baseline": 0.30,    # low correlation expected in healthy system
        "alert_above": 0.75, # strong correlation → long context driving latency
        "unit": "correlation",
    },
    "system_health": {
        "description": "Error rate and timeout rate across all requests",
        "baseline": {"error_rate": 0.00, "timeout_rate": 0.00},
        "alert_above": {"error_rate": 0.05, "timeout_rate": 0.02},
        "unit": "ratio",
    },
}

# ---------------------------------------------------------------------------
# PII check (used by agent.py)
# ---------------------------------------------------------------------------

def check_pii(text: str) -> list[str]:
    found = []
    for name, pattern in _PII_PATTERNS.items():
        if pattern.search(text):
            found.append(name)
    return found

# ---------------------------------------------------------------------------
# Record a query — extended signature for Week 7
# ---------------------------------------------------------------------------

def record_query(
    role:           str,
    latency_ms:     float,
    docs_retrieved: int,
    docs_denied:    int,
    answer:         str,
    query:          str,
    had_pii:        bool,
    cost_usd:       float = 0.0,    # NEW in Week 7
    tokens_input:   int   = 0,      # NEW in Week 7
    correct:        bool  = False,  # NEW in Week 7 — set when eval keywords available
    is_error:       bool  = False,  # NEW in Week 7
    is_timeout:     bool  = False,  # NEW in Week 7
):
    """Append a query record. Backwards-compatible with Week 6 callers."""
    global _error_count, _timeout_count

    _query_records.append({
        "role":           role,
        "latency_ms":     latency_ms,
        "docs_retrieved": docs_retrieved,
        "docs_denied":    docs_denied,
        "answer":         answer,
        "query":          query,
        "had_pii":        had_pii,
        "cost_usd":       cost_usd,
        "tokens_input":   tokens_input,
        "correct":        correct,
        "is_error":       is_error,
        "is_timeout":     is_timeout,
        "timestamp":      time.time(),
    })

    if is_error:
        _error_count += 1
    if is_timeout:
        _timeout_count += 1

    # Consistency tracking (week 6 — unchanged)
    key          = (role, hashlib.md5(query.strip().lower().encode()).hexdigest())
    answer_hash  = hashlib.md5(answer.strip().encode()).hexdigest()
    _consistency_map[key].add(answer_hash)


# ---------------------------------------------------------------------------
# Week 6 metric computations (unchanged)
# ---------------------------------------------------------------------------

def _metric_answer_completeness() -> dict:
    if not _query_records:
        return {"value": None, "status": "no_data"}
    fallback_phrases = ["unable to connect", "no relevant data",
                        "an error occurred", "i don't know", "i cannot answer"]
    complete = sum(
        1 for r in _query_records
        if r["answer"].strip()
        and not any(p in r["answer"].lower() for p in fallback_phrases)
    )
    value = complete / len(_query_records)
    defn  = METRIC_DEFINITIONS["answer_completeness"]
    return {
        "value": round(value, 4), "baseline": defn["baseline"],
        "alert_below": defn["alert_below"],
        "status": "ok" if value > defn["alert_below"] else "alert",
        "unit": defn["unit"], "description": defn["description"],
    }


def _metric_access_denials() -> dict:
    records = audit.read_audit_log()
    denials = [r for r in records
               if r.get("event") == "document_access" and r.get("decision") == "denied"]
    by_category: dict[str, int] = defaultdict(int)
    for r in denials:
        by_category[r.get("doc_category", "unknown")] += 1
    total = len(denials)
    defn  = METRIC_DEFINITIONS["access_denials_by_category"]
    return {
        "value": dict(by_category), "total": total,
        "baseline": defn["baseline"], "alert_above": defn["alert_above"],
        "status": "ok" if total <= defn["alert_above"] else "alert",
        "unit": defn["unit"], "description": defn["description"],
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
    for role, lats in by_role.items():
        lats_sorted = sorted(lats)
        n   = len(lats_sorted)
        p50 = median(lats_sorted)
        p95 = quantiles(lats_sorted, n=20)[18] if n >= 2 else lats_sorted[-1]
        p99 = quantiles(lats_sorted, n=100)[98] if n >= 2 else lats_sorted[-1]
        status = "alert" if p95 > defn["alert_above"]["p95"] else "ok"
        if status == "alert":
            overall_status = "alert"
        result[role] = {"p50_ms": round(p50,1), "p95_ms": round(p95,1),
                        "p99_ms": round(p99,1), "n": n, "status": status}
    return {
        "value": result, "baseline": defn["baseline"],
        "alert_above": defn["alert_above"], "status": overall_status,
        "unit": defn["unit"], "description": defn["description"],
    }


def _metric_doc_retrieval_coverage() -> dict:
    defn = METRIC_DEFINITIONS["doc_retrieval_coverage"]
    if not _query_records:
        return {"value": None, "status": "no_data", **defn}
    total_retrieved = sum(r["docs_retrieved"] for r in _query_records)
    total_denied    = sum(r["docs_denied"]    for r in _query_records)
    total_attempted = total_retrieved + total_denied
    if total_attempted == 0:
        return {"value": None, "status": "no_data", **defn}
    value = total_retrieved / total_attempted
    return {
        "value": round(value, 4), "total_allowed": total_retrieved,
        "total_denied": total_denied, "baseline": defn["baseline"],
        "alert_below": defn["alert_below"],
        "status": "ok" if value >= defn["alert_below"] else "alert",
        "unit": defn["unit"], "description": defn["description"],
    }


def _metric_access_denial_rate() -> dict:
    defn = METRIC_DEFINITIONS["access_denial_rate"]
    if not _query_records:
        return {"value": None, "status": "no_data", **defn}
    with_denials = sum(1 for r in _query_records if r["docs_denied"] > 0)
    value = with_denials / len(_query_records)
    return {
        "value": round(value, 4), "baseline": defn["baseline"],
        "alert_above": defn["alert_above"],
        "status": "ok" if value <= defn["alert_above"] else "alert",
        "unit": defn["unit"], "description": defn["description"],
    }


def _metric_audit_log_volume() -> dict:
    defn  = METRIC_DEFINITIONS["audit_log_volume_mb"]
    value = audit.get_log_size_mb()
    return {
        "value": round(value, 4), "baseline": defn["baseline"],
        "alert_above": defn["alert_above"],
        "status": "ok" if value <= defn["alert_above"] else "alert",
        "unit": defn["unit"], "description": defn["description"],
    }


def _metric_answer_consistency() -> dict:
    defn = METRIC_DEFINITIONS["answer_consistency"]
    if not _consistency_map:
        return {"value": None, "status": "no_data", **defn}
    consistent   = sum(1 for answers in _consistency_map.values() if len(answers) == 1)
    inconsistent = sum(1 for answers in _consistency_map.values() if len(answers) > 1)
    total        = consistent + inconsistent
    value        = consistent / total if total else 1.0
    return {
        "value": round(value, 4), "consistent_pairs": consistent,
        "inconsistent_pairs": inconsistent, "baseline": defn["baseline"],
        "alert_below": defn["alert_below"],
        "status": "ok" if value >= defn["alert_below"] else "alert",
        "unit": defn["unit"], "description": defn["description"],
    }


def _metric_pii_exposure() -> dict:
    defn = METRIC_DEFINITIONS["pii_exposure_rate"]
    if not _query_records:
        return {"value": None, "status": "no_data", **defn}
    exposed = sum(1 for r in _query_records if r["had_pii"])
    value   = exposed / len(_query_records)
    return {
        "value": round(value, 4), "exposed": exposed,
        "total": len(_query_records), "baseline": defn["baseline"],
        "alert_above": defn["alert_above"],
        "status": "ok" if value <= defn["alert_above"] else "alert",
        "unit": defn["unit"], "description": defn["description"],
    }


# ---------------------------------------------------------------------------
# Week 7 metric computations (new)
# ---------------------------------------------------------------------------

def _metric_cost_per_query() -> dict:
    defn = METRIC_DEFINITIONS["cost_per_query"]
    costs = [r["cost_usd"] for r in _query_records if r.get("cost_usd", 0) > 0]
    if not costs:
        return {"value": None, "status": "no_data", **defn}
    value = mean(costs)
    return {
        "value":        round(value, 6),
        "avg_cost_usd": round(value, 6),
        "max_cost_usd": round(max(costs), 6),
        "total_cost_usd": round(sum(costs), 6),
        "n":            len(costs),
        "baseline":     defn["baseline"],
        "alert_above":  defn["alert_above"],
        "status":       "ok" if value <= defn["alert_above"] else "alert",
        "unit":         defn["unit"],
        "description":  defn["description"],
        "monthly_projection": round(value * 30_000, 2),
    }


def _metric_latency_percentiles_w7() -> dict:
    defn = METRIC_DEFINITIONS["latency_percentiles_w7"]
    lats = sorted(r["latency_ms"] for r in _query_records)
    if not lats:
        return {"value": None, "status": "no_data", **defn}
    n   = len(lats)
    p50 = median(lats)
    p95 = quantiles(lats, n=20)[18] if n >= 2 else lats[-1]
    p99 = quantiles(lats, n=100)[98] if n >= 2 else lats[-1]
    status = "alert" if p95 > defn["alert_above"]["p95"] else "ok"
    return {
        "value":       {"p50_ms": round(p50,1), "p95_ms": round(p95,1), "p99_ms": round(p99,1)},
        "n":           n,
        "baseline":    defn["baseline"],
        "alert_above": defn["alert_above"],
        "status":      status,
        "unit":        defn["unit"],
        "description": defn["description"],
    }


def _metric_accuracy_rate() -> dict:
    defn    = METRIC_DEFINITIONS["accuracy_rate"]
    records = [r for r in _query_records if "correct" in r]
    if not records:
        return {"value": None, "status": "no_data", **defn}
    correct = sum(1 for r in records if r.get("correct"))
    value   = correct / len(records)
    return {
        "value":       round(value, 4),
        "correct":     correct,
        "total":       len(records),
        "baseline":    defn["baseline"],
        "alert_below": defn["alert_below"],
        "status":      "ok" if value >= defn["alert_below"] else "alert",
        "unit":        defn["unit"],
        "description": defn["description"],
    }


def _metric_feedback_correction_rate() -> dict:
    defn = METRIC_DEFINITIONS["feedback_correction_rate"]
    try:
        from feedback import get_correction_rate
        value = get_correction_rate(window_minutes=60)
    except Exception:
        value = 0.0
    return {
        "value":       round(value, 4),
        "window_min":  60,
        "baseline":    defn["baseline"],
        "alert_above": defn["alert_above"],
        "status":      "ok" if value <= defn["alert_above"] else "alert",
        "unit":        defn["unit"],
        "description": defn["description"],
    }


def _metric_corpus_freshness() -> dict:
    """
    Load the active corpus and compute average doc age + stale doc count.
    Uses DOCS_PATH env var (same as embedding.py) so it inspects the live corpus.
    """
    defn = METRIC_DEFINITIONS["corpus_freshness"]
    docs_path = os.environ.get(
        "DOCS_PATH",
        os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw",
                     "techcorp", "documents_week7_cleaned.json"),
    )
    if not os.path.exists(docs_path):
        return {"value": None, "status": "no_data", **defn}

    try:
        with open(docs_path) as f:
            docs = json.load(f)
    except Exception:
        return {"value": None, "status": "no_data", **defn}

    from datetime import datetime, timezone
    now  = datetime.now(timezone.utc)
    ages = []
    stale_threshold_days = 180

    for doc in docs:
        raw = doc.get("last_updated", "")
        try:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_days = (now - dt).days
            ages.append(age_days)
        except (ValueError, TypeError):
            pass

    if not ages:
        return {"value": None, "status": "no_data", **defn}

    avg_age   = mean(ages)
    stale_cnt = sum(1 for a in ages if a > stale_threshold_days)
    status    = (
        "alert"
        if avg_age > defn["alert_above"]["avg_age_days"]
        or stale_cnt > defn["alert_above"]["stale_count"]
        else "ok"
    )
    return {
        "value": {"avg_age_days": round(avg_age, 1), "stale_count": stale_cnt},
        "total_docs":          len(docs),
        "stale_threshold_days": stale_threshold_days,
        "baseline":    defn["baseline"],
        "alert_above": defn["alert_above"],
        "status":      status,
        "unit":        defn["unit"],
        "description": defn["description"],
    }


def _metric_model_cost_latency_corr() -> dict:
    """
    Pearson correlation between tokens_input and latency_ms.
    High correlation (>0.75) means latency is being driven by context size —
    a signal that corpus bloat is inflating retrieval context.
    """
    defn    = METRIC_DEFINITIONS["model_cost_latency_corr"]
    records = [r for r in _query_records
               if r.get("tokens_input", 0) > 0 and r.get("latency_ms", 0) > 0]
    if len(records) < 5:
        return {"value": None, "status": "no_data", **defn}

    xs = [r["tokens_input"] for r in records]
    ys = [r["latency_ms"]   for r in records]
    n  = len(xs)
    mx, my = mean(xs), mean(ys)

    if n < 2:
        return {"value": None, "status": "no_data", **defn}

    try:
        sx, sy = stdev(xs), stdev(ys)
        if sx == 0 or sy == 0:
            corr = 0.0
        else:
            corr = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / ((n - 1) * sx * sy)
    except Exception:
        corr = 0.0

    return {
        "value":       round(corr, 4),
        "n":           n,
        "interpretation": (
            "strong — latency driven by context size (corpus bloat risk)" if corr > 0.75
            else "moderate" if corr > 0.40
            else "weak — latency not driven by context size"
        ),
        "baseline":    defn["baseline"],
        "alert_above": defn["alert_above"],
        "status":      "ok" if corr <= defn["alert_above"] else "alert",
        "unit":        defn["unit"],
        "description": defn["description"],
    }


def _metric_system_health() -> dict:
    defn  = METRIC_DEFINITIONS["system_health"]
    total = len(_query_records)
    if total == 0:
        return {"value": None, "status": "no_data", **defn}

    error_rate   = _error_count   / total
    timeout_rate = _timeout_count / total
    status = (
        "alert"
        if error_rate   > defn["alert_above"]["error_rate"]
        or timeout_rate > defn["alert_above"]["timeout_rate"]
        else "ok"
    )
    return {
        "value": {"error_rate": round(error_rate, 4), "timeout_rate": round(timeout_rate, 4)},
        "error_count":   _error_count,
        "timeout_count": _timeout_count,
        "total_queries": total,
        "baseline":      defn["baseline"],
        "alert_above":   defn["alert_above"],
        "status":        status,
        "unit":          defn["unit"],
        "description":   defn["description"],
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_metrics() -> dict:
    """Compute and return all 15 metrics with baselines and alert statuses."""
    metrics = {
        # Week 6
        "answer_completeness":      _metric_answer_completeness(),
        "access_denials":           _metric_access_denials(),
        "response_latency":         _metric_response_latency(),
        "doc_retrieval_coverage":   _metric_doc_retrieval_coverage(),
        "access_denial_rate":       _metric_access_denial_rate(),
        "audit_log_volume_mb":      _metric_audit_log_volume(),
        "answer_consistency":       _metric_answer_consistency(),
        "pii_exposure_rate":        _metric_pii_exposure(),
        # Week 7
        "cost_per_query":           _metric_cost_per_query(),
        "latency_percentiles_w7":   _metric_latency_percentiles_w7(),
        "accuracy_rate":            _metric_accuracy_rate(),
        "feedback_correction_rate": _metric_feedback_correction_rate(),
        "corpus_freshness":         _metric_corpus_freshness(),
        "model_cost_latency_corr":  _metric_model_cost_latency_corr(),
        "system_health":            _metric_system_health(),
    }

    alerting = [name for name, m in metrics.items() if m.get("status") == "alert"]
    overall  = "alert" if alerting else "ok"

    return {
        "status":        overall,
        "total_queries": len(_query_records),
        "alerting":      alerting,
        "metrics":       metrics,
    }
