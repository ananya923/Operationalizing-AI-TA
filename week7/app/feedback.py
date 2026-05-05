"""
feedback.py — Week 7: Real-time user feedback and correction loop.

Provides two FastAPI endpoints to bolt onto the existing agent:

  POST /feedback          — submit a correction for a previous agent answer
  GET  /feedback/stats    — summary metrics over all collected corrections

Corrections are validated by role:
  - Only certain roles may correct certain intent domains
  - The correcting user must have provided a non-empty correction
  - Duplicate corrections (same query + same correction) are deduplicated

Validated corrections are appended to a JSONL log (one JSON object per line)
so they can be ingested by feedback_metrics.py and, eventually, used to
update the corpus or fine-tune the retrieval model.

Usage — mount these endpoints in agent.py:
    from feedback import router as feedback_router
    app.include_router(feedback_router)
"""

import json
import os
import time
import hashlib
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Storage path for the feedback log
# ---------------------------------------------------------------------------

FEEDBACK_LOG_PATH = os.environ.get(
    "FEEDBACK_LOG_PATH",
    os.path.join(os.path.dirname(__file__), "..", "feedback.jsonl"),
)

# ---------------------------------------------------------------------------
# Role → domain correction permissions
#
# Defines which roles are trusted to correct which query intents.
# "executive" can correct anything.
# ---------------------------------------------------------------------------

CORRECTION_PERMISSIONS: dict[str, list[str]] = {
    "hr":        ["benefits", "employee", "policy"],
    "finance":   ["expense", "policy"],
    "manager":   ["project", "employee", "policy"],
    "executive": ["benefits", "employee", "expense", "project", "policy"],
    "engineer":  [],   # engineers may not correct — read-only role
}

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class FeedbackRequest(BaseModel):
    query:          str
    wrong_answer:   str
    correct_answer: str
    intent:         str          # policy / employee / expense / project / benefits
    user_id:        str
    user_role:      str
    query_id:       Optional[str] = None   # optional reference to original query

    @field_validator("query", "correct_answer")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("must not be empty")
        return v.strip()

    @field_validator("intent")
    @classmethod
    def valid_intent(cls, v: str) -> str:
        allowed = {"policy", "employee", "expense", "project", "benefits"}
        if v not in allowed:
            raise ValueError(f"intent must be one of {sorted(allowed)}")
        return v

    @field_validator("user_role")
    @classmethod
    def valid_role(cls, v: str) -> str:
        if v not in CORRECTION_PERMISSIONS:
            raise ValueError(f"unknown role '{v}'")
        return v


class FeedbackResponse(BaseModel):
    accepted:     bool
    feedback_id:  str
    message:      str


class FeedbackStats(BaseModel):
    total_corrections:      int
    accepted:               int
    rejected:               int
    by_intent:              dict
    by_role:                dict
    most_corrected_queries: list[dict]
    recent:                 list[dict]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_correction(request: FeedbackRequest) -> tuple[bool, str]:
    """
    Check whether this role is allowed to correct this intent domain.

    Returns (accepted: bool, reason: str).
    """
    allowed_intents = CORRECTION_PERMISSIONS.get(request.user_role, [])

    if not allowed_intents:
        return False, (
            f"Role '{request.user_role}' is not permitted to submit corrections. "
            "Contact your manager or HR to flag incorrect answers."
        )

    if request.intent not in allowed_intents:
        return False, (
            f"Role '{request.user_role}' cannot correct '{request.intent}' answers. "
            f"Allowed domains: {sorted(allowed_intents)}."
        )

    if request.correct_answer.strip() == request.wrong_answer.strip():
        return False, "Correction is identical to the original answer — nothing to update."

    return True, "Correction accepted."


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def _feedback_id(request: FeedbackRequest) -> str:
    """Stable hash of (user_id, query, correct_answer) for deduplication."""
    payload = f"{request.user_id}|{request.query.strip().lower()}|{request.correct_answer.strip().lower()}"
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _append_feedback(record: dict) -> None:
    """Append one feedback record to the JSONL log (thread-safe via line append)."""
    os.makedirs(os.path.dirname(os.path.abspath(FEEDBACK_LOG_PATH)), exist_ok=True)
    with open(FEEDBACK_LOG_PATH, "a") as f:
        f.write(json.dumps(record) + "\n")


def _load_feedback() -> list[dict]:
    """Load all feedback records from the JSONL log."""
    if not os.path.exists(FEEDBACK_LOG_PATH):
        return []
    records = []
    with open(FEEDBACK_LOG_PATH) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def _is_duplicate(feedback_id: str) -> bool:
    """Return True if this exact correction has already been logged."""
    records = _load_feedback()
    return any(r.get("feedback_id") == feedback_id for r in records)


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/feedback", tags=["feedback"])


@router.post("", response_model=FeedbackResponse)
def submit_feedback(request: FeedbackRequest):
    """
    Submit a correction for an agent answer.

    Validates:
    - Role is permitted to correct this intent domain
    - Correction differs from the original answer
    - Not a duplicate of an already-logged correction

    Accepted corrections are appended to feedback.jsonl.
    """
    feedback_id = _feedback_id(request)

    # Deduplication check
    if _is_duplicate(feedback_id):
        return FeedbackResponse(
            accepted=False,
            feedback_id=feedback_id,
            message="This correction has already been submitted.",
        )

    # Role / domain validation
    accepted, reason = validate_correction(request)

    record = {
        "feedback_id":    feedback_id,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "accepted":       accepted,
        "user_id":        request.user_id,
        "user_role":      request.user_role,
        "intent":         request.intent,
        "query":          request.query,
        "wrong_answer":   request.wrong_answer,
        "correct_answer": request.correct_answer,
        "query_id":       request.query_id,
        "rejection_reason": None if accepted else reason,
    }

    _append_feedback(record)

    if accepted:
        logger.info(
            f"Feedback accepted | id={feedback_id} | role={request.user_role} "
            f"| intent={request.intent} | user={request.user_id}"
        )
    else:
        logger.warning(
            f"Feedback rejected | id={feedback_id} | role={request.user_role} "
            f"| intent={request.intent} | reason={reason}"
        )

    return FeedbackResponse(
        accepted=accepted,
        feedback_id=feedback_id,
        message=reason,
    )


@router.get("/stats", response_model=FeedbackStats)
def feedback_stats():
    """
    Return summary statistics over all collected feedback.

    Includes:
    - Total / accepted / rejected counts
    - Breakdown by intent and role
    - Top 5 most-corrected queries
    - 10 most recent corrections
    """
    records = _load_feedback()

    accepted_records = [r for r in records if r.get("accepted")]
    rejected_records = [r for r in records if not r.get("accepted")]

    # Breakdown by intent
    by_intent: dict[str, dict] = defaultdict(lambda: {"total": 0, "accepted": 0})
    for r in records:
        intent = r.get("intent", "unknown")
        by_intent[intent]["total"] += 1
        if r.get("accepted"):
            by_intent[intent]["accepted"] += 1

    # Breakdown by role
    by_role: dict[str, dict] = defaultdict(lambda: {"total": 0, "accepted": 0})
    for r in records:
        role = r.get("user_role", "unknown")
        by_role[role]["total"] += 1
        if r.get("accepted"):
            by_role[role]["accepted"] += 1

    # Most corrected queries (by accepted corrections)
    query_counts: dict[str, int] = defaultdict(int)
    for r in accepted_records:
        query_counts[r.get("query", "")] += 1
    most_corrected = sorted(query_counts.items(), key=lambda x: -x[1])[:5]
    most_corrected_queries = [
        {"query": q[:100], "correction_count": c} for q, c in most_corrected
    ]

    # 10 most recent accepted corrections
    recent = sorted(accepted_records, key=lambda r: r.get("timestamp", ""), reverse=True)[:10]
    recent_summary = [
        {
            "timestamp":      r.get("timestamp"),
            "feedback_id":    r.get("feedback_id"),
            "user_role":      r.get("user_role"),
            "intent":         r.get("intent"),
            "query":          r.get("query", "")[:80],
            "correct_answer": r.get("correct_answer", "")[:120],
        }
        for r in recent
    ]

    return FeedbackStats(
        total_corrections=len(records),
        accepted=len(accepted_records),
        rejected=len(rejected_records),
        by_intent=dict(by_intent),
        by_role=dict(by_role),
        most_corrected_queries=most_corrected_queries,
        recent=recent_summary,
    )


# ---------------------------------------------------------------------------
# Standalone helper — load all accepted corrections for corpus integration
# ---------------------------------------------------------------------------


def get_accepted_corrections() -> list[dict]:
    """
    Return all accepted corrections.
    Used by auto_recovery.py to decide whether to trigger corpus re-archival.
    """
    return [r for r in _load_feedback() if r.get("accepted")]


def get_correction_rate(window_minutes: int = 60) -> float:
    """
    Return the fraction of recent queries (within the last window_minutes)
    that received an accepted correction.

    Used by monitoring.py to compute the feedback correction rate metric.
    """
    records = _load_feedback()
    if not records:
        return 0.0

    cutoff = time.time() - window_minutes * 60
    recent = [
        r for r in records
        if r.get("accepted")
        and _iso_to_ts(r.get("timestamp", "")) >= cutoff
    ]
    total_recent = [
        r for r in records
        if _iso_to_ts(r.get("timestamp", "")) >= cutoff
    ]

    return len(recent) / len(total_recent) if total_recent else 0.0


def _iso_to_ts(iso: str) -> float:
    """Convert ISO 8601 string to unix timestamp. Returns 0.0 on parse error."""
    try:
        return datetime.fromisoformat(iso).timestamp()
    except (ValueError, TypeError):
        return 0.0
