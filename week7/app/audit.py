"""
audit.py — Access attempt logging for compliance.

Logs every document and field access attempt with:
- user_id, role, resource accessed, timestamp, allow/deny decision

Writes structured JSON lines to audit.log for easy parsing.
"""

import json
import logging
import os
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Audit log setup
# ---------------------------------------------------------------------------

_LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "audit.log")

# Use a dedicated logger separate from the app logger
_audit_logger = logging.getLogger("audit")
_audit_logger.setLevel(logging.INFO)
_audit_logger.propagate = False  # don't bleed into root logger

# Write one JSON object per line to audit.log
_handler = logging.FileHandler(_LOG_PATH)
_handler.setFormatter(logging.Formatter("%(message)s"))
_audit_logger.addHandler(_handler)


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


def _write(record: dict):
    """Serialize a record to a JSON line in the audit log."""
    record["timestamp"] = datetime.now(timezone.utc).isoformat()
    _audit_logger.info(json.dumps(record))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def log_document_access(
    user_id: str,
    role: str,
    doc_id: str,
    doc_category: str,
    decision: str,
    reason: str = "",
):
    """Log a document-level access attempt."""
    _write(
        {
            "event": "document_access",
            "user_id": user_id,
            "role": role,
            "doc_id": doc_id,
            "doc_category": doc_category,
            "decision": decision,
            "reason": reason,
        }
    )


def read_audit_log() -> list[dict]:
    """Read all audit log entries as a list of dicts."""
    if not os.path.exists(_LOG_PATH):
        return []
    records = []
    with open(_LOG_PATH) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records


def get_log_size_mb() -> float:
    """Return current audit log size in MB."""
    if not os.path.exists(_LOG_PATH):
        return 0.0
    return os.path.getsize(_LOG_PATH) / (1024 * 1024)
