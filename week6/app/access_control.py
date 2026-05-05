"""
access_control.py — Document-level and field-level access control.

Loads access_control.json at startup and exposes two functions:
- filter_documents(): remove docs a role cannot see entirely
- redact_fields():    scrub sensitive fields from allowed documents
"""

import json
import os
from typing import Optional

# ---------------------------------------------------------------------------
# Load access_control.json at startup
# ---------------------------------------------------------------------------

_AC_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "data",
    "raw",
    "techcorp",
    "access_control.json",
)

with open(_AC_PATH) as f:
    _ACCESS_CONTROL = json.load(f)

# Map each category to the permission key that grants access to it
_CATEGORY_PERMISSION = {
    "Finance": "view_financial_reports",
    "HR": "view_hr_data",
    "Engineering": "view_engineering_budget",
    "Internal": "view_project_details",
    "Compliance": "view_project_details",
    "Sales": "view_project_details",
    "Operations": "view_project_details",
    "Company": None,
}

# Fields that are always redacted unless the role explicitly allows them
_SENSITIVE_FIELDS = {
    "salary": "view_other_salaries",
    "ssn": "view_hr_data",
    "compensation": "view_other_salaries",
    "base_salary": "view_other_salaries",
}

# Regex patterns that indicate sensitive content in free text
import re

_PII_PATTERNS = [
    re.compile(r"\$[\d,]+"),  # dollar amounts
    re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),  # SSN pattern
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_permissions(role: str) -> dict:
    """Return the permission dict for a role, defaulting to engineer if unknown."""
    roles = _ACCESS_CONTROL.get("roles", {})
    return roles.get(role, roles.get("engineer", {})).get("permissions", {})


def _role_can_see_category(role: str, category: str) -> bool:
    perms = _get_permissions(role)
    permission_key = _CATEGORY_PERMISSION.get(category)
    if permission_key is None:
        return True  # no restriction — public or unknown category
    return perms.get(permission_key, False)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def filter_documents(docs: list[dict], role: str) -> tuple[list[dict], list[dict]]:
    """
    Filter out documents the role cannot access entirely.

    Args:
        docs: list of document dicts (from embedding.search or similar)
        role: the requesting user's role

    Returns:
        allowed:  documents the role can see
        denied:   documents that were filtered out
    """
    allowed, denied = [], []
    for doc in docs:
        category = doc.get("category", "")
        if _role_can_see_category(role, category):
            allowed.append(doc)
        else:
            denied.append(doc)
    return allowed, denied


def redact_fields(doc: dict, role: str) -> dict:
    """
    Redact sensitive fields from a document the role is allowed to see.

    Scrubs both top-level dict keys and inline content that matches
    sensitive field names or PII patterns.

    Args:
        doc:  a single document dict
        role: the requesting user's role

    Returns:
        A copy of the document with forbidden fields redacted.
    """
    perms = _get_permissions(role)
    redacted = dict(doc)

    # Redact top-level sensitive keys
    for field, permission_key in _SENSITIVE_FIELDS.items():
        if field in redacted and not perms.get(permission_key, False):
            redacted[field] = "[REDACTED]"

    # Redact salary/SSN patterns from free-text content if role lacks permission
    if not perms.get("view_other_salaries", False):
        content = redacted.get("content", "")
        content = re.sub(
            r"\b(salary|compensation|base pay)[^\n]*\$[\d,]+",
            lambda m: m.group(0).split("$")[0] + "$[REDACTED]",
            content,
            flags=re.IGNORECASE,
        )
        redacted["content"] = content

    if not perms.get("view_hr_data", False):
        content = redacted.get("content", "")
        content = re.sub(r"\b\d{3}-\d{2}-\d{4}\b", "[REDACTED-SSN]", content)
        redacted["content"] = content

    return redacted


def apply_access_control(docs: list[dict], role: str) -> tuple[list[dict], list[dict]]:
    """
    Convenience function: filter documents then redact fields on allowed ones.

    Returns:
        allowed:  filtered + redacted documents safe to return to this role
        denied:   documents blocked entirely
    """
    allowed, denied = filter_documents(docs, role)
    allowed = [redact_fields(doc, role) for doc in allowed]
    return allowed, denied
