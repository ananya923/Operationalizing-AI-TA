"""
guardrails.py — Access control, cost limits, and rate limiting for the TechCorp agent.

Three layers of protection:
1. Role-based tool access: certain tools only callable by certain roles
2. Cost limits: reject queries whose estimated cost exceeds a threshold
3. Rate limiting: max queries per minute per user
"""

import json
import os
import time
from collections import defaultdict
from typing import Optional

# ---------------------------------------------------------------------------
# Load access control config
# ---------------------------------------------------------------------------

_ACCESS_CONTROL_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "data",
    "raw",
    "techcorp",
    "access_control.json",
)
with open(_ACCESS_CONTROL_PATH) as f:
    _ACCESS_CONTROL = json.load(f)

ROLES = _ACCESS_CONTROL["roles"]
VALID_ROLES = set(ROLES.keys())  # engineer, manager, hr, finance, executive

# ---------------------------------------------------------------------------
# Tool → required permission mapping
# Each tool name maps to the permission key it requires from access_control.json
# ---------------------------------------------------------------------------

TOOL_PERMISSION_MAP = {
    "search_employees": "view_employee_directory",
    "get_employee": "view_employee_directory",
    "get_headcount": "view_employee_directory",
    "get_expenses_by_employee": "view_all_expenses",
    "get_expense_summary": "view_all_expenses",
    "get_total_expenses": "view_financial_reports",
    "get_project": "view_project_details",
    "get_project_budget_summary": "view_project_details",
    "get_benefits": "view_own_benefits",
    "get_pto_summary": "view_hr_data",
    "get_health_plan_distribution": "view_hr_data",
    "search_policies": "view_travel_policy",  # travel_policy is the most basic doc permission
}

# ---------------------------------------------------------------------------
# Cost tracking constants (approximate, based on typical small LLM pricing)
# Using $0.0002 per 1K tokens as a reasonable open-source LLM proxy cost estimate
# ---------------------------------------------------------------------------

COST_PER_1K_TOKENS = 0.0002  # USD per 1K tokens
MAX_COST_PER_QUERY = 0.01  # USD — reject queries estimated to exceed this
AVG_CHARS_PER_TOKEN = 4  # rough approximation

# ---------------------------------------------------------------------------
# Rate limiting state (in-memory, resets on restart)
# ---------------------------------------------------------------------------

MAX_QUERIES_PER_MINUTE = 10  # per user_id

_rate_limit_store: dict[str, list[float]] = defaultdict(
    list
)  # user_id → list of timestamps


# ---------------------------------------------------------------------------
# 1. Role validation
# ---------------------------------------------------------------------------


class AuthorizationError(Exception):
    """Raised when a role tries to use a tool it doesn't have access to."""

    pass


class RateLimitError(Exception):
    """Raised when a user exceeds the query rate limit."""

    pass


class CostLimitError(Exception):
    """Raised when a query's estimated cost exceeds the threshold."""

    pass


def validate_role(user_role: str) -> None:
    """Raise AuthorizationError if the role is not recognized."""
    if user_role not in VALID_ROLES:
        raise AuthorizationError(
            f"Unknown role '{user_role}'. Valid roles: {sorted(VALID_ROLES)}"
        )


def check_tool_access(tool_name: str, user_role: str) -> None:
    """
    Raise AuthorizationError if the role is not permitted to call this tool.

    Looks up the required permission for the tool and checks it against
    the role's permissions in access_control.json.
    """
    validate_role(user_role)

    required_permission = TOOL_PERMISSION_MAP.get(tool_name)
    if required_permission is None:
        # Tool not in map — allow by default (no special permission needed)
        return

    role_permissions = ROLES[user_role]["permissions"]
    if not role_permissions.get(required_permission, False):
        raise AuthorizationError(
            f"Role '{user_role}' does not have permission '{required_permission}' "
            f"required to use tool '{tool_name}'."
        )


def get_allowed_tools(user_role: str) -> list[str]:
    """Return the list of tools this role is allowed to call."""
    validate_role(user_role)
    return [
        tool
        for tool, perm in TOOL_PERMISSION_MAP.items()
        if ROLES[user_role]["permissions"].get(perm, False)
    ]


# ---------------------------------------------------------------------------
# 2. Cost estimation and enforcement
# ---------------------------------------------------------------------------


def estimate_tokens(text: str) -> int:
    """Estimate token count from character length."""
    return max(1, len(text) // AVG_CHARS_PER_TOKEN)


def estimate_cost(input_text: str, output_tokens: int = 200) -> float:
    """
    Estimate query cost in USD.

    Args:
        input_text:    The full prompt/context sent to the LLM
        output_tokens: Estimated output length (default 200 tokens)

    Returns:
        Estimated cost in USD
    """
    input_tokens = estimate_tokens(input_text)
    total_tokens = input_tokens + output_tokens
    return (total_tokens / 1000) * COST_PER_1K_TOKENS


def check_cost_limit(input_text: str, output_tokens: int = 200) -> float:
    """
    Raise CostLimitError if the estimated cost exceeds MAX_COST_PER_QUERY.

    Returns the estimated cost if within limits.
    """
    cost = estimate_cost(input_text, output_tokens)
    if cost > MAX_COST_PER_QUERY:
        raise CostLimitError(
            f"Estimated query cost ${cost:.4f} exceeds limit ${MAX_COST_PER_QUERY:.4f}. "
            f"Please shorten your query or reduce context."
        )
    return cost


# ---------------------------------------------------------------------------
# 3. Rate limiting
# ---------------------------------------------------------------------------


def check_rate_limit(user_id: str) -> None:
    """
    Raise RateLimitError if the user has exceeded MAX_QUERIES_PER_MINUTE.

    Uses a sliding window — only counts requests in the last 60 seconds.
    """
    now = time.time()
    window_start = now - 60.0

    # Remove timestamps outside the sliding window
    _rate_limit_store[user_id] = [
        t for t in _rate_limit_store[user_id] if t > window_start
    ]

    if len(_rate_limit_store[user_id]) >= MAX_QUERIES_PER_MINUTE:
        oldest = _rate_limit_store[user_id][0]
        retry_in = int(60 - (now - oldest)) + 1
        raise RateLimitError(
            f"Rate limit exceeded: {MAX_QUERIES_PER_MINUTE} queries/minute. "
            f"Try again in {retry_in} seconds."
        )

    # Record this request
    _rate_limit_store[user_id].append(now)


def get_rate_limit_status(user_id: str) -> dict:
    """Return current rate limit usage for a user."""
    now = time.time()
    window_start = now - 60.0
    recent = [t for t in _rate_limit_store.get(user_id, []) if t > window_start]
    return {
        "user_id": user_id,
        "queries_used": len(recent),
        "queries_limit": MAX_QUERIES_PER_MINUTE,
        "queries_remaining": max(0, MAX_QUERIES_PER_MINUTE - len(recent)),
    }


# ---------------------------------------------------------------------------
# 4. Combined pre-query check — call this once before processing any query
# ---------------------------------------------------------------------------


def run_all_checks(
    user_id: str,
    user_role: str,
    query: str,
    tool_name: Optional[str] = None,
) -> float:
    """
    Run all guardrail checks in order: rate limit → role → cost.

    Returns estimated cost in USD if all checks pass.
    Raises RateLimitError, AuthorizationError, or CostLimitError on failure.
    """
    check_rate_limit(user_id)
    validate_role(user_role)
    if tool_name:
        check_tool_access(tool_name, user_role)
    cost = check_cost_limit(query)
    return cost
