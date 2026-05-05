"""
test_guardrails.py — Unit tests for access control, cost limits, and rate limiting.

Run with:
    cd week5
    pytest tests/test_guardrails.py -v
"""

import sys
import os
import time
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import guardrails as g

# ---------------------------------------------------------------------------
# Role validation tests
# ---------------------------------------------------------------------------


class TestRoleValidation:

    def test_valid_roles_accepted(self):
        for role in ["engineer", "manager", "hr", "finance", "executive"]:
            g.validate_role(role)  # should not raise

    def test_invalid_role_raises(self):
        with pytest.raises(g.AuthorizationError):
            g.validate_role("hacker")

    def test_empty_role_raises(self):
        with pytest.raises(g.AuthorizationError):
            g.validate_role("")

    def test_case_sensitive_role(self):
        with pytest.raises(g.AuthorizationError):
            g.validate_role("Engineer")  # must be lowercase


# ---------------------------------------------------------------------------
# Tool access control tests
# ---------------------------------------------------------------------------


class TestToolAccessControl:

    # Engineer permissions
    def test_engineer_can_search_employees(self):
        g.check_tool_access("search_employees", "engineer")

    def test_engineer_can_search_policies(self):
        g.check_tool_access("search_policies", "engineer")

    def test_engineer_can_view_projects(self):
        g.check_tool_access("get_project", "engineer")

    def test_engineer_cannot_view_all_expenses(self):
        with pytest.raises(g.AuthorizationError):
            g.check_tool_access("get_expense_summary", "engineer")

    def test_engineer_cannot_view_financial_reports(self):
        with pytest.raises(g.AuthorizationError):
            g.check_tool_access("get_total_expenses", "engineer")

    def test_engineer_cannot_view_hr_data(self):
        with pytest.raises(g.AuthorizationError):
            g.check_tool_access("get_pto_summary", "engineer")

    # Finance permissions
    def test_finance_can_view_expenses(self):
        g.check_tool_access("get_expense_summary", "finance")

    def test_finance_can_view_financial_reports(self):
        g.check_tool_access("get_total_expenses", "finance")

    def test_finance_cannot_view_hr_data(self):
        with pytest.raises(g.AuthorizationError):
            g.check_tool_access("get_pto_summary", "finance")

    # HR permissions
    def test_hr_can_view_hr_data(self):
        g.check_tool_access("get_pto_summary", "hr")
        g.check_tool_access("get_health_plan_distribution", "hr")

    def test_hr_cannot_view_all_expenses(self):
        with pytest.raises(g.AuthorizationError):
            g.check_tool_access("get_expense_summary", "hr")

    # Manager permissions
    def test_manager_can_view_all_expenses(self):
        g.check_tool_access("get_expense_summary", "manager")

    def test_manager_cannot_view_hr_data(self):
        with pytest.raises(g.AuthorizationError):
            g.check_tool_access("get_pto_summary", "manager")

    # Executive permissions — can do everything
    def test_executive_can_use_all_tools(self):
        for tool in g.TOOL_PERMISSION_MAP:
            g.check_tool_access(tool, "executive")

    # Unknown tool — should be allowed (no restriction)
    def test_unknown_tool_is_allowed(self):
        g.check_tool_access("some_future_tool", "engineer")

    def test_get_allowed_tools_engineer(self):
        tools = g.get_allowed_tools("engineer")
        assert "search_policies" in tools
        assert "get_expense_summary" not in tools
        assert "get_total_expenses" not in tools

    def test_get_allowed_tools_executive_has_all(self):
        exec_tools = g.get_allowed_tools("executive")
        eng_tools = g.get_allowed_tools("engineer")
        # Executive should have a superset of engineer's tools
        assert set(eng_tools).issubset(set(exec_tools))


# ---------------------------------------------------------------------------
# Cost estimation and enforcement tests
# ---------------------------------------------------------------------------


class TestCostLimits:

    def test_short_query_has_low_cost(self):
        cost = g.estimate_cost("What is the travel policy?")
        assert cost < g.MAX_COST_PER_QUERY

    def test_cost_is_positive(self):
        cost = g.estimate_cost("hello")
        assert cost > 0

    def test_very_long_input_exceeds_limit(self):
        long_text = "x" * 300_000  # ~75K tokens
        with pytest.raises(g.CostLimitError):
            g.check_cost_limit(long_text)

    def test_normal_query_passes_cost_check(self):
        cost = g.check_cost_limit("What is the PTO policy?", output_tokens=200)
        assert cost <= g.MAX_COST_PER_QUERY

    def test_token_estimation_scales_with_length(self):
        short = g.estimate_tokens("hi")
        long = g.estimate_tokens("hi " * 100)
        assert long > short

    def test_cost_calculation_formula(self):
        # Manually verify: 1000 tokens at $0.0002/1K = $0.0002
        tokens = 1000
        expected = (tokens / 1000) * g.COST_PER_1K_TOKENS
        actual = g.estimate_cost(
            "x" * (tokens * g.AVG_CHARS_PER_TOKEN), output_tokens=0
        )
        assert abs(actual - expected) < 0.0001


# ---------------------------------------------------------------------------
# Rate limiting tests
# ---------------------------------------------------------------------------


class TestRateLimiting:

    def test_first_query_allowed(self):
        # Use a unique user ID so state from other tests doesn't interfere
        g.check_rate_limit("test_rate_fresh_user_1")

    def test_up_to_limit_allowed(self):
        user = "test_rate_fresh_user_2"
        for _ in range(g.MAX_QUERIES_PER_MINUTE):
            g.check_rate_limit(user)

    def test_exceeding_limit_raises(self):
        user = "test_rate_fresh_user_3"
        for _ in range(g.MAX_QUERIES_PER_MINUTE):
            g.check_rate_limit(user)
        with pytest.raises(g.RateLimitError):
            g.check_rate_limit(user)

    def test_different_users_independent(self):
        user_a = "test_rate_user_a_isolated"
        user_b = "test_rate_user_b_isolated"
        # Exhaust user_a's limit
        for _ in range(g.MAX_QUERIES_PER_MINUTE):
            g.check_rate_limit(user_a)
        with pytest.raises(g.RateLimitError):
            g.check_rate_limit(user_a)
        # user_b should still be fine
        g.check_rate_limit(user_b)

    def test_rate_limit_status_shows_usage(self):
        user = "test_rate_status_user"
        g.check_rate_limit(user)
        g.check_rate_limit(user)
        status = g.get_rate_limit_status(user)
        assert status["queries_used"] == 2
        assert status["queries_limit"] == g.MAX_QUERIES_PER_MINUTE
        assert status["queries_remaining"] == g.MAX_QUERIES_PER_MINUTE - 2

    def test_rate_limit_error_message_contains_retry(self):
        user = "test_rate_msg_user"
        for _ in range(g.MAX_QUERIES_PER_MINUTE):
            g.check_rate_limit(user)
        with pytest.raises(g.RateLimitError, match="Try again in"):
            g.check_rate_limit(user)


# ---------------------------------------------------------------------------
# Combined guardrail check
# ---------------------------------------------------------------------------


class TestRunAllChecks:

    def test_valid_request_passes(self):
        cost = g.run_all_checks(
            user_id="eval_combined_1",
            user_role="engineer",
            query="What is the travel policy?",
            tool_name="search_policies",
        )
        assert cost > 0

    def test_invalid_role_blocked(self):
        with pytest.raises(g.AuthorizationError):
            g.run_all_checks(
                user_id="eval_combined_2",
                user_role="unknown_role",
                query="What is the travel policy?",
            )

    def test_unauthorized_tool_blocked(self):
        with pytest.raises(g.AuthorizationError):
            g.run_all_checks(
                user_id="eval_combined_3",
                user_role="engineer",
                query="Show all expenses",
                tool_name="get_total_expenses",
            )
