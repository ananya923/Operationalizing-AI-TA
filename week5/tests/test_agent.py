"""
test_agent.py — Unit tests for intent classification, tool routing, and fallback behavior.

Run with:
    cd week5
    pytest tests/test_agent.py -v
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from agent import classify_intent, run_tools
import guardrails as g

# ---------------------------------------------------------------------------
# Intent classification tests
# ---------------------------------------------------------------------------


class TestIntentClassification:

    def test_policy_intent_travel(self):
        assert (
            classify_intent("What is the travel expense reimbursement limit?")
            == "policy"
        )

    def test_policy_intent_remote(self):
        assert classify_intent("What is TechCorp's remote work policy?") == "policy"

    def test_policy_intent_gdpr(self):
        assert classify_intent("What is the GDPR compliance policy?") == "policy"

    def test_policy_intent_handbook(self):
        assert classify_intent("Where can I find the employee handbook?") == "policy"

    def test_employee_intent_headcount(self):
        assert (
            classify_intent("How many employees are in each department?") == "employee"
        )

    def test_employee_intent_who(self):
        assert (
            classify_intent("Who is the manager of the Engineering team?") == "employee"
        )

    def test_expense_intent_total(self):
        assert (
            classify_intent("What is the total amount spent on expenses?") == "expense"
        )

    def test_expense_intent_category(self):
        assert (
            classify_intent(
                "What are the expense categories with the highest spending?"
            )
            == "expense"
        )

    def test_project_intent_budget(self):
        assert (
            classify_intent("Show me the budget status of all projects.") == "project"
        )

    def test_project_intent_active(self):
        assert classify_intent("Which projects are currently active?") == "project"

    def test_benefits_intent_pto(self):
        assert (
            classify_intent("What is the average PTO days employees get per year?")
            == "benefits"
        )

    def test_benefits_intent_health(self):
        assert (
            classify_intent("What health insurance plans does TechCorp offer?")
            == "benefits"
        )

    def test_unknown_falls_back_to_policy(self):
        # Completely unrecognized query should default to policy search
        result = classify_intent("xyzzy frobulate")
        assert result == "policy"


# ---------------------------------------------------------------------------
# Tool routing tests
# ---------------------------------------------------------------------------


class TestToolRouting:

    def test_policy_query_calls_search_policies(self):
        tools, context = run_tools("policy", "What is the travel policy?", "engineer")
        tool_names = [t.tool for t in tools]
        assert "search_policies" in tool_names

    def test_policy_query_returns_non_empty_context(self):
        tools, context = run_tools("policy", "What is the travel policy?", "engineer")
        assert len(context) > 100

    def test_employee_query_calls_headcount(self):
        tools, context = run_tools(
            "employee", "How many people are in each department?", "manager"
        )
        tool_names = [t.tool for t in tools]
        assert "get_headcount" in tool_names

    def test_employee_query_with_name(self):
        # A query with a capitalized name should trigger get_employee
        tools, context = run_tools("employee", "Who is Richard in Finance?", "manager")
        tool_names = [t.tool for t in tools]
        assert "get_employee" in tool_names or "search_employees" in tool_names

    def test_expense_query_finance_role(self):
        tools, context = run_tools("expense", "What is the total spending?", "finance")
        tool_names = [t.tool for t in tools]
        assert any("expense" in t for t in tool_names)

    def test_expense_query_category_breakdown(self):
        tools, context = run_tools(
            "expense", "Show expense breakdown by category", "finance"
        )
        tool_names = [t.tool for t in tools]
        assert "get_expense_summary" in tool_names

    def test_project_budget_query(self):
        tools, context = run_tools(
            "project", "Show me budget status of all projects", "manager"
        )
        tool_names = [t.tool for t in tools]
        assert "get_project_budget_summary" in tool_names

    def test_project_active_query(self):
        tools, context = run_tools("project", "Which projects are active?", "engineer")
        tool_names = [t.tool for t in tools]
        assert "get_project" in tool_names

    def test_benefits_pto_query(self):
        tools, context = run_tools(
            "benefits", "What is the average PTO per year?", "hr"
        )
        tool_names = [t.tool for t in tools]
        assert "get_pto_summary" in tool_names

    def test_benefits_health_query(self):
        tools, context = run_tools("benefits", "What health plans are available?", "hr")
        tool_names = [t.tool for t in tools]
        assert "get_health_plan_distribution" in tool_names


# ---------------------------------------------------------------------------
# Fallback behavior tests
# ---------------------------------------------------------------------------


class TestFallbackBehavior:

    def test_unknown_intent_falls_back_to_policy_search(self):
        # Even garbage input should not raise — falls back to policy search
        tools, context = run_tools(
            "policy", "something completely unrelated", "engineer"
        )
        assert tools is not None
        assert context is not None

    def test_engineer_expense_query_raises_auth_error(self):
        # Engineer should be blocked from expense tools
        with pytest.raises(g.AuthorizationError):
            run_tools("expense", "What is the total spending?", "engineer")

    def test_engineer_cannot_see_financial_reports(self):
        with pytest.raises(g.AuthorizationError):
            g.check_tool_access("get_total_expenses", "engineer")

    def test_all_roles_can_search_policies(self):
        for role in ["engineer", "manager", "hr", "finance", "executive"]:
            # Should not raise
            g.check_tool_access("search_policies", role)

    def test_context_is_string(self):
        _, context = run_tools("policy", "What is the PTO policy?", "engineer")
        assert isinstance(context, str)

    def test_tools_called_is_list(self):
        tools, _ = run_tools("policy", "What is the PTO policy?", "engineer")
        assert isinstance(tools, list)
        assert len(tools) > 0
