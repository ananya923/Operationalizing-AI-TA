"""
test_access_control.py — Tests for document-level and field-level access control.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import access_control as ac

# ---------------------------------------------------------------------------
# Sample documents for testing
# ---------------------------------------------------------------------------

FINANCE_DOC = {
    "id": "doc_fin_001",
    "title": "Travel and Expense Policy",
    "category": "Finance",
    "sensitivity": "Internal",
    "content": "Hotel reimbursement limit is $150/night. Salary ranges: $120,000 - $160,000.",
    "version": "2.5",
    "last_updated": "2024-01-01",
}

HR_DOC = {
    "id": "doc_hr_002",
    "title": "Compensation and Benefits Policy",
    "category": "HR",
    "sensitivity": "Confidential",
    "content": "Base salary: $85,000 - $120,000. SSN required for enrollment: 123-45-6789.",
    "version": "3.1",
    "last_updated": "2024-01-01",
}

ENGINEERING_DOC = {
    "id": "doc_eng_001",
    "title": "System Architecture Overview",
    "category": "Engineering",
    "sensitivity": "Internal",
    "content": "Our system uses microservices architecture deployed on GKE.",
    "version": "4.2",
    "last_updated": "2024-01-01",
}

COMPANY_DOC = {
    "id": "doc_gen_001",
    "title": "Mission and Values",
    "category": "Company",
    "sensitivity": "Public",
    "content": "TechCorp is committed to innovation and excellence.",
    "version": "1.0",
    "last_updated": "2024-01-01",
}

ALL_DOCS = [FINANCE_DOC, HR_DOC, ENGINEERING_DOC, COMPANY_DOC]


# ---------------------------------------------------------------------------
# Document-level filtering tests
# ---------------------------------------------------------------------------


class TestDocumentFiltering:

    def test_engineer_cannot_see_finance_docs(self):
        allowed, denied = ac.filter_documents([FINANCE_DOC], "engineer")
        assert FINANCE_DOC in denied
        assert FINANCE_DOC not in allowed

    def test_engineer_cannot_see_hr_docs(self):
        allowed, denied = ac.filter_documents([HR_DOC], "engineer")
        assert HR_DOC in denied
        assert HR_DOC not in allowed

    def test_engineer_can_see_engineering_docs(self):
        allowed, denied = ac.filter_documents([ENGINEERING_DOC], "engineer")
        assert ENGINEERING_DOC in allowed
        assert ENGINEERING_DOC not in denied

    def test_engineer_can_see_company_docs(self):
        allowed, denied = ac.filter_documents([COMPANY_DOC], "engineer")
        assert COMPANY_DOC in allowed

    def test_finance_can_see_finance_docs(self):
        allowed, denied = ac.filter_documents([FINANCE_DOC], "finance")
        assert FINANCE_DOC in allowed
        assert FINANCE_DOC not in denied

    def test_hr_can_see_hr_docs(self):
        allowed, denied = ac.filter_documents([HR_DOC], "hr")
        assert HR_DOC in allowed
        assert HR_DOC not in denied

    def test_hr_cannot_see_finance_docs(self):
        allowed, denied = ac.filter_documents([FINANCE_DOC], "hr")
        assert FINANCE_DOC in denied

    def test_denied_count_is_correct(self):
        allowed, denied = ac.filter_documents(ALL_DOCS, "engineer")
        # Engineer can't see Finance or HR
        assert len(denied) == 2
        assert len(allowed) == 2

    def test_empty_doc_list(self):
        allowed, denied = ac.filter_documents([], "engineer")
        assert allowed == []
        assert denied == []


# ---------------------------------------------------------------------------
# Field-level redaction tests
# ---------------------------------------------------------------------------


class TestFieldRedaction:

    def test_engineer_salary_redacted_in_content(self):
        redacted = ac.redact_fields(FINANCE_DOC, "engineer")
        # Dollar amounts tied to salary should be redacted
        assert "$[REDACTED]" in redacted["content"] or "REDACTED" in redacted["content"]

    def test_engineer_ssn_redacted(self):
        redacted = ac.redact_fields(HR_DOC, "engineer")
        assert "123-45-6789" not in redacted["content"]
        assert "REDACTED" in redacted["content"]

    def test_hr_ssn_visible(self):
        # HR role has view_hr_data permission — SSN should NOT be redacted
        redacted = ac.redact_fields(HR_DOC, "hr")
        assert "123-45-6789" in redacted["content"]

    def test_redaction_does_not_mutate_original(self):
        original_content = FINANCE_DOC["content"]
        ac.redact_fields(FINANCE_DOC, "engineer")
        assert FINANCE_DOC["content"] == original_content

    def test_non_sensitive_content_unchanged(self):
        redacted = ac.redact_fields(COMPANY_DOC, "engineer")
        assert redacted["content"] == COMPANY_DOC["content"]


# ---------------------------------------------------------------------------
# Combined apply_access_control tests
# ---------------------------------------------------------------------------


class TestApplyAccessControl:

    def test_engineer_sees_only_allowed_docs(self):
        allowed, denied = ac.apply_access_control(ALL_DOCS, "engineer")
        categories = [d["category"] for d in allowed]
        assert "Finance" not in categories
        assert "HR" not in categories

    def test_denied_docs_not_redacted(self):
        # Denied docs should be returned as-is (not modified)
        _, denied = ac.apply_access_control([FINANCE_DOC], "engineer")
        assert denied[0]["content"] == FINANCE_DOC["content"]

    def test_finance_role_sees_finance_docs(self):
        allowed, denied = ac.apply_access_control([FINANCE_DOC], "finance")
        assert len(allowed) == 1
        assert len(denied) == 0

    def test_all_roles_can_see_company_docs(self):
        for role in ["engineer", "hr", "finance", "manager"]:
            allowed, _ = ac.apply_access_control([COMPANY_DOC], role)
            assert len(allowed) == 1, f"Role {role} should see Company docs"
