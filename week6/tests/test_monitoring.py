"""
test_monitoring.py — Tests for the 8 monitoring metrics.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import monitoring as mon

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reset():
    """Clear in-memory state between tests."""
    mon._query_records.clear()
    mon._consistency_map.clear()


def _add_record(
    role="engineer",
    latency_ms=300.0,
    docs_retrieved=3,
    docs_denied=0,
    answer="The hotel limit is $150/night.",
    query="What is the hotel limit?",
    had_pii=False,
):
    mon.record_query(
        role=role,
        latency_ms=latency_ms,
        docs_retrieved=docs_retrieved,
        docs_denied=docs_denied,
        answer=answer,
        query=query,
        had_pii=had_pii,
    )


# ---------------------------------------------------------------------------
# PII detection
# ---------------------------------------------------------------------------


class TestPIIDetection:

    def test_ssn_detected(self):
        assert "ssn" in mon.check_pii("Employee SSN: 123-45-6789")

    def test_salary_detected(self):
        assert "salary" in mon.check_pii("Base salary is $120,000 per year")

    def test_clean_text_no_pii(self):
        assert mon.check_pii("The hotel limit is $150 per night.") == []

    def test_multiple_pii_types(self):
        result = mon.check_pii("SSN: 123-45-6789 and salary $95,000")
        assert "ssn" in result
        assert "salary" in result


# ---------------------------------------------------------------------------
# Answer completeness
# ---------------------------------------------------------------------------


class TestAnswerCompleteness:

    def setup_method(self):
        _reset()

    def test_all_complete(self):
        for _ in range(5):
            _add_record(answer="The travel limit is $3,000 per trip.")
        metric = mon._metric_answer_completeness()
        assert metric["value"] == 1.0
        assert metric["status"] == "ok"

    def test_all_fallback(self):
        for _ in range(5):
            _add_record(answer="I was unable to connect to the language model.")
        metric = mon._metric_answer_completeness()
        assert metric["value"] == 0.0
        assert metric["status"] == "alert"

    def test_mixed(self):
        _add_record(answer="The travel limit is $3,000.")
        _add_record(answer="No relevant data found.")
        metric = mon._metric_answer_completeness()
        assert metric["value"] == 0.5

    def test_no_data(self):
        metric = mon._metric_answer_completeness()
        assert metric["status"] == "no_data"


# ---------------------------------------------------------------------------
# Access denial rate
# ---------------------------------------------------------------------------


class TestAccessDenialRate:

    def setup_method(self):
        _reset()

    def test_no_denials(self):
        for _ in range(5):
            _add_record(docs_denied=0)
        metric = mon._metric_access_denial_rate()
        assert metric["value"] == 0.0
        assert metric["status"] == "ok"

    def test_all_denials(self):
        for _ in range(5):
            _add_record(docs_denied=2)
        metric = mon._metric_access_denial_rate()
        assert metric["value"] == 1.0
        assert metric["status"] == "alert"

    def test_below_threshold(self):
        for _ in range(9):
            _add_record(docs_denied=0)
        _add_record(docs_denied=1)
        metric = mon._metric_access_denial_rate()
        assert metric["value"] == 0.1
        assert metric["status"] == "ok"


# ---------------------------------------------------------------------------
# Doc retrieval coverage
# ---------------------------------------------------------------------------


class TestDocRetrievalCoverage:

    def setup_method(self):
        _reset()

    def test_full_coverage(self):
        _add_record(docs_retrieved=5, docs_denied=0)
        metric = mon._metric_doc_retrieval_coverage()
        assert metric["value"] == 1.0
        assert metric["status"] == "ok"

    def test_half_coverage(self):
        _add_record(docs_retrieved=5, docs_denied=5)
        metric = mon._metric_doc_retrieval_coverage()
        assert metric["value"] == 0.5
        assert metric["status"] == "alert"

    def test_no_docs(self):
        _add_record(docs_retrieved=0, docs_denied=0)
        metric = mon._metric_doc_retrieval_coverage()
        assert metric["status"] == "no_data"


# ---------------------------------------------------------------------------
# Answer consistency
# ---------------------------------------------------------------------------


class TestAnswerConsistency:

    def setup_method(self):
        _reset()

    def test_consistent_same_answer(self):
        for _ in range(3):
            _add_record(
                query="What is the hotel limit?", answer="The limit is $150/night."
            )
        metric = mon._metric_answer_consistency()
        assert metric["value"] == 1.0
        assert metric["status"] == "ok"

    def test_inconsistent_different_answers(self):
        _add_record(query="What is the hotel limit?", answer="The limit is $150/night.")
        _add_record(query="What is the hotel limit?", answer="The limit is $300/night.")
        metric = mon._metric_answer_consistency()
        assert metric["inconsistent_pairs"] == 1
        assert metric["status"] == "alert"

    def test_different_queries_independent(self):
        _add_record(query="What is the hotel limit?", answer="$150/night.")
        _add_record(query="What is the flight limit?", answer="$3,000/trip.")
        metric = mon._metric_answer_consistency()
        assert metric["value"] == 1.0


# ---------------------------------------------------------------------------
# PII exposure rate
# ---------------------------------------------------------------------------


class TestPIIExposureRate:

    def setup_method(self):
        _reset()

    def test_no_pii(self):
        for _ in range(5):
            _add_record(had_pii=False)
        metric = mon._metric_pii_exposure()
        assert metric["value"] == 0.0
        assert metric["status"] == "ok"

    def test_pii_detected_alerts(self):
        _add_record(had_pii=True)
        for _ in range(4):
            _add_record(had_pii=False)
        metric = mon._metric_pii_exposure()
        assert metric["value"] == 0.2
        assert metric["status"] == "alert"


# ---------------------------------------------------------------------------
# Full get_metrics integration
# ---------------------------------------------------------------------------


class TestGetMetrics:

    def setup_method(self):
        _reset()

    def test_returns_all_8_metrics(self):
        _add_record()
        result = mon.get_metrics()
        assert "metrics" in result
        expected_keys = {
            "answer_completeness",
            "access_denials",
            "response_latency",
            "doc_retrieval_coverage",
            "access_denial_rate",
            "audit_log_volume_mb",
            "answer_consistency",
            "pii_exposure_rate",
        }
        assert expected_keys == set(result["metrics"].keys())

    def test_overall_status_ok_when_all_ok(self):
        for _ in range(5):
            _add_record()
        result = mon.get_metrics()
        assert result["status"] == "ok"

    def test_total_queries_count(self):
        for _ in range(7):
            _add_record()
        result = mon.get_metrics()
        assert result["total_queries"] == 7
