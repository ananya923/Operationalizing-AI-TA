"""
test_optimization.py — Week 7: Tests for cost_optimization.py

Verifies:
  1. Score computation produces values in [0, 1]
  2. Injected problem docs score below threshold at a high rate
  3. Pruning reduces corpus size significantly
  4. Original quality docs mostly survive pruning
  5. Near-duplicate detection flags duplicates and preserves originals
  6. archive_corpus writes valid output files
  7. Idempotency — running optimization twice on cleaned corpus is a no-op
  8. Custom threshold changes how many docs survive
"""

import json
import os
import sys
import tempfile
import pytest

# ── path setup ────────────────────────────────────────────────────────────────
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
WEEK7_APP = os.path.join(REPO_ROOT, "week7", "app")
sys.path.insert(0, WEEK7_APP)

import cost_optimization as co

# ── corpus paths ──────────────────────────────────────────────────────────────
BLOATED_CORPUS  = os.path.join(REPO_ROOT, "data", "raw", "techcorp", "documents_week7_bloated.json")
BASELINE_CORPUS = os.path.join(REPO_ROOT, "data", "raw", "techcorp", "documents.json")

# ── fixtures ──────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def bloated_docs():
    with open(BLOATED_CORPUS) as f:
        return json.load(f)

@pytest.fixture(scope="module")
def baseline_docs():
    with open(BASELINE_CORPUS) as f:
        return json.load(f)

@pytest.fixture(scope="module")
def optimization_result(bloated_docs):
    """Run archive_corpus once and return (summary, kept_docs, archived_docs)."""
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "kept.json")
        arc = os.path.join(tmp, "archived.json")
        summary = co.archive_corpus(BLOATED_CORPUS, out, arc)
        with open(out) as f:
            kept = json.load(f)
        with open(arc) as f:
            archived = json.load(f)
    return summary, kept, archived


# ── 1. Score range ────────────────────────────────────────────────────────────

def test_score_range(bloated_docs):
    """All composite scores must be in [0.0, 1.0]."""
    for doc in bloated_docs[:100]:
        result = co.score_document(doc)
        score = result["composite"]
        assert 0.0 <= score <= 1.0, (
            f"Score {score} out of range for doc id={doc.get('id')}"
        )


def test_score_document_returns_expected_keys(bloated_docs):
    """score_document must return composite, dimensions, and archive_reason."""
    result = co.score_document(bloated_docs[0])
    assert "composite"      in result
    assert "dimensions"     in result
    assert "archive_reason" in result
    assert isinstance(result["dimensions"], dict)
    assert isinstance(result["archive_reason"], list)


# ── 2. Injected problem docs score below threshold ────────────────────────────

def test_misleading_docs_flagged_by_scorer(bloated_docs):
    """
    Docs injected as misleading/circular/vague should have 'not_misleading'
    listed as a failing dimension in at least 80% of cases.
    Note: composite score may still be ≥ threshold if other dimensions pass —
    these docs are caught via near-duplicate detection in the full pipeline.
    """
    misleading_keywords = {"circular", "vague", "doesn't answer", "no actual info"}
    misleading = [
        d for d in bloated_docs
        if any(kw in d.get("_problem", "").lower() for kw in misleading_keywords)
    ]
    assert len(misleading) >= 10, (
        f"Expected ≥10 misleading docs, found {len(misleading)}"
    )
    flagged = [
        d for d in misleading
        if "not_misleading" in co.score_document(d)["archive_reason"]
    ]
    rate = len(flagged) / len(misleading)
    assert rate >= 0.80, (
        f"Only {rate:.1%} of misleading docs flagged by not_misleading scorer "
        f"({len(flagged)}/{len(misleading)})"
    )


def test_draft_docs_flagged_by_scorer(bloated_docs):
    """
    Unfinished draft docs should have 'not_draft' listed as a failing dimension.
    The composite score stays above 0.5 for many drafts (other dimensions pass),
    but the dimension-level flag confirms the scorer detects the problem.
    """
    drafts = [
        d for d in bloated_docs
        if "unfinished" in d.get("_problem", "").lower()
        or "published by mistake" in d.get("_problem", "").lower()
    ]
    assert len(drafts) >= 10, f"Expected ≥10 draft docs, found {len(drafts)}"
    flagged = [
        d for d in drafts
        if "not_draft" in co.score_document(d)["archive_reason"]
    ]
    rate = len(flagged) / len(drafts)
    assert rate >= 0.80, (
        f"Only {rate:.1%} of draft docs flagged by not_draft scorer "
        f"({len(flagged)}/{len(drafts)})"
    )


def test_versioned_docs_archived_via_near_dup(optimization_result):
    """
    Old versioned copies score above threshold (their non-version dimensions pass),
    but are caught by near-duplicate title detection. All versioned docs should
    be archived in the final pipeline output.
    """
    _, _, archived = optimization_result

    # Build set of archived IDs from the full pipeline run
    archived_ids = {d["id"] for d in archived}

    # Find versioned docs in the bloated corpus using the _problem field
    with open(BLOATED_CORPUS) as f:
        bloated_docs = json.load(f)
    versioned = [
        d for d in bloated_docs
        if "version" in d.get("_problem", "").lower()
        and "outdated" in d.get("_problem", "").lower()
    ]
    assert len(versioned) >= 10, f"Expected ≥10 versioned docs, found {len(versioned)}"

    archived_versioned = [d for d in versioned if d["id"] in archived_ids]
    rate = len(archived_versioned) / len(versioned)
    assert rate >= 0.90, (
        f"Only {rate:.1%} of versioned docs were archived "
        f"({len(archived_versioned)}/{len(versioned)})"
    )


# ── 3. Pruning reduces corpus size ───────────────────────────────────────────

def test_pruning_reduces_size(optimization_result):
    """Optimized corpus must be meaningfully smaller than bloated corpus."""
    summary, kept, archived = optimization_result
    assert summary["after"]["kept_docs"] < summary["before"]["total_docs"]
    assert summary["after"]["archived_docs"] > 0
    assert (summary["after"]["kept_docs"] + summary["after"]["archived_docs"]
            == summary["before"]["total_docs"])


def test_pruning_removes_at_least_30_percent(optimization_result):
    """Should remove at least 30% of the bloated corpus."""
    summary, _, _ = optimization_result
    reduction = summary["after"]["reduction_pct"]
    assert reduction >= 30.0, (
        f"Corpus reduction {reduction:.1f}% is less than expected 30%"
    )


def test_archived_avg_score_lower_than_kept(optimization_result):
    """Archived docs should have a lower average quality score than kept docs."""
    summary, _, _ = optimization_result
    assert summary["score_stats"]["archived_avg"] < summary["score_stats"]["kept_avg"], (
        "Archived docs have equal or higher scores than kept docs — scoring logic is inverted"
    )


# ── 4. Quality docs mostly survive ───────────────────────────────────────────

def test_quality_docs_mostly_survive(baseline_docs, optimization_result):
    """
    Original baseline docs (the 74 true-signal documents) should mostly survive.
    Acceptable false-positive rate: ≤30% (i.e. ≥70% survival).
    """
    summary, kept, _ = optimization_result
    baseline_ids = {d["id"] for d in baseline_docs}
    kept_ids      = {d["id"] for d in kept}

    survived = baseline_ids & kept_ids
    survival_rate = len(survived) / len(baseline_ids)
    assert survival_rate >= 0.70, (
        f"Only {survival_rate:.1%} of baseline docs survived optimization "
        f"({len(survived)}/{len(baseline_ids)}) — too many false positives"
    )


# ── 5. Near-duplicate detection ───────────────────────────────────────────────

def test_near_duplicate_detection_basic():
    """Near-duplicate detector flags pairs with high Jaccard title similarity."""
    docs = [
        {"id": "remote_work_policy",
         "title": "Remote Work Policy",
         "content": "employees may work from home three days per week"},
        {"id": "remote_work_policy_v2",
         "title": "Remote Work Policy Updated",
         "content": "employees may work from home three days per week revised 2024"},
        {"id": "expense_reimbursement",
         "title": "Expense Reimbursement Guide",
         "content": "submit receipts within 30 days for reimbursement"},
    ]
    dup_ids = co.find_near_duplicates(docs, threshold=0.5)
    # The v2 remote-work doc should be flagged
    assert "remote_work_policy_v2" in dup_ids, (
        "Versioned near-duplicate not detected"
    )
    # The unrelated expense doc should not be flagged
    assert "expense_reimbursement" not in dup_ids, (
        "Unrelated doc incorrectly flagged as near-duplicate"
    )


def test_near_dup_prefers_original_over_versioned(optimization_result):
    """
    Versioned copies (_v1, _v2 …) should be archived more often than originals.
    """
    _, _, archived = optimization_result
    archived_ids = {d["id"] for d in archived}

    versioned_archived = [i for i in archived_ids
                          if "_v" in i and i.split("_v")[-1].isdigit()]
    # Versioned docs in the bloated corpus number in the hundreds —
    # at least 50 should end up archived
    assert len(versioned_archived) >= 50, (
        f"Only {len(versioned_archived)} versioned copies archived — "
        "near-dup detection may not be working correctly"
    )


def test_near_duplicates_returns_set_of_ids():
    """find_near_duplicates must return a set of string IDs."""
    docs = [
        {"id": "doc_a", "title": "Employee Handbook", "content": "rules"},
        {"id": "doc_b", "title": "Employee Handbook v2", "content": "updated rules"},
    ]
    result = co.find_near_duplicates(docs, threshold=0.5)
    assert isinstance(result, set), "find_near_duplicates should return a set"
    for item in result:
        assert isinstance(item, str), f"Expected string ID, got {type(item)}"


# ── 6. archive_corpus writes valid output files ───────────────────────────────

def test_archive_corpus_output_files_valid():
    """archive_corpus should write parseable JSON to both output and archive paths."""
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "kept.json")
        arc = os.path.join(tmp, "archived.json")
        summary = co.archive_corpus(BLOATED_CORPUS, out, arc)

        assert os.path.exists(out), "Output (kept) file not created"
        assert os.path.exists(arc), "Archive file not created"

        with open(out) as f:
            kept = json.load(f)
        with open(arc) as f:
            archived = json.load(f)

        assert isinstance(kept, list),     "kept.json should be a list"
        assert isinstance(archived, list), "archived.json should be a list"
        assert len(kept) > 0,             "kept corpus is unexpectedly empty"
        assert len(archived) > 0,         "archive is unexpectedly empty"


def test_archive_corpus_summary_keys():
    """archive_corpus summary dict must contain expected keys."""
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "kept.json")
        arc = os.path.join(tmp, "archived.json")
        summary = co.archive_corpus(BLOATED_CORPUS, out, arc)

    for key in ("threshold", "before", "after", "archive_reasons", "score_stats"):
        assert key in summary, f"Missing key '{key}' in summary"
    assert "kept_docs"     in summary["after"]
    assert "archived_docs" in summary["after"]
    assert "reduction_pct" in summary["after"]


# ── 7. Idempotency ───────────────────────────────────────────────────────────

def test_idempotency():
    """
    Running archive_corpus on its own output should archive 0 additional docs.
    """
    with tempfile.TemporaryDirectory() as tmp:
        out1 = os.path.join(tmp, "pass1.json")
        arc1 = os.path.join(tmp, "arc1.json")
        co.archive_corpus(BLOATED_CORPUS, out1, arc1)

        out2 = os.path.join(tmp, "pass2.json")
        arc2 = os.path.join(tmp, "arc2.json")
        summary2 = co.archive_corpus(out1, out2, arc2)

        with open(arc2) as f:
            second_pass_archived = json.load(f)

        assert len(second_pass_archived) == 0, (
            f"Second pass still archived {len(second_pass_archived)} docs — "
            "optimization is not idempotent"
        )
        assert summary2["after"]["kept_docs"] == summary2["before"]["total_docs"], (
            "Second pass should keep all docs (nothing left to archive)"
        )


# ── 8. Custom threshold changes output ───────────────────────────────────────

def test_stricter_threshold_keeps_fewer_docs():
    """Threshold 0.6 should keep fewer docs than default 0.5."""
    with tempfile.TemporaryDirectory() as tmp:
        out05 = os.path.join(tmp, "kept_05.json")
        arc05 = os.path.join(tmp, "arc_05.json")
        s05 = co.archive_corpus(BLOATED_CORPUS, out05, arc05, threshold=0.5)

        out06 = os.path.join(tmp, "kept_06.json")
        arc06 = os.path.join(tmp, "arc_06.json")
        s06 = co.archive_corpus(BLOATED_CORPUS, out06, arc06, threshold=0.6)

    assert s06["after"]["kept_docs"] <= s05["after"]["kept_docs"], (
        f"Stricter threshold (0.6) kept MORE docs than default (0.5): "
        f"{s06['after']['kept_docs']} > {s05['after']['kept_docs']}"
    )


def test_permissive_threshold_keeps_more_docs():
    """Threshold 0.3 should keep more docs than default 0.5."""
    with tempfile.TemporaryDirectory() as tmp:
        out05 = os.path.join(tmp, "kept_05.json")
        arc05 = os.path.join(tmp, "arc_05.json")
        s05 = co.archive_corpus(BLOATED_CORPUS, out05, arc05, threshold=0.5)

        out03 = os.path.join(tmp, "kept_03.json")
        arc03 = os.path.join(tmp, "arc_03.json")
        s03 = co.archive_corpus(BLOATED_CORPUS, out03, arc03, threshold=0.3)

    assert s03["after"]["kept_docs"] >= s05["after"]["kept_docs"], (
        f"Permissive threshold (0.3) kept FEWER docs than default (0.5): "
        f"{s03['after']['kept_docs']} < {s05['after']['kept_docs']}"
    )
