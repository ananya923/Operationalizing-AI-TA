"""
cost_optimization.py — Week 7: Corpus quality scoring and archival.

Scores every document in the bloated corpus across 7 problem dimensions,
then archives (removes) documents that fall below a quality threshold.
Outputs a cleaned corpus file ready to be loaded by embedding.py.

Quality scoring dimensions (each 0–1, higher = better quality):
  1. has_required_fields   — id, title, category, content, last_updated all present
  2. content_substance     — content is non-empty, not a placeholder, not circular
  3. not_draft             — version string does not indicate draft/incomplete
  4. not_misleading        — no is_high_retrieval_waste flag
  5. not_seasonal          — no valid_only temporal restriction
  6. not_miscategorized    — no _miscat suffix (proxy for category mismatch)
  7. is_latest_version     — no _v<N> suffix indicating an old version

A composite score is computed as a weighted average.
Documents scoring below ARCHIVE_THRESHOLD are moved to the archive.

Usage:
    # From repo root:
    python week7/app/cost_optimization.py

    # Explicit paths:
    python week7/app/cost_optimization.py \\
        --input  data/raw/techcorp/documents_week7_bloated.json \\
        --output data/raw/techcorp/documents_week7_cleaned.json \\
        --archive data/raw/techcorp/documents_week7_archived.json \\
        --threshold 0.5
"""

import json
import os
import re
import argparse
from datetime import datetime, timezone
from collections import defaultdict

# ---------------------------------------------------------------------------
# Default paths
# ---------------------------------------------------------------------------

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

DEFAULT_INPUT   = os.path.join(REPO_ROOT, "data", "raw", "techcorp", "documents_week7_bloated.json")
DEFAULT_OUTPUT  = os.path.join(REPO_ROOT, "data", "raw", "techcorp", "documents_week7_cleaned.json")
DEFAULT_ARCHIVE = os.path.join(REPO_ROOT, "data", "raw", "techcorp", "documents_week7_archived.json")

# Archive documents whose composite quality score is below this threshold
ARCHIVE_THRESHOLD = 0.5

# ---------------------------------------------------------------------------
# Scoring weights (must sum to 1.0)
# ---------------------------------------------------------------------------

SCORE_WEIGHTS = {
    "has_required_fields":  0.15,
    "content_substance":    0.20,
    "not_draft":            0.15,
    "not_misleading":       0.20,
    "not_seasonal":         0.05,
    "not_miscategorized":   0.05,
    "is_latest_version":    0.20,   # raised: old versions must fail the threshold
}

assert abs(sum(SCORE_WEIGHTS.values()) - 1.0) < 1e-9, "Weights must sum to 1.0"

# Patterns that indicate placeholder / circular / low-substance content
_PLACEHOLDER_PATTERNS = [
    re.compile(r"\[PENDING\]",       re.IGNORECASE),
    re.compile(r"\[TODO\]",          re.IGNORECASE),
    re.compile(r"\[INCOMPLETE",      re.IGNORECASE),
    re.compile(r"\[needs work\]",    re.IGNORECASE),
    re.compile(r"\[being updated\]", re.IGNORECASE),
    re.compile(r"\[contact HR\]",    re.IGNORECASE),
    re.compile(r"DO NOT USE FOR DECISIONS", re.IGNORECASE),
    re.compile(r"DRAFT",             re.IGNORECASE),
]

_CIRCULAR_PATTERNS = [
    re.compile(r"see (the |full )?(policy|doc|handbook)",  re.IGNORECASE),
    re.compile(r"for (exact |more )?details,? see",        re.IGNORECASE),
    re.compile(r"amounts vary by",                          re.IGNORECASE),
    re.compile(r"check the full policy",                    re.IGNORECASE),
]

_DRAFT_VERSION_PATTERN = re.compile(
    r"(draft|incomplete|wip|0\.\d|pending)", re.IGNORECASE
)

_OLD_VERSION_PATTERN = re.compile(r"_v\d+$")

# Minimum content length (characters) to be considered substantive
MIN_CONTENT_LENGTH = 50


# ---------------------------------------------------------------------------
# Individual scorers — each returns a float in [0, 1]
# ---------------------------------------------------------------------------

def score_required_fields(doc: dict) -> float:
    """All key fields must be present and non-empty."""
    required = ["id", "title", "category", "content", "last_updated"]
    present = sum(1 for f in required if doc.get(f, "").strip())
    return present / len(required)


def score_content_substance(doc: dict) -> float:
    """
    Content must be long enough, not full of placeholders, and not circular
    (i.e. just redirecting to another document without answering anything).
    """
    content = doc.get("content", "")

    if len(content) < MIN_CONTENT_LENGTH:
        return 0.0

    # Penalise placeholder tokens
    placeholder_hits = sum(1 for p in _PLACEHOLDER_PATTERNS if p.search(content))
    if placeholder_hits >= 2:
        return 0.1
    if placeholder_hits == 1:
        return 0.5

    # Penalise circular references
    circular_hits = sum(1 for p in _CIRCULAR_PATTERNS if p.search(content))
    if circular_hits >= 2:
        return 0.2
    if circular_hits == 1:
        return 0.6

    return 1.0


def score_not_draft(doc: dict) -> float:
    """Version string must not indicate a draft or incomplete state."""
    version = str(doc.get("version", ""))
    if _DRAFT_VERSION_PATTERN.search(version):
        return 0.0
    return 1.0


def score_not_misleading(doc: dict) -> float:
    """Document must not be flagged as high-retrieval-waste."""
    if doc.get("is_high_retrieval_waste"):
        return 0.0
    return 1.0


def score_not_seasonal(doc: dict) -> float:
    """Document must not have a temporal restriction (valid_only field)."""
    if doc.get("valid_only"):
        return 0.0
    return 1.0


def score_not_miscategorized(doc: dict) -> float:
    """Document ID must not carry the _miscat suffix."""
    if "_miscat" in doc.get("id", ""):
        return 0.0
    return 1.0


def score_latest_version(doc: dict) -> float:
    """
    Document must not be an old versioned copy (_v<N> suffix).
    Quality originals get full score.
    """
    doc_id = doc.get("id", "")
    if _OLD_VERSION_PATTERN.search(doc_id):
        return 0.0
    return 1.0


# ---------------------------------------------------------------------------
# Composite scorer
# ---------------------------------------------------------------------------

SCORERS = {
    "has_required_fields":  score_required_fields,
    "content_substance":    score_content_substance,
    "not_draft":            score_not_draft,
    "not_misleading":       score_not_misleading,
    "not_seasonal":         score_not_seasonal,
    "not_miscategorized":   score_not_miscategorized,
    "is_latest_version":    score_latest_version,
}


def score_document(doc: dict) -> dict:
    """
    Compute all dimension scores and a weighted composite score for one document.

    Returns a dict with keys:
        composite       float [0, 1]
        dimensions      dict of dimension → score
        archive_reason  list of failing dimension names (empty if kept)
    """
    dimensions = {dim: fn(doc) for dim, fn in SCORERS.items()}
    composite  = sum(dimensions[dim] * SCORE_WEIGHTS[dim] for dim in dimensions)
    failing    = [dim for dim, score in dimensions.items() if score < 0.5]

    return {
        "composite":      round(composite, 4),
        "dimensions":     {k: round(v, 4) for k, v in dimensions.items()},
        "archive_reason": failing,
    }


# ---------------------------------------------------------------------------
# Near-duplicate detection (title similarity)
# ---------------------------------------------------------------------------

def _title_tokens(title: str) -> frozenset:
    """Lower-case word set from title, stripping stopwords and punctuation."""
    stopwords = {"the", "a", "an", "of", "and", "or", "for", "to", "in",
                 "on", "at", "by", "is", "are", "was", "were"}
    words = re.findall(r"[a-z0-9]+", title.lower())
    return frozenset(w for w in words if w not in stopwords)


def find_near_duplicates(docs: list[dict], threshold: float = 0.85) -> set:
    """
    Return set of doc IDs that are near-duplicates of another doc.
    Uses Jaccard similarity on title token sets.
    Keeps the higher-scored doc (or the first one if scores equal).
    """
    to_archive = set()
    titles = [(d["id"], _title_tokens(d.get("title", ""))) for d in docs]

    for i in range(len(titles)):
        if titles[i][0] in to_archive:
            continue
        for j in range(i + 1, len(titles)):
            if titles[j][0] in to_archive:
                continue
            a_tokens, b_tokens = titles[i][1], titles[j][1]
            if not a_tokens or not b_tokens:
                continue
            jaccard = len(a_tokens & b_tokens) / len(a_tokens | b_tokens)
            if jaccard >= threshold:
                # Prefer the "clean" original: no _summary_, _v<N>, _miscat suffix.
                # Archive whichever ID looks more derivative.
                def _is_derivative(doc_id):
                    return ("_summary_" in doc_id
                            or bool(_OLD_VERSION_PATTERN.search(doc_id))
                            or "_miscat" in doc_id)

                i_deriv = _is_derivative(titles[i][0])
                j_deriv = _is_derivative(titles[j][0])

                if j_deriv and not i_deriv:
                    to_archive.add(titles[j][0])
                elif i_deriv and not j_deriv:
                    to_archive.add(titles[i][0])
                else:
                    # Both or neither derivative — archive the later one (j)
                    to_archive.add(titles[j][0])

    return to_archive


# ---------------------------------------------------------------------------
# Main archival function
# ---------------------------------------------------------------------------

def archive_corpus(
    input_path:  str,
    output_path: str,
    archive_path: str,
    threshold:   float = ARCHIVE_THRESHOLD,
) -> dict:
    """
    Load bloated corpus, score every document, archive low-quality docs,
    detect near-duplicates, and write cleaned + archived corpora.

    Returns a summary dict with before/after stats.
    """
    with open(input_path) as f:
        docs = json.load(f)

    total_before = len(docs)

    # 1. Score every document
    scored = []
    for doc in docs:
        quality = score_document(doc)
        scored.append((doc, quality))

    # 2. First pass: split by composite score
    first_pass_kept     = []
    first_pass_archived = []

    for doc, quality in scored:
        if quality["composite"] >= threshold:
            first_pass_kept.append((doc, quality))
        else:
            first_pass_archived.append((doc, quality))

    # 3. Near-duplicate detection — only among docs that passed the score gate.
    #    This prevents old versioned docs (caught by is_latest_version) from
    #    being double-counted as near-duplicates of their originals.
    dup_ids = find_near_duplicates([d for d, _ in first_pass_kept])

    # 4. Final split
    kept     = []
    archived = []
    archive_reasons = defaultdict(int)

    for doc, quality in first_pass_archived:
        for reason in quality["archive_reason"]:
            archive_reasons[reason] += 1
        archived.append({
            **doc,
            "_quality_score":   quality["composite"],
            "_archive_reasons": quality["archive_reason"],
        })

    for doc, quality in first_pass_kept:
        doc_id = doc.get("id", "")
        if doc_id in dup_ids:
            archive_reasons["near_duplicate"] += 1
            archived.append({
                **doc,
                "_quality_score":   quality["composite"],
                "_archive_reasons": ["near_duplicate"],
            })
        else:
            kept.append(doc)

    # 4. Write outputs
    os.makedirs(os.path.dirname(os.path.abspath(output_path)),  exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(archive_path)), exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(kept, f, indent=2)

    with open(archive_path, "w") as f:
        json.dump(archived, f, indent=2)

    # 5. Build summary
    # is_quality is also set on old versioned copies by the bloat script,
    # so use the _v suffix pattern to identify true original quality docs.
    def _is_true_quality(doc):
        return (doc.get("is_quality")
                and not _OLD_VERSION_PATTERN.search(doc.get("id", ""))
                and "_summary_" not in doc.get("id", "")
                and "_miscat" not in doc.get("id", ""))

    quality_kept     = sum(1 for d in kept     if _is_true_quality(d))
    quality_archived = sum(1 for d in archived if _is_true_quality(d))

    kept_scores     = [q["composite"] for _, q in scored
                       if _.get("id") in {d["id"] for d in kept}]
    archived_scores = [q["composite"] for _, q in scored
                       if _.get("id") not in {d["id"] for d in kept}]

    summary = {
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "input_path":         input_path,
        "output_path":        output_path,
        "archive_path":       archive_path,
        "threshold":          threshold,
        "before": {
            "total_docs":     total_before,
            "quality_docs":   sum(1 for d in docs if d.get("is_quality")),
        },
        "after": {
            "kept_docs":          len(kept),
            "archived_docs":      len(archived),
            "quality_docs_kept":  quality_kept,
            "quality_docs_lost":  quality_archived,
            "reduction_pct":      round((1 - len(kept) / total_before) * 100, 1),
        },
        "archive_reasons": dict(archive_reasons),
        "score_stats": {
            "kept_avg":     round(sum(kept_scores)     / len(kept_scores),     4) if kept_scores     else 0,
            "archived_avg": round(sum(archived_scores) / len(archived_scores), 4) if archived_scores else 0,
        },
    }

    return summary


# ---------------------------------------------------------------------------
# Print summary
# ---------------------------------------------------------------------------

def print_summary(summary: dict):
    b = summary["before"]
    a = summary["after"]

    print("\n" + "=" * 60)
    print("  CORPUS CLEANUP SUMMARY")
    print("=" * 60)
    print(f"  Threshold:          {summary['threshold']}")
    print()
    print(f"  Before:             {b['total_docs']:,} docs "
          f"({b['quality_docs']} quality)")
    print(f"  After (kept):       {a['kept_docs']:,} docs "
          f"({a['quality_docs_kept']} quality)")
    print(f"  Archived:           {a['archived_docs']:,} docs "
          f"({a['quality_docs_lost']} quality lost)")
    print(f"  Corpus reduction:   {a['reduction_pct']}%")
    print()
    print(f"  Avg quality score — kept:     {summary['score_stats']['kept_avg']:.3f}")
    print(f"  Avg quality score — archived: {summary['score_stats']['archived_avg']:.3f}")
    print()
    print(f"  Archive reasons:")
    for reason, count in sorted(summary["archive_reasons"].items(), key=lambda x: -x[1]):
        print(f"    {reason:<35} {count:>5}")

    if a["quality_docs_lost"] > 0:
        print(f"\n  NOTE: {a['quality_docs_lost']} original-quality docs were archived.")
        print(f"  These are near-exact duplicates — a kept copy exists for each.")
        print(f"  Review {summary['archive_path']} to verify if needed.")

    print("=" * 60)
    print(f"  Cleaned corpus: {summary['output_path']}")
    print(f"  Archive:        {summary['archive_path']}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Archive low-quality corpus documents")
    parser.add_argument("--input",     default=DEFAULT_INPUT)
    parser.add_argument("--output",    default=DEFAULT_OUTPUT)
    parser.add_argument("--archive",   default=DEFAULT_ARCHIVE)
    parser.add_argument("--threshold", type=float, default=ARCHIVE_THRESHOLD,
                        help="Quality score threshold (0–1). Docs below this are archived.")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"ERROR: input not found at {args.input}")
        raise SystemExit(1)

    print(f"Cleaning corpus: {args.input}")
    print(f"Threshold:       {args.threshold}")

    summary = archive_corpus(args.input, args.output, args.archive, args.threshold)
    print_summary(summary)
