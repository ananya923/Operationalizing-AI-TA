"""
Week 6 TA Script: Inject real-world corpus problems (IMPROVED VERSION).

Extends the original script with:
- Reliable triggering (probability-adjusted for corpus size)
- ~15% duplicate docs with conflicting versions (v1.0 vs v2.0 with different values)
- ~10% missing metadata fields (no last_updated / version)
- ~8% access control mismatches ([CONFIDENTIAL] tag but not in access_control.json)
- ~20% regional travel policy variants with conflicting rates
- 50 junk/filler documents
- All original corruption types, now with corpus-aware triggers

The script adds four entirely new corruption types from the README that weren't implemented at all: conflicting v1.0 legacy duplicates of the key policy docs (Travel, Compensation, PTO, Remote Work) with old dollar values that contradict the current versions, access control mismatches where docs get marked [CONFIDENTIAL] without being added to access_control.json, regional policy addenda with conflicting rates (APAC/EMEA/LATAM/US-East), and 50 junk filler documents.

For the original 10 corruption types, the main fix was adjusting probabilities upward for small eligibility pools — e.g. circular references had a 15% chance on only 7 docs (expected 1 hit), now it's 60% (expected 4 hits). The eligibility logic was also rewritten to use doc metadata (category, title keywords, version prefix) rather than brittle content string matching.
Expected output: 128 total docs (74 original + 4 legacy duplicates + 50 junk), with all corruption types visibly represented.

NOT for distribution to students.

Usage:
  python3 week6/ta_scripts/corrupt_corpus_improved.py \\
    --input data/raw/techcorp/documents.json \\
    --output data/raw/techcorp/documents_week6_corrupted_improved.json \\
    --seed 42
"""

import json
import copy
import argparse
import random
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pick(lst, rng):
    return lst[rng.randint(0, len(lst) - 1)]


# ---------------------------------------------------------------------------
# NEW: Duplicate docs with conflicting version values (~15% of corpus)
# ---------------------------------------------------------------------------

CONFLICT_PATCHES = {
    "doc_fin_001": {
        "old_version": "1.0",
        "old_date": "2021-03-15",
        "replacements": [
            ("$3,000/trip", "$6,000/trip"),
            ("$5,000/trip", "$9,000/trip"),
            ("$10,000/trip", "$15,000/trip"),
            ("7 days in advance", "14 days in advance"),
        ],
        "note": "v1.0 policy — superseded by v2.5 but still in corpus",
    },
    "doc_hr_002": {
        "old_version": "1.0",
        "old_date": "2020-01-10",
        "replacements": [
            ("$60,000 - $85,000", "$45,000 - $65,000"),
            ("$85,000 - $120,000", "$65,000 - $95,000"),
            ("$120,000 - $160,000", "$95,000 - $130,000"),
            ("20-50%", "10-30%"),
        ],
        "note": "v1.0 salary bands — outdated, replaced by v3.1",
    },
    "doc_hr_003": {
        "old_version": "1.0",
        "old_date": "2019-06-01",
        "replacements": [
            ("15 days/year", "10 days/year"),
            ("20 days/year", "15 days/year"),
            ("25 days/year", "18 days/year"),
            ("10 sick days", "5 sick days"),
        ],
        "note": "v1.0 PTO policy — legacy, replaced by v1.2",
    },
    "doc_ops_001": {
        "old_version": "1.0",
        "old_date": "2020-09-01",
        "replacements": [
            ("3 days in office", "5 days in office"),
            ("10am-3pm PT", "9am-5pm PT"),
        ],
        "note": "v1.0 remote work policy — pre-pandemic rules, replaced by v2.0",
    },
}


def inject_conflicting_duplicates(docs, rng):
    """
    Insert a legacy v1.0 copy (with old values) immediately after each patched doc.
    Targets all docs listed in CONFLICT_PATCHES — gives ~5% of original corpus.
    """
    legacy_map = {}
    for doc in docs:
        if doc["id"] not in CONFLICT_PATCHES:
            continue
        patch = CONFLICT_PATCHES[doc["id"]]
        old_doc = copy.deepcopy(doc)
        old_doc["id"] = doc["id"] + "_v1_legacy"
        old_doc["version"] = patch["old_version"]
        old_doc["last_updated"] = patch["old_date"]
        old_doc["_conflict_note"] = patch["note"]
        content = old_doc["content"]
        for current_val, old_val in patch["replacements"]:
            content = content.replace(current_val, old_val)
        old_doc["content"] = content
        legacy_map[doc["id"]] = old_doc

    result = []
    for doc in docs:
        result.append(doc)
        if doc["id"] in legacy_map:
            result.append(legacy_map[doc["id"]])
    return result


# ---------------------------------------------------------------------------
# NEW: Access control mismatches (~8% of corpus)
# ---------------------------------------------------------------------------


def inject_access_control_mismatches(docs, rng):
    """
    Mark ~8% of non-Confidential docs as [CONFIDENTIAL] in content/title
    without adding them to access_control.json — they slip through filters.
    """
    eligible = [d for d in docs if d.get("sensitivity") != "Confidential"]
    n_target = max(1, int(len(docs) * 0.08))
    targets = {d["id"] for d in rng.sample(eligible, min(n_target, len(eligible)))}

    for doc in docs:
        if doc["id"] in targets:
            doc["title"] = "[CONFIDENTIAL] " + doc["title"]
            doc["content"] = (
                "[CONFIDENTIAL — restricted distribution]\n\n" + doc["content"]
            )
            doc["_access_control_issue"] = (
                "Marked confidential in content but missing from access_control.json"
            )
    return docs


# ---------------------------------------------------------------------------
# NEW: Regional travel policy variants (~20% of corpus)
# ---------------------------------------------------------------------------

REGIONAL_VARIANTS = [
    (
        "APAC",
        "Asia-Pacific Regional Addendum: Hotel cap is $250/night (not $150). "
        "Flight budget +40% for APAC routes. Approval threshold: $2,000 (not $1,000).",
    ),
    (
        "EMEA",
        "EMEA Regional Addendum: Hotel cap is $200/night. Rail travel preferred over flights "
        "for journeys under 3 hours. Meal per diem: €60/day.",
    ),
    (
        "LATAM",
        "LATAM Regional Addendum: All expenses in USD. Hotel cap $120/night. "
        "Local transport reimbursed up to $30/day. Approval required for trips > $800.",
    ),
    (
        "US-East",
        "US-East Addendum: NYC hotel cap $300/night (exception approved for high-cost market). "
        "Domestic flights must use preferred carrier (Delta/United).",
    ),
]


def inject_regional_variants(docs, rng):
    """
    Append a regional rate addendum to ~20% of all docs (not just Travel docs,
    since any policy doc can have regional footnotes).
    """
    n_target = max(1, int(len(docs) * 0.20))
    targets = {d["id"] for d in rng.sample(docs, min(n_target, len(docs)))}

    for doc in docs:
        if doc["id"] in targets:
            region, addendum = _pick(REGIONAL_VARIANTS, rng)
            doc[
                "content"
            ] += f"\n\n---\n**{region} Regional Policy Addendum**\n{addendum}"
            doc["_regional_variant"] = region
    return docs


# ---------------------------------------------------------------------------
# NEW: Junk / filler documents (50 docs)
# ---------------------------------------------------------------------------

JUNK_TEMPLATES = [
    {
        "title": "Untitled Document {n}",
        "category": "Internal",
        "content": "Draft. Work in progress. See owner for details. TBD.",
        "sensitivity": "Internal",
    },
    {
        "title": "Meeting Notes {n}",
        "category": "Internal",
        "content": "Attendees: TBD\nAgenda: TBD\nAction items: follow up with stakeholders.",
        "sensitivity": "Internal",
    },
    {
        "title": "Policy Reference {n}",
        "category": "HR",
        "content": "Please refer to the main HR policy document for details. This document is a placeholder.",
        "sensitivity": "Internal",
    },
    {
        "title": "Expense Guidelines {n}",
        "category": "Finance",
        "content": "Contact finance@techcorp.com. See policy. Limits apply. Standard rates.",
        "sensitivity": "Internal",
    },
    {
        "title": "Process Doc {n}",
        "category": "Operations",
        "content": "Step 1: TBD. Step 2: TBD. Step 3: Escalate to manager. This process is under review.",
        "sensitivity": "Internal",
    },
]


def inject_junk_documents(docs, n=50, rng=None):
    """Append n low-quality filler documents."""
    junk = []
    for i in range(n):
        template = copy.deepcopy(_pick(JUNK_TEMPLATES, rng))
        template["id"] = f"doc_junk_{i:03d}"
        template["title"] = template["title"].format(n=i)
        template["version"] = "0.1"
        template["last_updated"] = "2023-01-01"
        template["author"] = "auto-generated"
        template["_is_junk"] = True
        junk.append(template)
    return docs + junk


# ---------------------------------------------------------------------------
# ORIGINAL corruptions — probabilities tuned for this corpus size
# ---------------------------------------------------------------------------


def inject_original_corruptions(docs, rng):
    """
    All 10 original corruption types, rewritten with:
    - Broader eligibility pools (not brittle string matches)
    - Higher probabilities where pool sizes are small, to guarantee visible hits
    """
    corrupted = []

    # Build eligibility sets from actual doc metadata
    travel_policy_ids = {
        d["id"]
        for d in docs
        if "Travel" in d.get("title", "") or "Policy" in d.get("title", "")
    }
    approval_ids = {
        d["id"]
        for d in docs
        if "Approval" in d.get("title", "") or "Guidelines" in d.get("title", "")
    }
    finance_hr_ids = {d["id"] for d in docs if d.get("category") in ("Finance", "HR")}
    versioned_ids = {d["id"] for d in docs if d.get("version", "").startswith("2.")}
    # Broaden Finance to include any doc with numeric/dollar content for field drift
    dollar_content_ids = {d["id"] for d in docs if "$" in d.get("content", "")}

    circular_addendum = (
        "For exception approval, see Approval Policy (doc_id: approval_policy_1). "
        "For travel expenses exceeding limits, consult the Travel Policy."
    )
    approval_addendum = (
        "For travel-specific approvals, consult Travel Policy. "
        "For general approvals, see Approval Policy (this document)."
    )

    context_variants = [
        "US domestic flights: max $3,000. European flights: max $5,000. Asia-Pacific: max $8,000.",
        "Software engineers: $1,500 monthly limit. Sales: $2,500. Executives: no limit.",
        "Applies to all employees hired after 2024. Legacy employees (pre-2024) see old policy.",
    ]

    exceptions = [
        "Exception: Marketing team received special approval for Q2 2025 to exceed budget.",
        "NOTE: John's team approved exception to standard 5-day approval period (approved by CEO on Jan 10).",
        "Legacy: This rule didn't apply before July 2023. Check grandfathering clause.",
        "Approved variance: Engineering gets +20% travel budget for conference season (Mar-May).",
    ]

    old_examples = [
        "\nExample (2022): Flights cost $400, hotels $120/night. [NOTE: Prices outdated, see current rate card]",
        "\nHistorical: Before Q1 2024, approval took 2 weeks. Now it's 5 days.",
        "\nLegacy rate: Old employees under contract see $1,500 base. New employees see $2,000.",
    ]

    contradictions = [
        (
            "HR says: 'Unlimited PTO policy.' Finance says: 'Managers must approve usage >30 days.'",
            "Both true but create confusion",
        ),
        (
            "Engineering docs: 'Work from anywhere.' Office policy: 'Must be in office 3 days/week.'",
            "Unclear which applies",
        ),
        (
            "Policy says: 'No alcohol on company events.' Legacy doc says: 'Wine/beer allowed for team dinners.'",
            "One is outdated but still in corpus",
        ),
    ]

    temporal_options = [
        f"Valid: {(datetime.now() - timedelta(days=30)).date()} to {(datetime.now() + timedelta(days=60)).date()}",
        "Seasonal: Only applies Mar-May and Sept-Nov",
        "Cohort: Applies only to employees hired between 2024-01-01 and 2024-12-31",
    ]

    for doc in docs:
        doc_id = doc["id"]

        # 1. Circular references
        # Pool: 7 travel/policy docs. Use 60% to guarantee ~4 hits.
        if doc_id in travel_policy_ids and rng.random() < 0.60:
            doc["content"] += "\n\n" + circular_addendum
        # Pool: 16 approval/guidelines docs. Use 20% → ~3 hits.
        if doc_id in approval_ids and rng.random() < 0.20:
            doc["content"] += "\n\n" + approval_addendum

        # 2. Context-dependent rules
        # Pool: 7 travel/policy docs. Use 60% → ~4 hits.
        if doc_id in travel_policy_ids and rng.random() < 0.60:
            doc["content"] = _pick(context_variants, rng) + "\n\n" + doc["content"]

        # 3. Undocumented exceptions — 18% of all docs (was fine, keeping)
        if rng.random() < 0.18:
            doc["content"] += "\n\n" + _pick(exceptions, rng)

        # 4. Implicit hierarchy
        # Pool: 5 Finance/HR docs. Use 80% → ~4 hits.
        if doc_id in finance_hr_ids and rng.random() < 0.80:
            doc["hierarchy"] = {
                "version": doc.get("version", "2.0"),
                "applies_if": _pick(
                    [
                        "employee level >= manager",
                        "hire_date < 2023",
                        "department in [Engineering, Sales]",
                        "this is the most specific policy for your case",
                    ],
                    rng,
                ),
                "note": "(hierarchy implicit, not documented)",
            }

        # 5. Outdated examples in current docs
        # Pool: 4 v2.x docs. Use 80% → ~3 hits.
        if doc_id in versioned_ids and rng.random() < 0.80:
            doc["content"] += _pick(old_examples, rng)

        # 6. Field definition drift
        # Broaden pool: any doc with $ in content (not just Finance). Use 8%.
        if doc_id in dollar_content_ids and rng.random() < 0.08:
            doc["_metadata_note"] = (
                "Field 'status' changed meaning: v1.0='sent to finance', "
                "v2.0='approved by manager', v3.0='fully approved and paid'"
            )

        # 7. Internal contradictions — 8% of all docs (was fine, keeping)
        if rng.random() < 0.08:
            contradiction, note = _pick(contradictions, rng)
            doc["content"] += f"\n\n[INTERNAL CONFLICT: {note}]\n{contradiction}"

        # 8. Temporal validity — 10% of all docs (was fine, keeping)
        if rng.random() < 0.10:
            doc["valid_period"] = _pick(temporal_options, rng)

        # 9. Missing critical metadata — 10% of all docs (was fine, keeping)
        if rng.random() < 0.10:
            doc.pop("last_updated", None)
            doc.pop("version", None)

        # 10. Correct but misleading
        # Pool: 5 Finance/HR docs. Use 60% → ~3 hits.
        if doc_id in finance_hr_ids and rng.random() < 0.60:
            doc["content"] += (
                "\n\n[DISCLAIMER] This document is accurate but insufficient for final decisions. "
                "Always verify with manager/finance before acting."
            )

        corrupted.append(doc)

    return corrupted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(input_file, output_file, seed=42):
    rng = random.Random(seed)

    with open(input_file, "r") as f:
        docs = json.load(f)

    original_count = len(docs)
    print(f"Input: {original_count} documents")

    # Apply all corruption passes
    docs = inject_conflicting_duplicates(docs, rng)
    docs = inject_access_control_mismatches(docs, rng)
    docs = inject_regional_variants(docs, rng)
    docs = inject_original_corruptions(docs, rng)
    docs = inject_junk_documents(docs, n=50, rng=rng)

    with open(output_file, "w") as f:
        json.dump(docs, f, indent=2)

    # Summary
    n = len(docs)
    legacy = [d for d in docs if "_conflict_note" in d]
    ac_issues = [d for d in docs if "_access_control_issue" in d]
    regional = [d for d in docs if "_regional_variant" in d]
    junk = [d for d in docs if d.get("_is_junk")]
    circular = [
        d
        for d in docs
        if "For exception approval, see Approval Policy" in str(d.get("content", ""))
        or "For travel-specific approvals" in str(d.get("content", ""))
    ]
    context_dep = [
        d
        for d in docs
        if any(
            x in str(d.get("content", ""))
            for x in [
                "US domestic flights",
                "Software engineers:",
                "Applies to all employees hired after",
            ]
        )
    ]
    exceptions_ = [
        d
        for d in docs
        if any(
            x in str(d.get("content", ""))
            for x in ["Exception:", "NOTE: John", "Legacy:", "Approved variance:"]
        )
    ]
    hierarchies = [d for d in docs if d.get("hierarchy")]
    outdated = [
        d
        for d in docs
        if any(
            x in str(d.get("content", ""))
            for x in ["[NOTE:", "Historical:", "Legacy rate:"]
        )
    ]
    field_drift = [d for d in docs if d.get("_metadata_note")]
    conflicts = [d for d in docs if "INTERNAL CONFLICT" in str(d.get("content", ""))]
    temporal = [d for d in docs if d.get("valid_period")]
    missing_meta = [d for d in docs if "last_updated" not in d or "version" not in d]
    misleading = [d for d in docs if "DISCLAIMER" in str(d.get("content", ""))]

    print(f"\nCorpus corrupted → {output_file}")
    print(f"Total documents: {n} (was {original_count})\n")

    print("=== README-specified corruptions ===")
    print(
        f"  Conflicting duplicate versions:    {len(legacy):3d}  ({len(legacy)/n*100:.1f}%)"
    )
    print(
        f"  Access control mismatches:         {len(ac_issues):3d}  ({len(ac_issues)/n*100:.1f}%)"
    )
    print(
        f"  Regional policy variants:          {len(regional):3d}  ({len(regional)/n*100:.1f}%)"
    )
    print(
        f"  Junk documents:                    {len(junk):3d}  ({len(junk)/n*100:.1f}%)"
    )

    print("\n=== Original corruption types ===")
    print(
        f"  Circular references:               {len(circular):3d}  ({len(circular)/n*100:.1f}%)"
    )
    print(
        f"  Context-dependent rules:           {len(context_dep):3d}  ({len(context_dep)/n*100:.1f}%)"
    )
    print(
        f"  Undocumented exceptions:           {len(exceptions_):3d}  ({len(exceptions_)/n*100:.1f}%)"
    )
    print(
        f"  Implicit hierarchies:              {len(hierarchies):3d}  ({len(hierarchies)/n*100:.1f}%)"
    )
    print(
        f"  Outdated examples:                 {len(outdated):3d}  ({len(outdated)/n*100:.1f}%)"
    )
    print(
        f"  Field definition drift:            {len(field_drift):3d}  ({len(field_drift)/n*100:.1f}%)"
    )
    print(
        f"  Internal conflicts:                {len(conflicts):3d}  ({len(conflicts)/n*100:.1f}%)"
    )
    print(
        f"  Temporal validity:                 {len(temporal):3d}  ({len(temporal)/n*100:.1f}%)"
    )
    print(
        f"  Missing metadata fields:           {len(missing_meta):3d}  ({len(missing_meta)/n*100:.1f}%)"
    )
    print(
        f"  Correct but misleading:            {len(misleading):3d}  ({len(misleading)/n*100:.1f}%)"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    main(args.input, args.output, args.seed)
