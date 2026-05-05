"""
Week 6 TA Script: Inject real-world corpus problems.

NOT simple duplicates. Real ambiguity:
- Context-dependent rules (different by region/role/date)
- Undocumented exceptions mixed with rules
- Circular policy references
- Implicit hierarchies (some rules override others, not documented)
- Historical artifacts that still apply to some employees
- Documents that contradict in subtle ways (both technically correct)
- Outdated examples embedded in current policy versions
- Field definition drift over time
- Access control that doesn't match intent

Students face the same problem production teams do:
- Is this document wrong or context-specific?
- Is this an exception or a new rule?
- Should I trust the version number or the timestamp?
- Which version applies to my query?

Provided to TAs only.

Usage:
  python week6/ta_scripts/corrupt_corpus.py \
    --input data/raw/techcorp/documents.json \
    --output data/raw/techcorp/documents_week6_corrupted.json \
    --seed 42
"""

import json
import argparse
from datetime import datetime, timedelta
import random


def inject_real_world_chaos(docs, seed=42):
    random.seed(seed)
    corrupted = []

    for doc in docs:
        # 1. CIRCULAR REFERENCES: Policy A cites Policy B which cites Policy A
        if 'Travel' in doc.get('title', '') and random.random() < 0.15:
            if 'expense policy' in doc['content'].lower():
                doc['content'] += "\n\nFor exception approval, see Approval Policy (doc_id: approval_policy_1). For travel expenses exceeding limits, see Travel Policy."

        if 'Approval' in doc.get('title', '') and random.random() < 0.15:
            if 'approval' in doc['content'].lower():
                doc['content'] += "\n\nFor travel-specific approvals, consult Travel Policy. For general approvals, see Approval Policy."

        # 2. CONTEXT-DEPENDENT RULES: Same rule, different by region but not explicit
        if 'Travel' in doc.get('title', '') and 'Policy' in doc.get('title', ''):
            variants = [
                ("US domestic flights: max $3,000. European flights: max $5,000. Asia-Pacific: max $8,000.", "Region-specific but unmarked"),
                ("Software engineers: $1,500 monthly limit. Sales: $2,500. Executives: no limit.", "Role-specific but reads like general rule"),
                ("Applies to all employees hired after 2024. Legacy employees (pre-2024) see old policy.", "Cohort-specific, embedded in example"),
            ]
            rule, context = random.choice(variants)
            if random.random() < 0.25:
                doc['content'] = rule + "\n\n" + doc['content']

        # 3. UNDOCUMENTED EXCEPTIONS: Rule + one-off exception buried in text
        if random.random() < 0.18:
            if 'policy' in doc['content'].lower() or 'rule' in doc['content'].lower():
                exceptions = [
                    "Exception: Marketing team received special approval for Q2 2025 to exceed budget.",
                    "NOTE: John's team approved exception to standard 5-day approval period (approved by CEO on Jan 10).",
                    "Legacy: This rule didn't apply before July 2023. Check grandfathering clause.",
                    "Approved variance: Engineering gets +20% travel budget for conference season (Mar-May).",
                ]
                doc['content'] += "\n\n" + random.choice(exceptions)

        # 4. IMPLICIT HIERARCHY: Multiple versions that both apply, but hierarchically
        if doc['category'] in ['Finance', 'HR'] and random.random() < 0.2:
            hierarchy = {
                'version': doc.get('version', '2.0'),
                'applies_if': random.choice([
                    'employee level >= manager',
                    'hire_date < 2023',
                    'department in [Engineering, Sales]',
                    'this is the most specific policy for your case'
                ]),
                'note': '(hierarchy implicit, not documented)'
            }
            doc['hierarchy'] = hierarchy

        # 5. OUTDATED EXAMPLES IN CURRENT DOCS: Version 2.0 is current but has old examples
        if doc.get('version') and '2.' in doc.get('version', '') and random.random() < 0.15:
            old_examples = [
                "\nExample (2022): Flights cost $400, hotels $120/night. [NOTE: Prices outdated, see current rate card]",
                "\nHistorical: Before Q1 2024, approval took 2 weeks. Now it's 5 days.",
                "\nLegacy rate: Old employees under contract see $1500 base. New employees see $2000.",
            ]
            doc['content'] += random.choice(old_examples)

        # 6. FIELD DEFINITION DRIFT: What "approved" means changed over time
        if 'Finance' in doc.get('category', '') and random.random() < 0.12:
            doc['_metadata_note'] = "Field 'status' changed meaning: v1.0='sent to finance', v2.0='approved by manager', v3.0='fully approved and paid'"

        # 7. CONTRADICTIONS THAT ARE BOTH CORRECT: Different perspectives on same rule
        if random.random() < 0.08:
            contradictions = [
                ("HR says: 'Unlimited PTO policy.' Finance says: 'Managers must approve usage >30 days.'", "Both true but create confusion"),
                ("Engineering docs: 'Work from anywhere.' Office policy: 'Must be in office 3 days/week.'", "Unclear which applies"),
                ("Policy says: 'No alcohol on company events.' Legacy doc says: 'Wine/beer allowed for team dinners.'", "One is outdated but still in corpus"),
            ]
            if random.random() < 0.5:
                contradiction, note = random.choice(contradictions)
                doc['content'] += f"\n\n[INTERNAL CONFLICT: {note}]\n{contradiction}"

        # 8. TEMPORAL VALIDITY: Document is correct but only applies certain dates
        if random.random() < 0.1:
            temporal = [
                f"Valid: {(datetime.now() - timedelta(days=30)).date()} to {(datetime.now() + timedelta(days=60)).date()}",
                f"Seasonal: Only applies Mar-May and Sept-Nov",
                f"Cohort: Applies only to employees hired between 2024-01-01 and 2024-12-31",
            ]
            doc['valid_period'] = random.choice(temporal)

        # 9. MISSING CRITICAL METADATA: No version, no date, no owner - unclear if current
        if random.random() < 0.08:
            if 'last_updated' in doc:
                del doc['last_updated']
            if 'version' in doc:
                del doc['version']

        # 10. DOCUMENT THAT IS CORRECT BUT MISLEADING: Not wrong, but leads to wrong decisions
        if random.random() < 0.12:
            if doc['category'] in ['Finance', 'HR']:
                doc['content'] += "\n\n[DISCLAIMER] This document is accurate but insufficient for final decisions. Always verify with manager/finance before acting."

        corrupted.append(doc)

    return corrupted


def main(input_file, output_file, seed=42):
    with open(input_file, 'r') as f:
        docs = json.load(f)

    corrupted = inject_real_world_chaos(docs, seed)

    with open(output_file, 'w') as f:
        json.dump(corrupted, f, indent=2)

    issues_found = {
        'circular_references': sum(1 for d in corrupted if 'CIRCULAR' in str(d.get('content', ''))),
        'context_dependent': sum(1 for d in corrupted if d.get('applies_if')),
        'undocumented_exceptions': sum(1 for d in corrupted if 'Exception:' in str(d.get('content', ''))),
        'implicit_hierarchies': sum(1 for d in corrupted if d.get('hierarchy')),
        'outdated_examples': sum(1 for d in corrupted if '[NOTE:' in str(d.get('content', ''))),
        'conflicting': sum(1 for d in corrupted if 'INTERNAL CONFLICT' in str(d.get('content', ''))),
        'temporal_validity': sum(1 for d in corrupted if d.get('valid_period')),
        'misleading_but_correct': sum(1 for d in corrupted if 'DISCLAIMER' in str(d.get('content', ''))),
    }

    print(f"Corpus corrupted: {output_file}")
    print(f"\nReal-world problems injected:")
    print(f"  Circular references: {issues_found['circular_references']}")
    print(f"  Context-dependent rules (region/role/date-specific): {issues_found['context_dependent']}")
    print(f"  Undocumented exceptions: {issues_found['undocumented_exceptions']}")
    print(f"  Implicit hierarchies: {issues_found['implicit_hierarchies']}")
    print(f"  Outdated examples in current docs: {issues_found['outdated_examples']}")
    print(f"  Internal conflicts (both technically correct): {issues_found['conflicting']}")
    print(f"  Temporal validity constraints: {issues_found['temporal_validity']}")
    print(f"  Correct but misleading: {issues_found['misleading_but_correct']}")

    print(f"\nStudent challenge:")
    print(f"  - Not all problems are obvious (some are subtle context issues)")
    print(f"  - Some 'conflicts' are actually correct (different perspectives)")
    print(f"  - No single 'right answer' on what to filter")
    print(f"  - Must build monitoring to detect which docs cause errors, not just flag suspicious ones")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    main(args.input, args.output, args.seed)
