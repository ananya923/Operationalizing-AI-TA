"""
Week 7 TA Script: Realistic corpus degradation at scale.

NOT just "add junk documents."

Real problems companies face:
- Documents that LOOK relevant but mislead (high retrieval rank, wrong answer)
- Duplicated content (doc_1 and doc_2 are 95% identical, both retrieved)
- Documents in wrong categories (retrieved as "Finance" but actually "Sales")
- Accumulated versions without cleanup (doc_v1, doc_v2, doc_v3, ... doc_v47)
- Generated summaries that lost critical nuance
- Documents written by non-experts (contractor docs mixed with official)
- Seasonal docs that should only apply certain months
- Incomplete documents (50% written, published by mistake)
- Documents that contradict current policy
- "Low-value but high-retrieval-frequency" docs that waste tokens

Students can't just delete everything "bad" — some junk is still useful context.
Real problem: What to KEEP, not what to DELETE.

Provided to TAs only.

Usage:
  python week7/ta_scripts/corpus_bloat.py \
    --input data/raw/techcorp/documents.json \
    --output data/raw/techcorp/documents_week7_bloated.json \
    --seed 42
"""

import json
import argparse
from datetime import datetime, timedelta
import random


def generate_misleading_doc(index):
    """Document that LOOKS relevant but leads to wrong answers."""
    templates = [
        {
            'title': f'Travel Policy Summary {index}',
            'content': 'Hotel rates vary by region. Check the full policy. No more details provided here.',
            'problem': 'Looks relevant but has no actual info (summary without substance)'
        },
        {
            'title': f'Expense Guidelines {index}',
            'content': 'All expenses require approval. Amounts vary by category. See policy doc for rates.',
            'problem': 'Circular reference, doesn\'t answer questions'
        },
        {
            'title': f'PTO Overview {index}',
            'content': '15-25 days depending on level. For exact amounts, see HR policy.',
            'problem': 'Vague, sends to other doc'
        },
    ]

    template = random.choice(templates)
    return {
        'id': f'misleading_{index}',
        'title': template['title'],
        'category': random.choice(['HR', 'Finance']),
        'sensitivity': 'Internal',
        'content': template['content'],
        'last_updated': (datetime.now() - timedelta(days=random.randint(30, 180))).isoformat(),
        'author': 'contractor',
        'version': '0.1 (draft)',
        'is_high_retrieval_waste': True,  # Gets retrieved often, but low value
        '_problem': template['problem']
    }


def generate_incomplete_doc(index):
    """Document that looks official but is 50% written."""
    sections = [
        'Travel Policy\n\n1. Flights\n   - Max price: $[PENDING]\n   - Approval: [TODO]\n2. Hotels\n   - [INCOMPLETE SECTION]',
        'Expense Policy\n\nDraft v0.8\n\nApproval Levels:\n- Under $500: [needs work]\n- $500-2k: Manager approval\n- [rest of section missing]',
        'Benefits Guide\n\nDRAFT - DO NOT USE FOR DECISIONS\n\nHealth Plans\nVision: [being updated]\nDental: [contact HR]\nRetirement: [incomplete]',
    ]
    return {
        'id': f'incomplete_{index}',
        'title': f'Draft Policy {index}',
        'category': 'Internal',
        'sensitivity': 'Internal',
        'content': random.choice(sections),
        'last_updated': (datetime.now() - timedelta(days=random.randint(1, 30))).isoformat(),
        'author': 'hr_intern',
        'version': '0.8 (incomplete)',
        '_problem': 'Unfinished document, published by mistake'
    }


def generate_duplicate_doc(original, index):
    """Near-duplicate that wastes retrieval slots."""
    doc = original.copy()
    doc['id'] = f"{original['id']}_summary_{index}"
    doc['title'] = original['title'] + " (Summary)"
    # Remove 50% of content to simulate summary
    lines = doc['content'].split('\n')
    doc['content'] = '\n'.join(lines[:len(lines)//2])
    doc['version'] = doc.get('version', '1.0') + '.summary'
    doc['author'] = 'auto_summary'
    return doc


def corpus_bloat(input_file, output_file, seed=42):
    random.seed(seed)

    with open(input_file, 'r') as f:
        docs = json.load(f)

    bloated = []

    # 1. Add originals with quality flag
    for doc in docs:
        doc['is_quality'] = True
        doc['_retrieval_frequency'] = random.randint(1, 100)  # Simulated
        bloated.append(doc)

    # 2. Accumulate versions: doc_v1, doc_v2, ... doc_v12 all in corpus
    for doc in docs[:30]:
        if random.random() < 0.6:
            for v in range(1, random.randint(3, 12)):
                old_version = doc.copy()
                old_version['id'] = f"{doc['id']}_v{v}"
                old_version['version'] = f'{v}.0'
                old_version['last_updated'] = (datetime.now() - timedelta(days=30*v)).isoformat()
                old_version['_problem'] = f'Version {v} (outdated, but still retrieved)'
                bloated.append(old_version)

    # 3. Near-duplicates (95% same content, waste retrieval slots)
    for doc in docs[5:15]:
        if random.random() < 0.7:
            dup = generate_duplicate_doc(doc, random.randint(1, 3))
            dup['_problem'] = 'Near-duplicate wastes retrieval rank'
            bloated.append(dup)

    # 4. Misleading high-retrieval-frequency docs (look relevant, aren't)
    for i in range(200):
        bloated.append(generate_misleading_doc(i))

    # 5. Incomplete/draft docs published by mistake
    for i in range(80):
        bloated.append(generate_incomplete_doc(i))

    # 6. Docs in wrong category
    for doc in random.sample(docs, min(20, len(docs))):
        miscat = doc.copy()
        miscat['id'] = f"{doc['id']}_miscat"
        original_cat = miscat['category']
        miscat['category'] = random.choice(['HR', 'Finance', 'Engineering', 'Operations'])
        while miscat['category'] == original_cat:
            miscat['category'] = random.choice(['HR', 'Finance', 'Engineering', 'Operations'])
        miscat['_problem'] = f'Categorized as {miscat["category"]}, should be {original_cat}'
        bloated.append(miscat)

    # 7. Seasonal docs (only valid Mar-May, but always retrieved)
    for i in range(50):
        bloated.append({
            'id': f'seasonal_conference_{i}',
            'title': f'Conference Travel Guidelines {i}',
            'category': 'Internal',
            'sensitivity': 'Internal',
            'content': 'Special rates for attendees. Valid only during conference season (Mar-May).',
            'last_updated': (datetime.now() - timedelta(days=random.randint(30, 300))).isoformat(),
            'author': 'events',
            'version': '1.0',
            'valid_only': 'Mar-May (but always in index)',
            '_problem': 'Seasonal but no temporal filtering'
        })

    # 8. Contradicting current policy
    for doc in random.sample(docs, min(15, len(docs))):
        if 'policy' in doc.get('content', '').lower():
            old_policy = doc.copy()
            old_policy['id'] = f"{doc['id']}_old_contradicts"
            old_policy['content'] = doc['content'].replace('$150', '$250').replace('5 days', '10 days')
            old_policy['version'] = '1.5 (contradicts current)'
            old_policy['_problem'] = 'Contradicts current policy but no clear indication'
            bloated.append(old_policy)

    # Shuffle to hide patterns
    random.shuffle(bloated)

    with open(output_file, 'w') as f:
        json.dump(bloated, f, indent=2)

    issues = {
        'misleading': sum(1 for d in bloated if d.get('is_high_retrieval_waste')),
        'incomplete': sum(1 for d in bloated if 'incomplete' in d.get('id', '')),
        'near_duplicates': sum(1 for d in bloated if 'summary' in d.get('id', '')),
        'versions': sum(1 for d in bloated if '_v' in d.get('id', '')),
        'miscategorized': sum(1 for d in bloated if 'miscat' in d.get('id', '')),
        'seasonal': sum(1 for d in bloated if d.get('valid_only')),
        'contradicting': sum(1 for d in bloated if 'contradicts' in d.get('id', '')),
    }

    print(f"\nCorpus bloated: {output_file}")
    print(f"Original quality docs: {len([d for d in bloated if d.get('is_quality')])}")
    print(f"Total docs: {len(bloated)}")
    print(f"\nRealistic problems injected:")
    print(f"  High-retrieval-waste (look relevant, aren't): {issues['misleading']}")
    print(f"  Incomplete/draft documents: {issues['incomplete']}")
    print(f"  Near-duplicates (waste retrieval slots): {issues['near_duplicates']}")
    print(f"  Version accumulation (v1-v12): {issues['versions']}")
    print(f"  Miscategorized documents: {issues['miscategorized']}")
    print(f"  Seasonal docs (temporal conflict): {issues['seasonal']}")
    print(f"  Contradicting current policy: {issues['contradicting']}")

    print(f"\nStudent challenge:")
    print(f"  - Can't just delete everything 'bad' (some is still context)")
    print(f"  - Misleading docs rank high in retrieval (hard to detect)")
    print(f"  - Duplicates waste token budget (detection requires similarity matching)")
    print(f"  - Version explosion (which version to keep? oldest? newest? none?)")
    print(f"  - Misclassification hidden (doc says 'Finance' but is 'HR')")
    print(f"  - No single 'right answer' on what to prune")
    print(f"\nMetrics to watch:")
    print(f"  - Cost per query (should drop after cleanup)")
    print(f"  - Answer correctness (should improve after cleanup)")
    print(f"  - Retrieval latency (should improve)")
    print(f"  - Documents retained vs removed (are they keeping too much or deleting useful context?)")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    corpus_bloat(args.input, args.output, args.seed)
