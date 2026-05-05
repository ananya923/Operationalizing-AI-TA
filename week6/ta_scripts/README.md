# Week 6 TA Scripts

## Corpus Corruption

**Do NOT distribute this script or its output to students.**

### Setup

Before Week 6 starts, run:

```bash
python week6/ta_scripts/corrupt_corpus.py \
  --input data/raw/techcorp/documents.json \
  --output data/raw/techcorp/documents_week6_corrupted.json \
  --seed 42
```

Give students `documents_week6_corrupted.json` for their agent to load.

### What's Injected

- **~15% duplicates with conflicts:** Finance/HR documents have old versions (v1.0) with conflicting values mixed with current (v2.0). Example: Travel policy says $150/night in v2.0 but $300/night in v1.0 (old).
- **~10% missing fields:** Some documents have no `last_updated` field.
- **~8% access control issues:** Documents marked "[CONFIDENTIAL]" but not in `access_control.json`.
- **~20% regional variants:** Travel policies with conflicting rates for different regions.
- **50 junk documents:** Auto-generated low-quality filler documents.

### What Students Should Detect

**Drift symptoms:**
- Agent gives conflicting answers to "What's the hotel rate?" (sometimes $150, sometimes $300)
- Access control monitoring shows documents marked confidential but not filtered
- Latency increases as agent retrieves more irrelevant docs
- Cost increases due to agent having to reason through conflicting information

**Expected deliverables:**
- Script that identifies duplicates/conflicts
- Report listing problematic documents
- Updated monitoring dashboard showing drift metrics
- Tests verifying access control filters out conflicts

### Grading Focus

Look for:
- [ ] Student identified at least 3 major corpus issues
- [ ] Student detected drift via monitoring (confidence drop, increased tokens)
- [ ] Access control prevents conflicting documents from being returned
- [ ] Student didn't just patch symptoms—they fixed root cause (identified bad docs)
