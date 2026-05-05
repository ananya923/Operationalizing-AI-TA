# Week 7 TA Scripts

## Corpus Bloat Simulation

**Do NOT distribute this script or its output to students.**

### Setup

Before Week 7 starts, run:

```bash
python week7/ta_scripts/corpus_bloat.py \
  --input data/raw/techcorp/documents.json \
  --output data/raw/techcorp/documents_week7_bloated.json \
  --num_junk 10000 \
  --seed 42
```

Give students `documents_week7_bloated.json` for their agent to load.

### What's Injected

- **10,000 junk documents:** Auto-generated filler with meaningless content ("Quarterly report on system maintenance. Status: pending. Action items: TBD.")
- **Outdated versions:** Old policy documents mixed in (Travel Policy v0.5 from 1 year ago alongside v2.0)
- **Signal-to-noise:** ~100 real quality documents + 10,400 junk = ~1% signal
- **Shuffled:** Real docs mixed randomly with junk (makes filtering hard)

### What Students Will Observe

**Performance degradation:**
- **Cost:** 3x increase (agent retrieves more docs, uses more tokens reasoning through noise)
- **Latency:** 2-4x increase (larger index, more documents to process)
- **Accuracy:** 20-30% drop (agent confused by conflicting/irrelevant docs)

**Baseline (before cleanup):**
- 10 evaluation queries: avg cost $0.0005, latency 1.2s, accuracy 85%

**After cleanup:**
- Same 10 queries: cost $0.00015, latency 0.4s, accuracy 95% (back to normal)

### What Students Should Do

1. **Detect bloat:**
   - Measure: avg tokens per query (baseline ~500, bloated ~1500)
   - Measure: latency by corpus size (should be linear, degradation signals problem)
   - Score docs: content length, has required fields, not auto-generated, recent date

2. **Archive junk:**
   - Remove auto-generated docs
   - Remove deprecated versions (keep only latest)
   - Filter by content quality

3. **Measure recovery:**
   - Graph: cost/latency/accuracy vs. corpus size before and after cleanup
   - Show recovery back to baseline

### Grading Focus

Look for:
- [ ] Student detected corpus bloat (measured tokens/latency/accuracy degradation)
- [ ] Student implemented quality scoring for documents
- [ ] Student archived/removed junk docs
- [ ] Recovery graphs show improvement after cleanup
- [ ] Student identified root cause (corpus size) not symptoms (high cost)

### Expected Student Work

**Strong response:**
- Corpus quality analysis tool that identifies junk docs
- Before/after metrics showing 3x cost reduction, 4x latency reduction
- Recovery graphs with clear inflection point (corpus size threshold)
- Understanding that document quality matters as much as quantity

**Weak response:**
- Just implemented cost optimization without addressing corpus
- No root cause analysis (corpus bloat) 
- No comparison of performance before/after cleanup
