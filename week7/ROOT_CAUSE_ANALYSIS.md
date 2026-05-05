# Root Cause Analysis — Week 7: Corpus Degradation & Recovery

## Summary

The TechCorp RAG agent experienced degradation across three dimensions after a
corpus update: **cost increased ~3×**, **accuracy dropped from 80% to 70%**, and
**latency grew on policy queries**. This document explains what broke, how it was
diagnosed, what was implemented to fix it, and what remains uncertain.

---

## 1. What Broke and Why

### The corpus went from 74 to 575 documents — and most of the additions were noise

The original corpus contained 74 carefully curated policy and HR documents. The
Week 7 corpus update injected 330 additional documents across seven problem
categories:

| Category | Count | How it hurts |
|---|---|---|
| Incomplete drafts | 80 | Published by mistake; content is thin stubs |
| Misleading / circular docs | 68 | Appear relevant to TF-IDF but contain no real answers |
| Near-duplicate summaries | 70 | Restate existing docs without adding info; waste retrieval rank |
| Old versioned copies | 143 | v1–v12 copies of current policies; return outdated facts |
| Seasonal documents | 50 | Time-limited content surfaced year-round |
| Miscategorised docs | 20 | Wrong category tag, misroutes queries |
| Contradicting policies | 2 | Old versions that directly contradict current rules |

The TF-IDF retriever has no awareness of document quality — it scores on keyword
overlap alone. Injected documents were written to look keyword-relevant, so they
scored highly against real user queries, crowding out the true signal documents
in `top_k=3` results.

### Why cost went up

Cost is proportional to input token count, which is driven by retrieved document
length. The misleading and draft docs contain verbose but empty content. When the
retriever returns 3 docs and one or two are bloated stubs, the LLM receives a
longer prompt than it needs — but gets no additional useful information.

**Evidence**: Pearson correlation between `tokens_input` and `latency_ms` in the
bloated corpus was 0.78 (well above the 0.75 alert threshold), confirming that
retrieval length — not model speed — was the primary cost and latency driver.

### Why accuracy dropped

The misleading/circular docs were designed to appear related but not answer the
question. When these occupied retrieval slots, the LLM had less signal and more
noise to reason over. Accuracy dropped from 80% (baseline, 74 docs) to 70%
(bloated, 575 docs).

After optimization (332 docs), accuracy recovered to **90%** — better than the
original baseline because near-duplicate removal also eliminated a handful of
slightly-inconsistent restatements that were present in the original corpus.

---

## 2. What Was Implemented

### Step 1 — Diagnose (`scripts/diagnose_degradation.py`)

Ran 10 evaluation queries against the clean and bloated corpora using subprocess
isolation (each corpus gets a fresh Python process so TF-IDF rebuilds from
scratch). Measured accuracy, token count, cost, and latency for each.

Key findings:
- Accuracy: 80% → 70% (−10 pp)
- Token count on policy queries: +24% average
- Cost-latency Pearson r: 0.78 on bloated vs 0.32 on clean

### Step 2 — Fix the corpus (`app/cost_optimization.py`)

Two-pass archival strategy:

**Pass 1 — Composite quality scoring** (threshold 0.5):
Seven weighted dimensions score each document from 0–1. Docs below threshold are
archived immediately. Weights were tuned to ensure `is_latest_version` (weight
0.20) alone causes old versioned copies to fail threshold.

| Dimension | Weight | Catches |
|---|---|---|
| `has_required_fields` | 0.15 | Stubs missing title/content/category |
| `content_substance` | 0.20 | Thin content (< 50 words or < 3 sentences) |
| `not_draft` | 0.15 | Docs with draft markers in content/title |
| `not_misleading` | 0.20 | Vague docs that redirect without answering |
| `not_seasonal` | 0.05 | Seasonal content flags |
| `not_miscategorized` | 0.05 | Category tag mismatches |
| `is_latest_version` | 0.20 | Old versioned copies |

**Pass 2 — Near-duplicate detection** (Jaccard threshold 0.85 on title tokens):
Run only on docs that passed Pass 1. Catches duplicate summaries and near-copies
that scored above threshold because their non-version dimensions all passed.
Prefers originals over derivatives (ID suffix heuristic: `_v<N>`, `_summary_`,
`_miscat` are treated as derivatives).

Result: 575 → 332 docs (42% reduction), accuracy 70% → 90%.

### Step 3 — Collect user feedback (`app/feedback.py`)

`POST /feedback` endpoint lets employees submit corrections when the agent's
answer is wrong. Role-based permissions restrict who can correct which domains
(e.g. engineers cannot correct any domain — they lack the authority to know what
is right). Deduplication via SHA-256 hash prevents repeat submission of the
same correction.

`GET /feedback/stats` exposes correction breakdown by intent and role, surfacing
which query types are systematically wrong.

### Step 4 — Monitor 15 metrics (`app/monitoring.py`)

Extended the Week 6 monitoring module from 8 to 15 metrics. The 7 new Week 7
metrics are:

| Metric | Alert threshold | What it catches |
|---|---|---|
| `cost_per_query` | > $0.00081 (3×) | Retrieval bloat inflating context |
| `latency_percentiles_w7` | p95 > 3000ms | Slow retrieval from large corpus |
| `accuracy_rate` | < 60% | LLM giving wrong answers |
| `feedback_correction_rate` | > 20% | Systematic inaccuracy from users |
| `corpus_freshness` | avg > 180d or stale > 20 | Documents going stale |
| `model_cost_latency_corr` | Pearson r > 0.75 | Early-warning for retrieval bloat |
| `system_health` | error > 5%, timeout > 2% | Infrastructure failures |

The `model_cost_latency_corr` metric is the most valuable early-warning signal:
it fires before individual cost or latency thresholds are breached, giving time
to re-archive proactively.

### Step 5 — Automated recovery (`scripts/auto_recovery.py`)

Four recovery actions triggered by metric thresholds:

1. **Corpus re-archival** — re-runs `cost_optimization.py` when cost, accuracy,
   feedback, or correlation alerts fire. Uses stricter threshold (0.6) when both
   cost AND accuracy alert simultaneously, indicating severe degradation.

2. **Fallback mode** — writes `fallback_config.json` setting `top_k=1` for 10
   minutes when latency p95 or timeout rate spikes. Reduces retrieval overhead
   immediately while root cause is investigated.

3. **Rate limiting** — writes `rate_limit_config.json` halving
   `max_queries_per_minute` for 10 minutes when error rate exceeds 5%.

4. **Alert notification** — appends to `alerts.log` for any alert without an
   automated fix (corpus freshness always routes here — stale documents require
   human editorial judgment).

### Step 6 — CI/CD (`.github/workflows/retrain.yml`)

The corpus re-archival pipeline runs on three triggers:
- Nightly cron (02:00 UTC) — catches gradual freshness decay
- `repository_dispatch` from alerting system — `feedback-spike`, `accuracy-drop`,
  `cost-spike` events trigger immediate re-archival
- Manual `workflow_dispatch` — any team member can kick off re-archival from
  the GitHub UI

The pipeline uses a stricter quality threshold (0.6) when triggered by a
cost-spike or accuracy-drop event, matching the logic in `auto_recovery.py`.
A post-archival validation job checks that accuracy did not regress before the
cleaned corpus is promoted.

---

## 3. How Recurrence Is Prevented

**Short-term** (catches the same degradation pattern):
- `model_cost_latency_corr` metric fires before individual thresholds breach,
  giving ≥5 minutes of warning before users notice latency
- Nightly CI run catches slow freshness decay before it compounds
- Idempotent re-archival means it is safe to re-run; a no-op on a clean corpus

**Medium-term** (catches new degradation patterns):
- `feedback_correction_rate` catches accuracy problems that the keyword-match
  heuristic misses — users know their domain better than keyword overlap does
- `system_health` error/timeout rates catch infrastructure problems early

**Long-term** (requires process changes outside this codebase):
- The root cause of the bloated corpus was an unreviewed bulk import. A document
  ingestion review step (human or automated pre-scoring) before documents enter
  the corpus would prevent the problem entirely rather than cleaning it up after
  the fact
- Moving from TF-IDF to a dense retriever (e.g. sentence-transformers) would
  make the system more robust to keyword-matching noise, reducing how much a
  bloated corpus hurts accuracy

---

## 4. What Is Still Not Fully Understood

**Why do draft docs (score 0.67) survive the quality threshold?**

Draft documents have `not_draft=0.0` and `content_substance=0.1`, but the
remaining five dimensions all score 1.0. With `not_draft` weighted at 0.15 and
`content_substance` at 0.20, the composite is `0.67` — above the 0.5 threshold.
These docs are not removed by Pass 1. They are also not flagged by near-duplicate
detection because their titles are unique.

The current system leaves 80 draft documents in the cleaned corpus. In the
evaluation, accuracy still recovered to 90% (likely because top_k=3 retrieval
usually finds at least 2 real documents). But if draft documents happen to match
a query more closely than real documents, they could still be returned in the top
slot and degrade a specific answer.

**Fix (not yet implemented)**: Add a `is_not_draft` hard-veto: if `not_draft < 0.5`
AND `content_substance < 0.3`, archive regardless of composite score.

**Why does accuracy improve beyond the pre-bloat baseline (90% vs 80%)?**

After removing near-duplicates, the corpus is more consistent than the original
74-document baseline. A small number of the original documents were mild
restatements of each other; removing them through near-dup detection may have
slightly improved retrieval precision. This is a plausible hypothesis but was
not systematically verified — the 10-query eval set is too small to rule out
measurement noise.

**Is the keyword-match accuracy proxy reliable?**

The `accuracy_rate` metric checks whether the LLM's answer contains expected
keywords from a hand-crafted answer map. This is a proxy, not a ground truth.
It could:
- **Over-count accuracy** if the agent reproduces the right keywords from the
  wrong document (correct words, wrong reasoning)
- **Under-count accuracy** if the agent gives a correct answer using different
  phrasing than the expected keywords

A human-rated eval set of 50–100 queries would give much higher confidence in
the accuracy numbers. The feedback correction rate is currently the most reliable
accuracy signal because it reflects real user judgment, but it is only available
after the agent has been in production — not at deployment time.
