# Week 7 — Production Operations, Cost Optimization & Real-Time Adaptation

## Assignment

The Week 5 agent has been in production for 3 months. Three problems at once:

1. **Cost tripled.** Budget was $1,500/month. Now it's $4,500/month. CFO is asking why.
2. **Speed degraded.** Queries that took 0.5s now take 2-3s. Some users timing out.
3. **Accuracy dropped.** 30% of queries now get wrong answers. Users are correcting the agent more.

Your job: Diagnose what's broken. Fix the three problems. Build systems to prevent regression.

Available: Weeks 5-6 agent code, Week 2 GKE, `data/raw/techcorp/documents_week7_bloated.json` (corpus), feedback dataset.

## Deliverables

Submit on Canvas by end of Week 7:

### 1. Diagnose the Problems

Use your Week 6 monitoring to investigate:
- What changed since last week?
- Compare latency, cost, accuracy week-by-week
- Break down cost: where are tokens going?
- Segment by query type: which queries degraded most?
- Segment by document: which docs are being retrieved for failing queries?

Don't assume. Measure. Hypothesize. Test.

File: `scripts/diagnose_degradation.py` (pull metrics, segment data, generate analysis report)

### 2. Implement Cost Optimization

Once you understand where cost is coming from, implement optimizations:
- Examples: cascade inference, caching, retrieval optimization, reasoning simplification
- Measure impact: cost reduction, accuracy impact, latency impact
- Document tradeoff: what quality do you lose?

Only implement what data shows will help YOUR problem (not generic optimization).

Files: `app/cost_optimization.py`, `scripts/measure_optimization_impact.py`

### 3. Build Real-Time Feedback Loop

Users are correcting the agent. Capture that feedback, learn from it:
- Endpoint to collect user corrections
- Validate corrections (only certain roles can correct certain domains)
- Integrate corrections into corpus (or log for later review)
- Track: which queries get corrected? Are corrections accurate? Do they help future queries?

Files: `app/feedback.py`, `scripts/feedback_metrics.py`

### 4. Implement Monitoring & Alerting

Add 8 metrics to catch problems early (before CFO complains):

1. Cost per query (with threshold)
2. Latency percentiles (p50, p95, p99)
3. Accuracy/correctness rate
4. Feedback correction rate (% of queries corrected)
5. Document retrieval metrics (coverage, freshness)
6. Model performance metrics (cost/latency correlation)
7. System health metrics (errors, timeouts, retries)
8. Your choice: what else signals trouble?

For each: define baseline, threshold, what triggers alert, and what to do about it.

Create runbooks: when alert fires, what are the diagnostics steps?

Files: `app/monitoring.py`, `scripts/alert_runbook.py`, `k8s/alert-rules.yaml`

### 5. Implement Recovery Mechanisms

When you detect problems, what happens automatically?

Examples:
- Retraining pipeline (triggered by feedback spike)
- Automatic document archival (triggered by latency spike)
- Fallback to simpler queries (triggered by cost spike)
- Rate limiting (triggered by error rate spike)

You choose what makes sense for YOUR diagnoses.

Files: `.github/workflows/retrain.yml`, `scripts/auto_recovery.py`

### 6. Document Root Causes & Solutions

Write a report (not a design doc):
- What broke and why (your hypothesis + evidence)
- What you implemented to fix it (code + metrics)
- How you'll prevent it from happening again (monitoring + automation)
- What you still don't fully understand (honest assessment)

File: `ROOT_CAUSE_ANALYSIS.md`

## Testing

Write tests for:
- Cost optimization reduces costs (measure before/after)
- Feedback collection works (verify corrections are logged)
- Alerts fire when metrics cross thresholds
- Recovery mechanisms trigger on alerts

Files: `tests/test_optimization.py`, `tests/test_monitoring.py`, `tests/test_recovery.py`

## Grading

| Criterion | Weight |
|-----------|--------|
| Diagnosed root causes (cost, latency, accuracy) | 25% |
| Implemented fixes (optimization, feedback, recovery) | 25% |
| Monitoring & alerts (8 metrics, runbooks working) | 25% |
| Tests & deployment | 15% |
| Clear root cause analysis (honest about unknowns) | 10% |

## Submission

- Live agent endpoint with fixes deployed
- GitHub repo with all code, tests, CI/CD
- Before/after metrics (show improvement)
- Root cause analysis document
- Monitoring dashboard screenshot
- Alert examples (screenshots of alerts firing)
- Test results (pytest output)

Due: end of Week 7
