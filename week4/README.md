# Week 4 — Monitoring, Drift Detection & Retraining Strategy

## Assignment

After Week 3 data quality fixes, your API resumed normal operation. But new data arriving February 2 – February 28, 2026 (weeks 4-6) shows performance degradation. The model is seeing different patterns. Your tasks:

1. Detect drift patterns (identify which features/segments changed)
2. Distinguish data drift from concept drift
3. Design a monitoring framework with specific metrics and thresholds
4. Design a retraining strategy (trigger conditions, pipeline, frequency)

**Deliverables:**
- Drift detection report (3-4 pages): 4 drift patterns identified with quantitative evidence
- Monitoring framework design (1-2 pages): metrics, thresholds, implementation approach
- Retraining strategy (1-2 pages): trigger conditions, pipeline, frequency, versioning
- Architecture diagram: monitoring → metrics → alerts → retraining → deployment
- Code (optional): `scripts/compute_monitoring_metrics.py`, `scripts/check_drift_thresholds.py`, updated CI/CD

---

## Part 1: Design Monitoring Framework

Specify metrics for:
- **Data drift:** How will you detect when feature distributions change? What thresholds?
- **Concept drift:** How will you detect when model performance degrades? Which segments matter?

Choose implementation: API endpoint, batch job, database, or combination.

Document at least 8 specific metrics you'll track, why each matters, and what thresholds trigger alerts.

---

## Part 2: Detect Drift

Using `demand_enriched_week4.parquet`, identify drift patterns. Compare training vs recent periods.

Find at least 4 specific drifts affecting different segments (zones, hours, days, boroughs). For each: what drifted, when it started, which segments, why it matters.

---

## Part 3: Design Retraining Strategy

Specify:
- **Trigger conditions:** What metrics/thresholds warrant retraining? How often?
- **Pipeline:** Steps from detecting drift to deploying new model. Who/what decides to promote?
- **Safeguards:** How do you prevent a worse model from going to production? How do you roll back?

Document the full workflow from metric monitoring → retraining decision → evaluation → deployment.

---

## Deliverables

Submit **via Canvas** (deadline end of Week 4):

1. **Drift Detection Report** (3-4 pages)
   - Detected 4 drift patterns with analysis
   - For each: what drifted, when, which segments, why, impact
   - Evidence: tables, plots, metrics showing the drift

2. **Monitoring Framework Design** (1-2 pages)
   - Metrics you'll track (list at least 8)
   - Where monitoring code lives (endpoint? database? job?)
   - Alert thresholds and what each means
   - Sample dashboard mockup (sketch or screenshot)

3. **Retraining Strategy** (1-2 pages)
   - Trigger conditions for retraining
   - Retraining pipeline (pseudocode or bash script outline)
   - Frequency/cadence (when do you retrain?)
   - How you handle model versioning and rollback
   - Data retention policy (how long to keep historical data?)

4. **Code** (if implementing monitoring)
   - `scripts/compute_monitoring_metrics.py` — computes drift metrics
   - `scripts/check_drift_thresholds.py` — checks if retraining needed
   - Updated `.github/workflows/` — adds retraining job (optional)
   - Tests for monitoring logic

5. **Architecture Diagram**
   - Updated system diagram showing: monitoring → metrics → alerts → retraining → new model → deployment

---

## Grading

| Criterion | Weight |
|-----------|--------|
| Drift detection (4 patterns with quantitative evidence) | 30% |
| Monitoring design (metrics, thresholds, implementation) | 25% |
| Retraining strategy (trigger conditions, pipeline, frequency) | 20% |
| Code (scripts work, tests pass) | 15% |
| Documentation (design choices, reasoning) | 10% |
