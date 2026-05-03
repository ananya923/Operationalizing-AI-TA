# Week 4 TA Notes — Drift Patterns

**Do NOT distribute this file to students.**

## Quick Reference

**Baseline:** Jan 1, 2023 – Jan 15, 2026 (training data, clean)
**Week 3:** Jan 16 – Feb 1, 2026 (data quality issues, students fix)
**Week 4:** Feb 2 – Feb 28, 2026 (4 weeks, drift patterns after fixes)

---

## Key Points for TAs

**Student mindset:** They see degrading performance. Must design monitoring & retraining.

**Your role:** Guide toward thinking, don't name drifts or specify thresholds.

- Don't: "Peak demand shifted from 9am to 6am"
- Do: "Compare hour-of-day distributions. Are peaks in same place?"

**Common questions:**

- "I can't find drift!" → "Start global metrics, then slice by borough/hour/day. Which segments degrade?"
- "What metrics?" → "What would signal a problem? Document 8+ metrics with thresholds you choose."
- "When to retrain?" → "You decide. What are your trigger conditions? How do you prevent bad models?"

**Drift difficulty tiers:**

- 1-2 drifts found → Solid foundation
- 1-3 drifts found → Good per-segment analysis
- All 4 drifts → Exceptional (drift 4 is very subtle)

OK if students miss drift 4. More important: does their monitoring *framework* catch issues?

---

## The 4 Drifts

| # | Name                            | Type          | Period        | Detection Method              | Difficulty |
| - | ------------------------------- | ------------- | ------------- | ----------------------------- | ---------- |
| 1 | Temporal peak shift             | Data drift    | Feb 2 2026+   | Hour distribution comparison  | Easy       |
| 2 | Manhattan lag deflation         | Data drift    | Feb 2 2026+   | PSI per borough on lag_1day   | Medium     |
| 3 | Outer borough baseline scramble | Concept drift | All data      | Per-zone correlation analysis | Hard       |
| 4 | Manhattan weekend reversal      | Concept drift | Feb 2 2026+   | MAPE by (borough, is_weekend) | Very hard  |

---

## Drift Details

### #1: Temporal Peak Shift

- **What:** Feb 2 2026+ all zones: 5-7am +45%, 9-11am -35% trip counts
- **Simulates:** Shift in commute patterns (earlier starts)
- **Student finds:** `groupby('hour')['trip_count'].mean()` shows peak moved from 9am to 6am
- **Metric:** PSI on hourly distribution > 0.25

### #2: Manhattan Lag Deflation

- **What:** Feb 2 2026+ Manhattan only: lag_1day, lag_1week, roll_mean_1day ×0.55
- **Simulates:** Recent demand signal weakening (broken sensor? competitor?)
- **Student finds:** Per-borough PSI analysis; lag_1day mean drops 45% for Manhattan only
- **Metric:** PSI > 0.3 for Manhattan, ~0 for Queens/Brooklyn

### #3: Outer Borough Baseline Scramble

- **What:** All data, all time: zone_slot_baseline ×0.22 or ×3.8 per zone (Queens/Brooklyn only)
- **Simulates:** Feature computed from wrong data source
- **Student finds:** Per-zone correlation analysis; `corr(zone_slot_baseline, trip_count)` drops from 0.82 → 0.05
- **Why subtle:** Values are valid floats, no schema violations. Requires feature-target correlation check.
- **Metric:** Feature-target correlation drops >30% for specific zones

### #4: Manhattan Weekend Reversal

- **What:** Feb 2 2026+ Manhattan weekends only: trip_count ×0.72 (28% reduction)
- **Simulates:** Weekend demand pattern changed (tourism down? hybrid work?)
- **Student finds:** MAPE by (borough, is_weekend); Manhattan weekend MAPE >> baseline
- **Why very subtle:** Global MAPE fine, per-borough MAPE fine, per-day-of-week MAPE fine. Must segment by BOTH dimensions.
- **Metric:** MAPE by segment (borough × day) reveals Manhattan weekend degradation

---

## Grading

| Criterion                                                      | Points         |
| -------------------------------------------------------------- | -------------- |
| Drift detection: 4 patterns with evidence                      | 30%            |
| Monitoring design: ≥8 metrics, thresholds, implementation     | 25%            |
| Retraining strategy: triggers, pipeline, frequency, safeguards | 20%            |
| Code: scripts work, monitoring functional                      | 15%            |
| Documentation: clear reasoning                                 | 10%            |
| **Total**                                                | **100%** |

---

## Expected Student Work

**Weak (60%):** Finds 1-2 obvious drifts. Monitoring is global only (no segmentation).

**Good (75%):** Finds drifts 1-3. Per-segment monitoring (borough, hour, zone). Retraining triggers are reasonable.

**Strong (85%):** Finds drifts 1-3. Deep segmentation analysis. Clear retraining pipeline with safeguards.

**Excellent (95%+):** Finds all 4 drifts. Multi-dimensional segmentation. Comprehensive monitoring. Well-reasoned strategy.

---

## TA Workflow

1. **Before class:**

   ```bash
   python week4/ta_scripts/simulate_week4.py \
     --input data/processed/demand_enriched.parquet \
     --output data/processed/demand_enriched_week4.parquet
   ```
2. **Distribute:** Upload parquet + `monitoring_baseline.json` to GCS. Tell students: "Model degradation detected. Investigate causes and design monitoring."
3. **Grading checklist:**

   - [ ] 4 drifts identified with specific zones/periods/percentages
   - [ ] Quantitative evidence (PSI, correlation, MAPE by segment)
   - [ ] Monitoring metrics documented (≥8, with thresholds)
   - [ ] Retraining triggers specified (conditions & frequency)
   - [ ] Safeguards for preventing bad model deployment
   - [ ] Clear reasoning for all design decisions
4. **Sample feedback:**

   ```
   Drifts found: 3/4
   #1 Temporal peak shift: hour distribution shows 6am peak vs 9am
   #2 Manhattan lag deflation: lag_1day PSI=0.42 for Manhattan, 0.01 elsewhere
   #3 Baseline scramble: corr(zone_slot_baseline, trip_count) = 0.05 (Queens/Brooklyn)
   #4 Weekend reversal: Not found

   Missing: Try MAPE by (borough, is_weekend). You'll see Manhattan weekends degrade.

   Monitoring looks solid (8 metrics, clear thresholds).
   Retraining triggers are well-reasoned (monthly + on-demand).
   ```

---

## If Students Ask

**Q: "I can't find any drift!"**
A: `.describe()` globally. Then slice by borough, hour, day-of-week, zone. Compare recent vs training period. Which slices degrade?

**Q: "How do I know if my metrics are good?"**
A: Do they catch the issues? Test your monitoring code on both clean and drifted data. If it flags the drifts, you're on the right track.

**Q: "Should I retrain weekly or on-demand?"**
A: You decide. Document your reasoning: cost vs risk, how long retraining takes, acceptable staleness, etc.

**Q: "What if my new model is worse?"**
A: That's the problem you need to solve. How do you detect it? How do you rollback? Document your safeguards.

---

## Hints for Students (If Stuck)

1. **Can't find drift 1:** Compare `groupby('hour')['trip_count'].mean()` for training vs recent. Which hours changed?
2. **Can't find drift 2:** Compare lag distributions per borough. `groupby('borough_id')['lag_1day'].describe()` for training vs recent.
3. **Can't find drift 3:** Compute correlation per zone: `corr(zone_slot_baseline, trip_count)` for each zone. Plot them. Queens/Brooklyn show anomalies.
4. **Can't find drift 4:** Try segmenting by borough AND day-of-week. MAPE by (borough, is_weekend). Manhattan weekend should stand out.

---

## Optional Extensions

- Implement automated drift detection: flag when PSI > 0.25 on any feature
- Cost analysis: quantify impact of each drift on recommendation quality
- Feature importance shap values: which features cause errors in drifted segments
- Simulate retraining: actually retrain model on post-drift data, show improvement
