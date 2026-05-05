# Week 6 — Access Control, Data Governance & Response Drift Detection

## Assignment

The Week 5 agent is deployed and running. New policy documents were added to the corpus. Some queries that worked last week now return inconsistent or incorrect answers. Users are complaining.

Your job: Implement access control to prevent unauthorized access. Build monitoring to detect degradation. Figure out what's causing the answer failures.

Available: Week 5 agent code, Week 2 GKE, `data/raw/techcorp/documents_week6_corrupted.json` (updated corpus), `access_control.json`.

## Deliverables

Submit on Canvas by end of Week 6:

### 1. Access Control Implementation

Implement field-level and document-level access control:
- Load access_control.json at startup (role → field visibility)
- Before returning any data: validate against role permissions
- Redact forbidden fields or filter documents entirely
- Log all access attempts (who, role, field, timestamp, denied/allowed)

Implement filtering at two levels:
- **Document retrieval:** Filter at query time (engineer queries don't retrieve finance docs)
- **Field level:** Redact sensitive fields even from allowed documents (salary/ssn)

Files: `app/access_control.py`, `app/audit.py`, updated `app/agent.py`

### 2. Monitoring & Metrics

Build 8 metrics to detect when things go wrong:

1. Answer completeness (% of questions agent fully answers)
2. Access denials (count by field)
3. Response latency (p50, p95, p99 by role)
4. Document retrieval coverage (% of relevant docs retrieved post-filtering)
5. Access denial rate (% of requests hitting denials)
6. Audit log volume (MB/day)
7. Answer consistency (do same queries in same role get same answer?)
8. PII exposure (regex check for SSN/salary patterns in responses)

For each metric: define baseline, alert threshold, and what it means when it spikes.

Create `/metrics` endpoint that returns all 8 metrics + thresholds in JSON.
Create monitoring dashboard (screenshot or Grafana).

Files: `app/monitoring.py`, `k8s/service-monitor.yaml`

### 3. Detect What's Wrong

Run your agent on 10 different queries. Test with different roles (engineer, HR, finance).

Compare:
- **Without filtering:** Agent sees all documents, all fields. Does it answer correctly? Cost? Latency?
- **With filtering:** Agent only sees role-appropriate documents/fields. Does it still answer? Quality change? Cost change?

Document findings: Which queries fail? Which roles affected most? What patterns emerge?

File: `scripts/analyze_query_failures.py` (test agent, log results, identify patterns)

### 4. Tests & Deployment

Write tests:
- Access control blocks unauthorized field access
- Access control filters unauthorized documents
- Audit logging captures all attempts
- Metrics computation is correct
- Same role, same query → same answer (consistency)

Update GitHub Actions to deploy with monitoring live.

Files: `tests/test_access_control.py`, `.github/workflows/deploy.yml`

## Code Structure

```
app/
  access_control.py   (Filter by role)
  audit.py            (Log access)
  monitoring.py       (Compute 8 metrics)
  agent.py            (Updated)

scripts/
  analyze_query_failures.py

tests/
  test_access_control.py
  test_monitoring.py
```

## Grading

| Criterion | Weight |
|-----------|--------|
| Access control working (fields + documents filtered) | 30% |
| Monitoring (8 metrics, accurate, actionable thresholds) | 30% |
| Root cause analysis (what's causing failures?) | 25% |
| Tests & deployment | 15% |

## Submission

- Live agent endpoint with access control enforced
- GitHub repo with code, tests, CI/CD
- Monitoring dashboard screenshot
- Analysis report: which queries fail and why
- Test results (pytest output)

Due: end of Week 6
