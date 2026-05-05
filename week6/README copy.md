# Week 5 — Agent Architecture for Enterprise RAG

## Assignment

Implement and deploy an agent that answers TechCorp business questions using its knowledge base (SQLite database, 74 policy documents, access control matrix, API schemas). Reuse Week 2 GKE infrastructure and CI/CD pipeline.

## Deliverables

Submit on Canvas by end of Week 5:

### 1. Agent Implementation

Build the agent in Python (FastAPI endpoint):
- Load TechCorp SQLite database at startup
- Load and embed policy documents (vector database or in-memory index)
- Implement 3-5 tools (database lookups, policy retrieval, calculations)
- Implement reasoning loop (determine which tools to call, handle errors, fallback to simple answers)
- Cost tracking: log tokens and cost per query

File: `app/agent.py` with Tool class definitions and Agent orchestration logic.

### 2. Deployment & CI/CD

Deploy to your Week 2 GKE cluster:
- Update Dockerfile to include agent code and dependencies
- Update `k8s/deployment.yaml` with agent service
- Update GitHub Actions workflow to build and push agent image
- Verify agent endpoint responds to queries

Files: `Dockerfile`, `k8s/deployment.yaml`, `.github/workflows/deploy.yml`

### 3. Evaluation & Cost Tracking

Test agent on 10 representative questions:
- Correctness: does agent answer correctly? (manual check or automated)
- Cost: track cost per query, total daily/yearly projections
- Latency: p50, p95, p99 response times
- Token usage: input/output tokens per query type

File: `scripts/evaluate_agent.py` (runs queries, logs costs, generates metrics table)

### 4. Guardrails & Safety

Implement constraints:
- Tool access control: certain tools only callable by certain roles (add user_role parameter to agent)
- Cost limits: reject queries exceeding cost threshold
- Rate limiting: max queries per minute per user

File: `app/guardrails.py` with access checks and cost enforcement

### 5. Architecture Diagram & README

Document system flow:
- How queries flow through retrieval → reasoning → tool calls → response
- Cost estimation and budgets
- Tool definitions and constraints
- Fallback behavior

File: `ARCHITECTURE.md` with ASCII diagram

## Code Structure

```
app/
  agent.py          (Agent class, Tool definitions, reasoning loop)
  guardrails.py     (Access control, cost limits, rate limiting)
  database.py       (SQLite queries, caching)
  embedding.py      (Load docs, build vector index)
  
k8s/
  deployment.yaml   (Updated with agent service)
  
.github/workflows/
  deploy.yml        (Build, push, deploy agent)

scripts/
  evaluate_agent.py (Cost tracking, correctness evaluation)

tests/
  test_agent.py     (Unit tests for tools, reasoning)
  test_guardrails.py (Verify access control, cost limits)
```

## Testing

Write pytest tests:
- Tool access control (user_role validation)
- Cost calculation (verify token counts → costs)
- Fallback behavior (agent gracefully handles missing data)
- Correctness on sample queries

Run: `pytest tests/` in GitHub Actions before deploy

## Grading

| Criterion | Weight |
|-----------|--------|
| Agent implementation (tools, reasoning, correctness) | 35% |
| Deployment & CI/CD functional | 25% |
| Cost tracking & guardrails working | 20% |
| Tests pass, code organized | 15% |
| Documentation (architecture, design choices) | 5% |

## Submission

- Live agent endpoint (public IP)
- GitHub repo with agent code, tests, CI/CD
- Cost tracking output (sample query costs)
- Test results (pytest output)
- ARCHITECTURE.md explaining design

Due: end of Week 5
