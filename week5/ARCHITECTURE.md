# TechCorp Agent — Architecture

## System Overview

The TechCorp Agent is a Retrieval-Augmented Generation (RAG) system that answers
employee business questions using TechCorp's internal knowledge base. It combines
structured database lookups (SQLite) with unstructured document retrieval (TF-IDF)
and uses a local Ollama LLM to generate natural language answers.

---

## Query Flow

```
Employee
   │
   │  POST /query
   │  { query, user_id, user_role }
   ▼
┌─────────────────────────────────────────────────────────┐
│                    FastAPI Agent                        │
│                    (agent.py)                           │
│                                                         │
│  1. GUARDRAILS                                          │
│     ├── Rate limit check  (10 queries/min per user)     │
│     ├── Role validation   (engineer/manager/hr/...)     │
│     └── Cost pre-check    (estimated tokens × price)    │
│                    │                                    │
│                    │ pass                               │
│                    ▼                                    │
│  2. INTENT CLASSIFICATION                               │
│     keyword matching → one of:                          │
│     employee │ expense │ project │ benefits │ policy    │
│                    │                                    │
│                    ▼                                    │
│  3. TOOL SELECTION & EXECUTION                          │
│     ┌─────────────────────────────────────────┐         │
│     │           Tool Router                   │         │
│     │  checks role permission before each call│         │
│     └──────────────┬──────────────────────────┘         │
│           ┌────────┴─────────┐                          │
│           ▼                  ▼                          │
│    ┌─────────────┐   ┌───────────────┐                  │
│    │ database.py │   │ embedding.py  │                  │
│    │             │   │               │                  │
│    │ employees   │   │ TF-IDF index  │                  │
│    │ expenses    │   │ 74 policy docs│                  │
│    │ projects    │   │               │                  │
│    │ benefits    │   │ search()      │                  │
│    └──────┬──────┘   └──────┬────────┘                  │
│           │                 │                           │
│           └────────┬────────┘                           │
│                    │ context (JSON + doc snippets)       │
│                    ▼                                    │
│  4. LLM REASONING                                       │
│     ┌──────────────────────────────────┐                │
│     │  Ollama (llama3.2:3b)            │                │
│     │  system prompt + context + query │                │
│     │  temperature=0.1 (factual)       │                │
│     └──────────────┬───────────────────┘                │
│                    │ answer text                        │
│                    ▼                                    │
│  5. COST TRACKING                                       │
│     actual tokens (input + output) × $0.0002/1K        │
│     logged to server stdout                             │
└─────────────────────────────────────────────────────────┘
   │
   │  QueryResponse
   │  { answer, tools_called, intent,
   │    tokens_input, tokens_output,
   │    cost_usd, latency_ms, model }
   ▼
Employee
```

---

## Tools

| Tool | Permission Required | Roles Allowed | Data Source |
|---|---|---|---|
| `search_employees` | `view_employee_directory` | all | employees table |
| `get_employee` | `view_employee_directory` | all | employees table |
| `get_headcount` | `view_employee_directory` | all | employees table |
| `get_expense_summary` | `view_all_expenses` | manager, finance, executive | expenses table |
| `get_expenses_by_employee` | `view_all_expenses` | manager, finance, executive | expenses table |
| `get_total_expenses` | `view_financial_reports` | finance, executive | expenses table |
| `get_project` | `view_project_details` | engineer, manager, finance, executive | projects table |
| `get_project_budget_summary` | `view_project_details` | engineer, manager, finance, executive | projects table |
| `get_benefits` | `view_own_benefits` | all | benefits table |
| `get_pto_summary` | `view_hr_data` | hr, executive | benefits table |
| `get_health_plan_distribution` | `view_hr_data` | hr, executive | benefits table |
| `search_policies` | `view_travel_policy` | all | documents.json (74 docs) |

### Sensitive Field Redaction

Fields are redacted at the database layer before being passed to the LLM:

| Field | Visible to |
|---|---|
| `salary` | executive, hr, finance |
| `ssn` | hr, finance |
| `address` | hr, executive |

---

## Guardrails

### 1. Role-Based Access Control
Every tool call checks the user's role against `access_control.json` before executing.
Unauthorized tool calls raise an `AuthorizationError` (HTTP 403).

### 2. Cost Limits
Each query estimates token count from input length before calling the LLM.
Queries exceeding **$0.01 USD** are rejected (HTTP 400) with a `CostLimitError`.
Token estimation: `len(text) / 4` (standard approximation).

### 3. Rate Limiting
A sliding-window counter tracks requests per `user_id` over the last 60 seconds.
Exceeding **10 queries/minute** raises a `RateLimitError` (HTTP 429).
State is in-memory — resets on pod restart.

---

## Cost Model

| Metric | Value |
|---|---|
| Cost per 1K tokens | $0.0002 (open-source LLM proxy estimate) |
| Avg tokens per query | ~1,500 |
| Avg cost per query | ~$0.0003 |
| Daily (1,000 queries) | ~$0.30 |
| Yearly (365K queries) | ~$110 |

Cost per query stays low because context is bounded — we retrieve at most 3 policy
doc snippets (500 chars each) and a single DB query result per request.

---

## Fallback Behavior

| Scenario | Behavior |
|---|---|
| Ollama not reachable | Returns raw context from tools with an explanatory message |
| No DB match for name lookup | Falls back to `search_employees` (department search) |
| No policy docs match query | Returns empty list — agent answers from DB context only |
| Unknown intent (no keywords match) | Defaults to `policy` intent — searches documents |
| Unauthorized tool for role | HTTP 403 with specific permission name |
| Rate limit exceeded | HTTP 429 with retry-after seconds |
| Query too expensive | HTTP 400 with cost estimate |

---

## Deployment Architecture

```
GitHub (main branch)
       │
       │  push to week5/**
       ▼
GitHub Actions (deploy.yml)
  ├── Job 1: pytest week5/tests/  (65 tests)
  │          └── fails → stop, no deploy
  └── Job 2: build & deploy
             ├── docker build -f week5/Dockerfile .
             │   (bakes app code + techcorp data into image)
             ├── docker push → Artifact Registry (GCR)
             │   us-central1-docker.pkg.dev/
             │   assignment02-494503/docker-repo/techcorp-agent
             └── kubectl apply → GKE cluster (operationalizing-ai, us-central1-a)
                    │
                    ▼
             ┌─────────────────────────────┐
             │  GKE Pod (techcorp-agent)   │
             │                             │
             │  ┌─────────────────────┐    │
             │  │  FastAPI :8001      │    │
             │  │  (agent.py)         │    │
             │  └─────────────────────┘    │
             │  ┌─────────────────────┐    │
             │  │  Ollama :11434      │    │
             │  │  (llama3.2:3b)      │    │
             │  └─────────────────────┘    │
             │                             │
             └─────────────────────────────┘
                    │
                    │  LoadBalancer Service
                    │  port 80 → 8001
                    ▼
             Public IP (GKE LoadBalancer)
             http://<EXTERNAL-IP>/query
```

---

## Design Decisions

**Why rule-based intent classification instead of an LLM router?**
The 5 intents map directly to the 4 DB tables + policy documents. Keyword matching
is deterministic, fast (~1ms), free, and easily testable — an LLM router would add
latency and cost for no measurable accuracy gain on a well-defined domain.

**Why TF-IDF instead of vector embeddings?**
TF-IDF requires no GPU, no API key, and no model download. It loads in under 1
second at startup, fits entirely in memory, and performs well on the 74-document
corpus. Vector embeddings would improve recall on paraphrased queries but add
significant deployment complexity for a small corpus.

**Why bake the data into the Docker image?**
The TechCorp dataset is static (no live updates). Baking it in avoids the GCS init
container complexity from Week 2, eliminates a runtime dependency, and keeps cold
start time low. The tradeoff is a larger image (~500MB), which is acceptable.

**Why Ollama as a sidecar (not a separate service)?**
Running Ollama in the same pod keeps the architecture simple — no inter-service
networking, no separate deployment to manage. The tradeoff is higher per-pod memory
usage. In production with high traffic, Ollama would be split into a separate
deployment with its own scaling policy.
