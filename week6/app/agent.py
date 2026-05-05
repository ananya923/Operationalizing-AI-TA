"""
agent.py — TechCorp RAG Agent with access control, audit logging, and monitoring.

Extends Week 5 agent with:
- Document-level and field-level access control (access_control.py)
- Audit logging of every access attempt (audit.py)
- /metrics endpoint (monitoring.py)
"""

import json
import time
import logging
import os
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import database as db
import embedding as emb
import guardrails as g
import access_control as ac
import audit
import monitoring as mon

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ollama config
# ---------------------------------------------------------------------------

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="TechCorp Agent API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class QueryRequest(BaseModel):
    query: str
    user_id: str = "anonymous"
    user_role: str = "engineer"


class ToolCall(BaseModel):
    tool: str
    result: object


class QueryResponse(BaseModel):
    answer: str
    tools_called: list[ToolCall]
    intent: str
    tokens_input: int
    tokens_output: int
    cost_usd: float
    latency_ms: float
    model: str
    docs_retrieved: int
    docs_denied: int


# ---------------------------------------------------------------------------
# Intent classification (unchanged from week5)
# ---------------------------------------------------------------------------

_INTENT_KEYWORDS = {
    "employee": [
        "who is",
        "who works",
        "headcount",
        "staff",
        "hire",
        "hired",
        "department",
        "team",
        "title",
        "manager",
        "director",
        "salary",
        "compensation",
        "people",
        "person",
        "contact",
    ],
    "expense": [
        "expenses",
        "spent",
        "spending",
        "costs",
        "receipt",
        "travel cost",
        "budget used",
        "charged",
        "vendor",
        "how much was spent",
        "total spent",
        "expense report",
    ],
    "project": [
        "project",
        "initiative",
        "roadmap",
        "milestone",
        "delivery",
        "deadline",
        "team lead",
        "project spend",
    ],
    "benefits": [
        "benefit",
        "benefits",
        "pto",
        "vacation",
        "health plan",
        "dental",
        "vision",
        "retirement",
        "401k",
        "insurance",
        "fsa",
        "hsa",
        "time off",
        "average pto",
        "pto days",
        "health insurance",
    ],
    "policy": [
        "policy",
        "policies",
        "rule",
        "rules",
        "guideline",
        "handbook",
        "procedure",
        "compliance",
        "gdpr",
        "allowed",
        "permitted",
        "how do i",
        "what is the process",
        "can i",
        "am i allowed",
        "remote work",
        "work from home",
        "reimbursement limit",
        "expense limit",
        "expense policy",
        "travel policy",
    ],
}


def classify_intent(query: str) -> str:
    q = query.lower()
    scores = {intent: 0 for intent in _INTENT_KEYWORDS}
    for intent, keywords in _INTENT_KEYWORDS.items():
        for kw in keywords:
            if kw in q:
                scores[intent] += 1
    best = max(scores, key=lambda k: scores[k])
    return best if scores[best] > 0 else "policy"


# ---------------------------------------------------------------------------
# Tool execution — now with access control applied to all retrieved docs
# ---------------------------------------------------------------------------


def run_tools(
    intent: str,
    query: str,
    user_role: str,
    user_id: str,
) -> tuple[list[ToolCall], str, int, int]:
    """
    Select and call tools based on intent, then apply access control to results.

    Returns:
        tools_called:   list of ToolCall objects
        context:        assembled text context for the LLM
        docs_retrieved: count of docs allowed through
        docs_denied:    count of docs blocked
    """
    tools_called = []
    context_parts = []
    total_denied = 0
    total_retrieved = 0

    def _call(tool_name: str, result_data):
        g.check_tool_access(tool_name, user_role)
        tools_called.append(ToolCall(tool=tool_name, result=result_data))
        context_parts.append(
            f"[Tool: {tool_name}]\n{json.dumps(result_data, indent=2)}"
        )

    def _call_with_ac(tool_name: str, raw_docs: list[dict]):
        """Call a tool that returns documents — apply access control before recording."""
        nonlocal total_denied, total_retrieved

        g.check_tool_access(tool_name, user_role)

        # Apply document + field-level access control
        allowed, denied = ac.apply_access_control(raw_docs, user_role)

        # Audit every document access decision
        for doc in allowed:
            audit.log_document_access(
                user_id=user_id,
                role=user_role,
                doc_id=doc.get("id", "unknown"),
                doc_category=doc.get("category", ""),
                decision="allowed",
            )
        for doc in denied:
            audit.log_document_access(
                user_id=user_id,
                role=user_role,
                doc_id=doc.get("id", "unknown"),
                doc_category=doc.get("category", ""),
                decision="denied",
                reason=f"role '{user_role}' cannot access category '{doc.get('category','')}'",
            )

        total_retrieved += len(allowed)
        total_denied += len(denied)

        # Only pass allowed docs to LLM context
        tools_called.append(ToolCall(tool=tool_name, result=allowed))
        if allowed:
            context_parts.append(
                f"[Tool: {tool_name}]\n{json.dumps(allowed, indent=2)}"
            )

    q = query.lower()

    if intent == "employee":
        words = query.split()
        candidate_name = None
        for w in words:
            if w[0].isupper() and w.lower() not in {
                "who",
                "what",
                "where",
                "when",
                "how",
                "is",
                "are",
                "does",
                "the",
                "a",
                "an",
                "techcorp",
                "company",
            }:
                candidate_name = w
                break
        if candidate_name:
            result = db.get_employee_by_name(candidate_name, user_role)
            if result:
                _call("get_employee", result)
            else:
                result = db.search_employees(user_role=user_role, limit=5)
                _call("search_employees", result)
        else:
            result = db.get_headcount_by_department(user_role)
            _call("get_headcount", result)

    elif intent == "expense":
        if "category" in q or "breakdown" in q or "summary" in q or "type" in q:
            result = db.get_expense_summary_by_category()
            _call("get_expense_summary", result)
        else:
            result = db.get_total_expenses()
            _call("get_total_expenses", result)
            summary = db.get_expense_summary_by_category()
            _call("get_expense_summary", summary)

    elif intent == "project":
        if any(w in q for w in ["active", "ongoing", "current", "in progress"]):
            result = db.get_projects_by_status("active")
            _call("get_project", result)
        elif any(w in q for w in ["budget", "spend", "cost", "overspend"]):
            result = db.get_project_budget_summary()
            _call("get_project_budget_summary", result)
        else:
            result = db.get_project_budget_summary()
            _call("get_project_budget_summary", result)

    elif intent == "benefits":
        if any(w in q for w in ["pto", "vacation", "time off", "leave"]):
            result = db.get_pto_summary()
            _call("get_pto_summary", result)
        elif any(w in q for w in ["health", "dental", "vision", "medical", "plan"]):
            result = db.get_health_plan_distribution()
            _call("get_health_plan_distribution", result)
        else:
            pto = db.get_pto_summary()
            _call("get_pto_summary", pto)
            health = db.get_health_plan_distribution()
            _call("get_health_plan_distribution", health)

    else:  # policy
        raw_docs = emb.search(query, top_k=3)
        _call_with_ac("search_policies", raw_docs)

    # Supplement with policy search for non-policy intents
    if intent != "policy":
        try:
            g.check_tool_access("search_policies", user_role)
            raw_docs = emb.search(query, top_k=2)
            if raw_docs and raw_docs[0]["score"] > 0.15:
                _call_with_ac("search_policies", raw_docs)
        except g.AuthorizationError:
            pass

    context = "\n\n".join(context_parts) if context_parts else "No relevant data found."
    return tools_called, context, total_retrieved, total_denied


# ---------------------------------------------------------------------------
# Ollama call (unchanged from week5)
# ---------------------------------------------------------------------------


async def call_ollama(query: str, context: str, user_role: str) -> tuple[str, int, int]:
    system_prompt = (
        "You are TechCorp's internal AI assistant. "
        "Answer the employee's question using ONLY the data provided in the context below. "
        "Be concise and factual. If the data doesn't contain enough information to answer, "
        "say so clearly. Do not make up numbers or facts.\n"
        f"The user has role: {user_role}. Respect any [REDACTED] fields in the data."
    )
    user_message = f"""Context from TechCorp knowledge base:
{context}

Employee question: {query}

Answer:"""

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": f"{system_prompt}\n\n{user_message}",
        "stream": False,
        "options": {"temperature": 0.1, "num_predict": 300},
    }

    input_tokens = g.estimate_tokens(payload["prompt"])
    output_tokens = 0
    answer = ""

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{OLLAMA_BASE_URL}/api/generate", json=payload
            )
            response.raise_for_status()
            data = response.json()
            answer = data.get("response", "").strip()
            output_tokens = g.estimate_tokens(answer)
    except httpx.ConnectError:
        logger.warning("Ollama not available — returning context-only fallback answer.")
        answer = (
            "I was unable to connect to the language model. "
            "Here is the raw data retrieved for your query:\n\n" + context[:1000]
        )
        output_tokens = g.estimate_tokens(answer)
    except Exception as e:
        logger.error(f"Ollama error: {e}")
        answer = f"An error occurred while generating the answer: {str(e)}"
        output_tokens = g.estimate_tokens(answer)

    return answer, input_tokens, output_tokens


# ---------------------------------------------------------------------------
# Main /query endpoint
# ---------------------------------------------------------------------------


@app.post("/query", response_model=QueryResponse)
async def query_agent(request: QueryRequest):
    start_time = time.time()

    # 1. Guardrails
    try:
        g.check_rate_limit(request.user_id)
        g.validate_role(request.user_role)
    except g.RateLimitError as e:
        raise HTTPException(status_code=429, detail=str(e))
    except g.AuthorizationError as e:
        raise HTTPException(status_code=403, detail=str(e))

    # 2. Classify intent
    intent = classify_intent(request.query)
    logger.info(
        f"Query intent: {intent} | role: {request.user_role} | user: {request.user_id}"
    )

    # 3. Run tools with access control
    try:
        tools_called, context, docs_retrieved, docs_denied = run_tools(
            intent, request.query, request.user_role, request.user_id
        )
    except g.AuthorizationError as e:
        raise HTTPException(status_code=403, detail=str(e))

    # 4. Cost check
    full_input = request.query + "\n" + context
    try:
        g.check_cost_limit(full_input)
    except g.CostLimitError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 5. Call Ollama
    answer, input_tokens, output_tokens = await call_ollama(
        request.query, context, request.user_role
    )

    # 6. Check response for PII leakage
    pii_check = mon.check_pii(answer)
    if pii_check:
        for pattern in pii_check:
            audit.log_pii_detected(
                user_id=request.user_id,
                role=request.user_role,
                query=request.query,
                pattern_matched=pattern,
            )
        logger.warning(
            f"PII detected in response for user={request.user_id} role={request.user_role}"
        )

    # 7. Compute cost and latency
    actual_cost = ((input_tokens + output_tokens) / 1000) * g.COST_PER_1K_TOKENS
    latency_ms = (time.time() - start_time) * 1000

    # 8. Audit log the full query
    audit.log_query(
        user_id=request.user_id,
        role=request.user_role,
        query=request.query,
        intent=intent,
        docs_retrieved=docs_retrieved,
        docs_denied=docs_denied,
        latency_ms=latency_ms,
        cost_usd=round(actual_cost, 6),
    )

    # 9. Record metrics
    mon.record_query(
        role=request.user_role,
        latency_ms=latency_ms,
        docs_retrieved=docs_retrieved,
        docs_denied=docs_denied,
        answer=answer,
        query=request.query,
        had_pii=bool(pii_check),
    )

    logger.info(
        f"Query complete | intent={intent} | tokens={input_tokens}+{output_tokens} "
        f"| cost=${actual_cost:.6f} | latency={latency_ms:.0f}ms "
        f"| docs_retrieved={docs_retrieved} | docs_denied={docs_denied}"
    )

    return QueryResponse(
        answer=answer,
        tools_called=tools_called,
        intent=intent,
        tokens_input=input_tokens,
        tokens_output=output_tokens,
        cost_usd=round(actual_cost, 6),
        latency_ms=round(latency_ms, 1),
        model=OLLAMA_MODEL,
        docs_retrieved=docs_retrieved,
        docs_denied=docs_denied,
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
def health():
    return {"status": "ok", "model": OLLAMA_MODEL}


@app.get("/metrics")
def metrics():
    """Return all 8 monitoring metrics with baselines and alert thresholds."""
    return mon.get_metrics()


@app.get("/tools")
def list_tools(user_role: str = "engineer"):
    try:
        g.validate_role(user_role)
    except g.AuthorizationError as e:
        raise HTTPException(status_code=403, detail=str(e))
    return {"user_role": user_role, "allowed_tools": g.get_allowed_tools(user_role)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
