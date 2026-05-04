"""
agent.py — TechCorp RAG Agent with Ollama-backed reasoning loop.

FastAPI endpoint that:
1. Validates the request via guardrails (rate limit, role, cost)
2. Classifies the query intent (employee / expense / project / policy / benefits)
3. Calls the relevant tool(s) to retrieve context from the database or documents
4. Sends query + context to a local Ollama LLM to generate a natural language answer
5. Tracks token usage and cost per query

Ollama must be running locally (or as a sidecar in Docker) on port 11434.
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

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ollama config
# ---------------------------------------------------------------------------

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")  # override via env var

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="TechCorp Agent API", version="1.0.0")

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


# ---------------------------------------------------------------------------
# Intent classification
# ---------------------------------------------------------------------------

# Keyword sets for each intent — checked in order
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
    """Return the most likely intent for a query based on keyword matching."""
    q = query.lower()
    scores = {intent: 0 for intent in _INTENT_KEYWORDS}
    for intent, keywords in _INTENT_KEYWORDS.items():
        for kw in keywords:
            if kw in q:
                scores[intent] += 1
    best = max(scores, key=lambda k: scores[k])
    # Fall back to policy search if nothing matched
    return best if scores[best] > 0 else "policy"


# ---------------------------------------------------------------------------
# Tool execution
# ---------------------------------------------------------------------------


def run_tools(intent: str, query: str, user_role: str) -> tuple[list[ToolCall], str]:
    """
    Select and call the appropriate tool(s) based on intent.

    Returns:
        tools_called: list of ToolCall objects (for the response)
        context:      assembled text context to pass to the LLM
    """
    tools_called = []
    context_parts = []

    def _call(tool_name: str, result_data):
        """Helper to record a tool call and add its result to context."""
        # Check access before recording
        g.check_tool_access(tool_name, user_role)
        tools_called.append(ToolCall(tool=tool_name, result=result_data))
        context_parts.append(
            f"[Tool: {tool_name}]\n{json.dumps(result_data, indent=2)}"
        )

    q = query.lower()

    if intent == "employee":
        # Try to find a name in the query for a specific lookup
        words = query.split()
        # Heuristic: look for capitalized word sequences (likely names)
        candidate_name = None
        for i, w in enumerate(words):
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
                # Fall back to department search
                result = db.search_employees(user_role=user_role, limit=5)
                _call("search_employees", result)
        else:
            # Generic: headcount or department search
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
            # Try name-based lookup first
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

    else:  # policy (default)
        results = emb.search(query, top_k=3)
        _call("search_policies", results)

    # Supplement with policy search for non-policy intents,
    # but only if the tool is accessible to the role and there's a strong match
    if intent != "policy":
        try:
            g.check_tool_access("search_policies", user_role)
            policy_results = emb.search(query, top_k=2)
            if policy_results and policy_results[0]["score"] > 0.15:
                _call("search_policies", policy_results)
        except g.AuthorizationError:
            pass  # role can't access policy search — skip silently

    context = "\n\n".join(context_parts) if context_parts else "No relevant data found."
    return tools_called, context


# ---------------------------------------------------------------------------
# Ollama call
# ---------------------------------------------------------------------------


async def call_ollama(query: str, context: str, user_role: str) -> tuple[str, int, int]:
    """
    Call Ollama with the query and retrieved context.

    Returns:
        answer:        LLM-generated answer string
        input_tokens:  estimated input token count
        output_tokens: estimated output token count
    """
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
        "options": {
            "temperature": 0.1,  # low temperature for factual answers
            "num_predict": 300,  # max output tokens
        },
    }

    input_tokens = g.estimate_tokens(payload["prompt"])
    output_tokens = 0
    answer = ""

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{OLLAMA_BASE_URL}/api/generate",
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            answer = data.get("response", "").strip()
            output_tokens = g.estimate_tokens(answer)
    except httpx.ConnectError:
        # Ollama not running — return a fallback answer using the raw context
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
    """
    Main agent endpoint. Accepts a natural language question and returns
    a grounded answer from the TechCorp knowledge base.
    """
    start_time = time.time()

    # 1. Guardrails: rate limit + role validation (cost checked after context assembly)
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

    # 3. Run tools (may raise AuthorizationError if role can't use a tool)
    try:
        tools_called, context = run_tools(intent, request.query, request.user_role)
    except g.AuthorizationError as e:
        raise HTTPException(status_code=403, detail=str(e))

    # 4. Cost check on assembled context + query
    full_input = request.query + "\n" + context
    try:
        estimated_cost = g.check_cost_limit(full_input)
    except g.CostLimitError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 5. Call Ollama
    answer, input_tokens, output_tokens = await call_ollama(
        request.query, context, request.user_role
    )

    # 6. Final cost (based on actual token counts)
    actual_cost = ((input_tokens + output_tokens) / 1000) * g.COST_PER_1K_TOKENS
    latency_ms = (time.time() - start_time) * 1000

    logger.info(
        f"Query complete | intent={intent} | tokens={input_tokens}+{output_tokens} "
        f"| cost=${actual_cost:.6f} | latency={latency_ms:.0f}ms"
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
    )


@app.get("/health")
def health():
    return {"status": "ok", "model": OLLAMA_MODEL}


@app.get("/tools")
def list_tools(user_role: str = "engineer"):
    """List tools available to a given role."""
    try:
        g.validate_role(user_role)
    except g.AuthorizationError as e:
        raise HTTPException(status_code=403, detail=str(e))
    return {"user_role": user_role, "allowed_tools": g.get_allowed_tools(user_role)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
