"""
evaluate_agent.py — Evaluation harness for the TechCorp agent.

Runs 10 representative questions through the agent, measures:
- Correctness (manual expected-answer check)
- Latency (p50, p95, p99)
- Cost per query and daily/yearly projections
- Token usage per query type

Usage:
    # With agent running:
    python scripts/evaluate_agent.py --base-url http://localhost:8001

    # Offline (no Ollama needed — uses tool outputs directly):
    python scripts/evaluate_agent.py --offline
"""

import sys
import os
import time
import json
import argparse
import statistics
from typing import Optional

import httpx

# Allow importing from week5/app directly when running offline
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

# ---------------------------------------------------------------------------
# Test questions — 10 representative queries covering all intents and roles
# ---------------------------------------------------------------------------

TEST_CASES = [
    {
        "id": 1,
        "query": "What is the company travel expense reimbursement limit?",
        "user_role": "engineer",
        "user_id": "eval_user_1",
        "intent": "policy",
        "expected_keywords": [
            "travel",
            "policy",
            "reimburse",
            "expense",
            "limit",
            "approval",
        ],
        "notes": "Policy retrieval — should find Travel and Expense Policy doc",
    },
    {
        "id": 2,
        "query": "How many employees are in each department?",
        "user_role": "manager",
        "user_id": "eval_user_2",
        "intent": "employee",
        "expected_keywords": [
            "department",
            "employees",
            "headcount",
            "human resources",
            "engineering",
        ],
        "notes": "Headcount aggregation — should return dept breakdown",
    },
    {
        "id": 3,
        "query": "What is the total amount spent on expenses across the company?",
        "user_role": "finance",
        "user_id": "eval_user_3",
        "intent": "expense",
        "expected_keywords": ["total", "expense", "amount", "spent"],
        "notes": "Aggregate expense query — finance role required",
    },
    {
        "id": 4,
        "query": "Show me the budget status of all projects.",
        "user_role": "manager",
        "user_id": "eval_user_4",
        "intent": "project",
        "expected_keywords": ["budget", "project", "spent", "remaining"],
        "notes": "Project budget summary — manager role",
    },
    {
        "id": 5,
        "query": "What health insurance plans does TechCorp offer?",
        "user_role": "hr",
        "user_id": "eval_user_5",
        "intent": "benefits",
        "expected_keywords": ["health", "plan", "insurance", "dental", "vision"],
        "notes": "Benefits query — HR role can see health plan distribution",
    },
    {
        "id": 6,
        "query": "What is the average PTO days employees get per year?",
        "user_role": "hr",
        "user_id": "eval_user_6",
        "intent": "benefits",
        "expected_keywords": ["pto", "days", "average", "vacation", "time off"],
        "notes": "PTO statistics — HR role",
    },
    {
        "id": 7,
        "query": "What are the expense categories with the highest total spending?",
        "user_role": "finance",
        "user_id": "eval_user_7",
        "intent": "expense",
        "expected_keywords": ["category", "conference", "software", "travel", "total"],
        "notes": "Expense breakdown by category — finance role",
    },
    {
        "id": 8,
        "query": "What is TechCorp's remote work policy?",
        "user_role": "engineer",
        "user_id": "eval_user_8",
        "intent": "policy",
        "expected_keywords": ["remote", "work", "policy", "home", "office"],
        "notes": "Policy retrieval — remote work doc",
    },
    {
        "id": 9,
        "query": "Which projects are currently active?",
        "user_role": "engineer",
        "user_id": "eval_user_9",
        "intent": "project",
        "expected_keywords": ["active", "project", "status"],
        "notes": "Project status filter — engineer role",
    },
    {
        "id": 10,
        "query": "What is the GDPR compliance policy at TechCorp?",
        "user_role": "engineer",
        "user_id": "eval_user_10",
        "intent": "policy",
        "expected_keywords": ["gdpr", "data", "compliance", "privacy", "personal"],
        "notes": "Compliance policy retrieval",
    },
]

# ---------------------------------------------------------------------------
# Correctness check
# ---------------------------------------------------------------------------


def check_correctness(answer: str, expected_keywords: list[str]) -> tuple[bool, float]:
    """
    Heuristic correctness: what fraction of expected keywords appear in the answer?
    Returns (passed, score) where passed = score >= 0.5
    """
    answer_lower = answer.lower()
    hits = sum(1 for kw in expected_keywords if kw.lower() in answer_lower)
    score = hits / len(expected_keywords) if expected_keywords else 0.0
    return score >= 0.5, round(score, 2)


# ---------------------------------------------------------------------------
# Online evaluation (agent must be running)
# ---------------------------------------------------------------------------


async def run_online(base_url: str) -> list[dict]:
    results = []
    async with httpx.AsyncClient(timeout=120.0) as client:
        for tc in TEST_CASES:
            print(f"  [{tc['id']:2d}/10] {tc['query'][:60]}...", end=" ", flush=True)
            start = time.time()
            try:
                resp = await client.post(
                    f"{base_url}/query",
                    json={
                        "query": tc["query"],
                        "user_id": tc["user_id"],
                        "user_role": tc["user_role"],
                    },
                )
                latency_ms = (time.time() - start) * 1000
                if resp.status_code == 200:
                    data = resp.json()
                    passed, score = check_correctness(
                        data["answer"], tc["expected_keywords"]
                    )
                    results.append(
                        {
                            **tc,
                            "status": "ok",
                            "answer": data["answer"][:200],
                            "tools_called": [t["tool"] for t in data["tools_called"]],
                            "tokens_input": data["tokens_input"],
                            "tokens_output": data["tokens_output"],
                            "cost_usd": data["cost_usd"],
                            "latency_ms": latency_ms,
                            "correct": passed,
                            "correct_score": score,
                        }
                    )
                    print(
                        f"✓ score={score:.2f} latency={latency_ms:.0f}ms cost=${data['cost_usd']:.5f}"
                    )
                else:
                    results.append(
                        {
                            **tc,
                            "status": f"http_{resp.status_code}",
                            "error": resp.text[:200],
                            "latency_ms": latency_ms,
                            "correct": False,
                            "correct_score": 0.0,
                        }
                    )
                    print(f"✗ HTTP {resp.status_code}")
            except Exception as e:
                latency_ms = (time.time() - start) * 1000
                results.append(
                    {
                        **tc,
                        "status": "error",
                        "error": str(e),
                        "latency_ms": latency_ms,
                        "correct": False,
                        "correct_score": 0.0,
                    }
                )
                print(f"✗ ERROR: {e}")
    return results


# ---------------------------------------------------------------------------
# Offline evaluation (no agent needed — runs tools directly)
# ---------------------------------------------------------------------------


def run_offline() -> list[dict]:
    from agent import classify_intent, run_tools
    import guardrails as g

    results = []
    for tc in TEST_CASES:
        print(f"  [{tc['id']:2d}/10] {tc['query'][:60]}...", end=" ", flush=True)
        start = time.time()
        try:
            intent = classify_intent(tc["query"])
            tools_called, context = run_tools(intent, tc["query"], tc["user_role"])
            latency_ms = (time.time() - start) * 1000

            # Use context as the "answer" for correctness check in offline mode
            passed, score = check_correctness(context, tc["expected_keywords"])
            input_tokens = g.estimate_tokens(tc["query"] + context)
            output_tokens = 200  # assumed
            cost = ((input_tokens + output_tokens) / 1000) * g.COST_PER_1K_TOKENS

            results.append(
                {
                    **tc,
                    "status": "ok",
                    "answer": context[:200],
                    "tools_called": [t.tool for t in tools_called],
                    "tokens_input": input_tokens,
                    "tokens_output": output_tokens,
                    "cost_usd": round(cost, 6),
                    "latency_ms": round(latency_ms, 1),
                    "correct": passed,
                    "correct_score": score,
                    "detected_intent": intent,
                }
            )
            print(f"✓ intent={intent} score={score:.2f} latency={latency_ms:.0f}ms")
        except g.AuthorizationError as e:
            latency_ms = (time.time() - start) * 1000
            results.append(
                {
                    **tc,
                    "status": "auth_error",
                    "error": str(e),
                    "latency_ms": latency_ms,
                    "correct": False,
                    "correct_score": 0.0,
                }
            )
            print(f"✗ AUTH: {e}")
        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            results.append(
                {
                    **tc,
                    "status": "error",
                    "error": str(e),
                    "latency_ms": latency_ms,
                    "correct": False,
                    "correct_score": 0.0,
                }
            )
            print(f"✗ ERROR: {e}")
    return results


# ---------------------------------------------------------------------------
# Metrics table
# ---------------------------------------------------------------------------


def print_metrics(results: list[dict]):
    ok = [r for r in results if r["status"] == "ok"]

    print("\n" + "=" * 80)
    print("EVALUATION RESULTS")
    print("=" * 80)

    # Per-query table
    print(
        f"\n{'#':>2}  {'Query':<45} {'Role':<10} {'Correct':>7} {'Tokens':>7} {'Cost':>8} {'Latency':>8}"
    )
    print("-" * 90)
    for r in results:
        q_short = r["query"][:43] + ".." if len(r["query"]) > 45 else r["query"]
        correct = f"{r.get('correct_score', 0):.2f}" if r["status"] == "ok" else "ERR"
        tokens = (
            str(r.get("tokens_input", 0) + r.get("tokens_output", 0))
            if r["status"] == "ok"
            else "-"
        )
        cost = f"${r.get('cost_usd', 0):.5f}" if r["status"] == "ok" else "-"
        latency = f"{r.get('latency_ms', 0):.0f}ms" if r["status"] == "ok" else "-"
        print(
            f"{r['id']:>2}  {q_short:<45} {r['user_role']:<10} {correct:>7} {tokens:>7} {cost:>8} {latency:>8}"
        )

    if not ok:
        print("\nNo successful results to summarize.")
        return

    # Latency percentiles
    latencies = sorted(r["latency_ms"] for r in ok)
    p50 = statistics.median(latencies)
    p95 = (
        latencies[int(len(latencies) * 0.95)] if len(latencies) >= 20 else latencies[-1]
    )
    p99 = (
        latencies[int(len(latencies) * 0.99)]
        if len(latencies) >= 100
        else latencies[-1]
    )

    # Cost stats
    costs = [r["cost_usd"] for r in ok]
    total_cost = sum(costs)
    avg_cost = statistics.mean(costs)

    # Token stats
    total_tokens = sum(r.get("tokens_input", 0) + r.get("tokens_output", 0) for r in ok)

    # Correctness
    correct_count = sum(1 for r in ok if r.get("correct", False))

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"  Queries run:        {len(results)}/10")
    print(f"  Successful:         {len(ok)}/10")
    print(
        f"  Correctness:        {correct_count}/{len(ok)} ({100*correct_count/max(len(ok),1):.0f}%)"
    )
    print()
    print(f"  Latency  p50:       {p50:.0f}ms")
    print(f"  Latency  p95:       {p95:.0f}ms")
    print(f"  Latency  p99:       {p99:.0f}ms")
    print()
    print(f"  Avg cost/query:     ${avg_cost:.5f}")
    print(f"  Total (10 queries): ${total_cost:.5f}")
    print(f"  Daily projection    (1,000 queries/day):  ${avg_cost * 1000:.2f}")
    print(f"  Yearly projection   (365,000 queries/yr): ${avg_cost * 365000:.2f}")
    print()
    print(f"  Total tokens used:  {total_tokens:,}")
    print(f"  Avg tokens/query:   {total_tokens // max(len(ok), 1):,}")

    # Intent accuracy (offline mode only)
    if any("detected_intent" in r for r in results):
        print()
        print("  Intent classification accuracy:")
        for r in results:
            if "detected_intent" in r:
                match = "✓" if r["detected_intent"] == r["intent"] else "✗"
                print(
                    f"    {match} Q{r['id']:02d}: expected={r['intent']:<10} detected={r['detected_intent']}"
                )

    print("=" * 80)

    # Save results to JSON
    out_path = os.path.join(os.path.dirname(__file__), "..", "evaluation_results.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nFull results saved to: evaluation_results.json")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate TechCorp agent")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8001",
        help="Agent base URL (default: http://localhost:8001)",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Run offline without a live agent (uses tools directly)",
    )
    args = parser.parse_args()

    print("TechCorp Agent Evaluation")
    print(f"Mode: {'offline' if args.offline else f'online ({args.base_url})'}")
    print("-" * 80)

    if args.offline:
        results = run_offline()
    else:
        import asyncio

        results = asyncio.run(run_online(args.base_url))

    print_metrics(results)
