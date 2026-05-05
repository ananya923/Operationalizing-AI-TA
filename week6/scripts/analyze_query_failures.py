"""
analyze_query_failures.py — Run 10 test queries across roles, with and without
access control, and identify patterns in failures.

Usage:
    python scripts/analyze_query_failures.py --base-url http://localhost:8001

Output:
    results printed to console + saved to scripts/query_failure_report.json
"""

import argparse
import json
import time
import httpx
from datetime import datetime

# ---------------------------------------------------------------------------
# Test queries — 10 questions covering all intent types
# ---------------------------------------------------------------------------

TEST_QUERIES = [
    {
        "query": "What is the hotel reimbursement limit for domestic travel?",
        "intent": "policy",
    },
    {
        "query": "What is the maximum flight budget for international travel?",
        "intent": "policy",
    },
    {
        "query": "How many days of PTO do individual contributors get?",
        "intent": "benefits",
    },
    {"query": "What are the salary ranges for senior engineers?", "intent": "employee"},
    {"query": "What is the total amount spent on expenses?", "intent": "expense"},
    {"query": "Can I work fully remote?", "intent": "policy"},
    {
        "query": "What is the approval process for travel over budget?",
        "intent": "policy",
    },
    {"query": "How many employees are in each department?", "intent": "employee"},
    {"query": "What health plans are available?", "intent": "benefits"},
    {"query": "What are the active projects and their budgets?", "intent": "project"},
]

ROLES = ["engineer", "hr", "finance"]

# ---------------------------------------------------------------------------
# Query runner
# ---------------------------------------------------------------------------


def run_query(base_url: str, query: str, user_id: str, role: str) -> dict:
    """Send a single query to the agent and return the full response + metadata."""
    payload = {"query": query, "user_id": user_id, "user_role": role}
    start = time.time()
    try:
        response = httpx.post(f"{base_url}/query", json=payload, timeout=60.0)
        latency_ms = (time.time() - start) * 1000
        if response.status_code == 200:
            data = response.json()
            return {
                "status": "success",
                "answer": data.get("answer", ""),
                "intent": data.get("intent", ""),
                "tokens_input": data.get("tokens_input", 0),
                "tokens_output": data.get("tokens_output", 0),
                "cost_usd": data.get("cost_usd", 0),
                "latency_ms": data.get("latency_ms", latency_ms),
                "docs_retrieved": data.get("docs_retrieved", 0),
                "docs_denied": data.get("docs_denied", 0),
            }
        else:
            return {
                "status": "error",
                "error_code": response.status_code,
                "error_msg": response.text,
                "latency_ms": (time.time() - start) * 1000,
            }
    except Exception as e:
        return {
            "status": "exception",
            "error_msg": str(e),
            "latency_ms": (time.time() - start) * 1000,
        }


# ---------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------


def is_failure(result: dict) -> bool:
    """Return True if the query failed or returned a fallback/empty answer."""
    if result["status"] != "success":
        return True
    answer = result.get("answer", "").lower()
    fallback_phrases = [
        "unable to connect",
        "no relevant data",
        "an error occurred",
        "i don't know",
        "i cannot answer",
        "not enough information",
    ]
    return any(p in answer for p in fallback_phrases)


def answers_are_consistent(answers: list[str]) -> bool:
    """Return True if all answers for the same (role, query) are identical."""
    normalized = [a.strip().lower() for a in answers if a]
    return len(set(normalized)) <= 1


def detect_conflict(answers_by_role: dict[str, str], query: str) -> str | None:
    """
    Check if different roles get meaningfully different answers to the same query.
    Returns a description of the conflict if found, else None.
    """
    non_empty = {role: ans for role, ans in answers_by_role.items() if ans}
    if len(non_empty) < 2:
        return None

    # Simple heuristic: look for dollar amounts that differ across roles
    import re

    dollar_pattern = re.compile(r"\$[\d,]+")
    amounts_by_role = {
        role: set(dollar_pattern.findall(ans)) for role, ans in non_empty.items()
    }
    all_amounts = set().union(*amounts_by_role.values())
    if len(all_amounts) > 1:
        role_summaries = ", ".join(
            f"{role}: {amounts or 'none'}" for role, amounts in amounts_by_role.items()
        )
        return f"Conflicting dollar amounts across roles — {role_summaries}"

    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(base_url: str):
    print(f"\n{'='*60}")
    print(f"TechCorp Agent — Query Failure Analysis")
    print(f"Base URL: {base_url}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"{'='*60}\n")

    all_results = []
    failure_summary = []
    conflict_summary = []

    for q_idx, test in enumerate(TEST_QUERIES):
        query = test["query"]
        intent = test["intent"]
        print(f"Query {q_idx+1:02d}: {query}")
        print(f"  Expected intent: {intent}")

        results_by_role = {}

        for role in ROLES:
            user_id = f"test_{role}_user"
            result = run_query(base_url, query, user_id, role)
            results_by_role[role] = result

            status_str = "✓" if result["status"] == "success" else "✗"
            denied_str = (
                f" | denied={result.get('docs_denied', '-')}"
                if result["status"] == "success"
                else ""
            )
            print(
                f"  [{role:8s}] {status_str} | "
                f"latency={result.get('latency_ms', 0):.0f}ms | "
                f"cost=${result.get('cost_usd', 0):.6f}"
                f"{denied_str}"
            )

            if is_failure(result):
                failure_summary.append(
                    {
                        "query": query,
                        "role": role,
                        "reason": result.get("error_msg")
                        or result.get("answer", "")[:120],
                        "status": result["status"],
                    }
                )

        # Check for cross-role answer conflicts
        answers_by_role = {
            role: res.get("answer", "")
            for role, res in results_by_role.items()
            if res["status"] == "success"
        }
        conflict = detect_conflict(answers_by_role, query)
        if conflict:
            print(f"  ⚠ CONFLICT: {conflict}")
            conflict_summary.append({"query": query, "conflict": conflict})

        all_results.append(
            {
                "query": query,
                "expected_intent": intent,
                "results_by_role": results_by_role,
                "conflict": conflict,
            }
        )
        print()

    # ---------------------------------------------------------------------------
    # Summary report
    # ---------------------------------------------------------------------------

    print(f"\n{'='*60}")
    print("FAILURE SUMMARY")
    print(f"{'='*60}")
    if failure_summary:
        for f in failure_summary:
            print(f"  [{f['role']:8s}] {f['query'][:55]}")
            print(f"             → {f['reason'][:80]}")
    else:
        print("  No failures detected.")

    print(f"\n{'='*60}")
    print("CONFLICT SUMMARY  (same query, different answers by role)")
    print(f"{'='*60}")
    if conflict_summary:
        for c in conflict_summary:
            print(f"  {c['query'][:55]}")
            print(f"    → {c['conflict']}")
    else:
        print("  No cross-role conflicts detected.")

    print(f"\n{'='*60}")
    print("COST & LATENCY SUMMARY")
    print(f"{'='*60}")
    all_costs = [
        r.get("cost_usd", 0)
        for rr in all_results
        for r in rr["results_by_role"].values()
        if r["status"] == "success"
    ]
    all_latencies = [
        r.get("latency_ms", 0)
        for rr in all_results
        for r in rr["results_by_role"].values()
        if r["status"] == "success"
    ]
    if all_costs:
        print(f"  Total cost:      ${sum(all_costs):.6f}")
        print(f"  Avg cost/query:  ${sum(all_costs)/len(all_costs):.6f}")
        print(f"  Avg latency:     {sum(all_latencies)/len(all_latencies):.0f}ms")
        print(f"  Max latency:     {max(all_latencies):.0f}ms")

    # Save full report
    report = {
        "timestamp": datetime.now().isoformat(),
        "base_url": base_url,
        "total_queries": len(TEST_QUERIES) * len(ROLES),
        "failures": len(failure_summary),
        "conflicts": len(conflict_summary),
        "failure_details": failure_summary,
        "conflict_details": conflict_summary,
        "full_results": all_results,
    }
    report_path = "scripts/query_failure_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nFull report saved to {report_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--base-url",
        default="http://localhost:8001",
        help="Agent base URL (default: http://localhost:8001)",
    )
    args = parser.parse_args()
    main(args.base_url)
