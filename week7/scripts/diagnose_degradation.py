"""
diagnose_degradation.py — Week 7: Diagnose the production degradation.

Runs the same 10 evaluation queries from Week 6 against both the clean corpus
(documents.json) and the bloated corpus (documents_week7_bloated.json), then
compares cost, latency, and accuracy side-by-side to identify root cause.

Also scores every document in the bloated corpus to surface the 7 injected
problem categories.

Because embedding.py builds its TF-IDF index at import time, each corpus run
is executed in a fresh subprocess so the index is rebuilt from scratch.

Usage (offline — no Ollama needed):
    # From the repo root:
    python week7/scripts/diagnose_degradation.py

    # Explicit paths:
    python week7/scripts/diagnose_degradation.py \\
        --clean-corpus   data/raw/techcorp/documents.json \\
        --bloated-corpus data/raw/techcorp/documents_week7_bloated.json \\
        --output         week7/diagnosis_report.json
"""

import sys
import os
import json
import time
import argparse
import statistics
import subprocess
import tempfile
from collections import defaultdict, Counter

# ---------------------------------------------------------------------------
# Default paths (resolved relative to repo root, two levels up from scripts/)
# ---------------------------------------------------------------------------

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

DEFAULT_CLEAN   = os.path.join(REPO_ROOT, "data", "raw", "techcorp", "documents.json")
DEFAULT_BLOATED = os.path.join(REPO_ROOT, "data", "raw", "techcorp", "documents_week7_bloated.json")
DEFAULT_OUTPUT  = os.path.join(os.path.dirname(__file__), "..", "diagnosis_report.json")

WEEK6_APP = os.path.join(REPO_ROOT, "week7", "app")

# ---------------------------------------------------------------------------
# Same 10 test cases as week6/scripts/evaluate_agent.py
# ---------------------------------------------------------------------------

TEST_CASES = [
    {
        "id": 1,
        "query": "What is the company travel expense reimbursement limit?",
        "user_role": "engineer",
        "user_id": "eval_user_1",
        "intent": "policy",
        "expected_keywords": ["travel", "policy", "reimburse", "expense", "limit", "approval"],
    },
    {
        "id": 2,
        "query": "How many employees are in each department?",
        "user_role": "manager",
        "user_id": "eval_user_2",
        "intent": "employee",
        "expected_keywords": ["department", "employees", "headcount", "human resources", "engineering"],
    },
    {
        "id": 3,
        "query": "What is the total amount spent on expenses across the company?",
        "user_role": "finance",
        "user_id": "eval_user_3",
        "intent": "expense",
        "expected_keywords": ["total", "expense", "amount", "spent"],
    },
    {
        "id": 4,
        "query": "Show me the budget status of all projects.",
        "user_role": "manager",
        "user_id": "eval_user_4",
        "intent": "project",
        "expected_keywords": ["budget", "project", "spent", "remaining"],
    },
    {
        "id": 5,
        "query": "What health insurance plans does TechCorp offer?",
        "user_role": "hr",
        "user_id": "eval_user_5",
        "intent": "benefits",
        "expected_keywords": ["health", "plan", "insurance", "dental", "vision"],
    },
    {
        "id": 6,
        "query": "What is the average PTO days employees get per year?",
        "user_role": "hr",
        "user_id": "eval_user_6",
        "intent": "benefits",
        "expected_keywords": ["pto", "days", "average", "vacation", "time off"],
    },
    {
        "id": 7,
        "query": "What are the expense categories with the highest total spending?",
        "user_role": "finance",
        "user_id": "eval_user_7",
        "intent": "expense",
        "expected_keywords": ["category", "conference", "software", "travel", "total"],
    },
    {
        "id": 8,
        "query": "What is TechCorp's remote work policy?",
        "user_role": "engineer",
        "user_id": "eval_user_8",
        "intent": "policy",
        "expected_keywords": ["remote", "work", "policy", "home", "office"],
    },
    {
        "id": 9,
        "query": "Which projects are currently active?",
        "user_role": "engineer",
        "user_id": "eval_user_9",
        "intent": "project",
        "expected_keywords": ["active", "project", "status"],
    },
    {
        "id": 10,
        "query": "What is the GDPR compliance policy at TechCorp?",
        "user_role": "engineer",
        "user_id": "eval_user_10",
        "intent": "policy",
        "expected_keywords": ["gdpr", "data", "compliance", "privacy", "personal"],
    },
]

# ---------------------------------------------------------------------------
# Worker script — runs inside a subprocess with DOCS_PATH set
# Prints a JSON array of results to stdout.
# ---------------------------------------------------------------------------

_WORKER_SCRIPT = """
import sys, os, json, time
sys.path.insert(0, sys.argv[1])   # week6/app

os.environ["DOCS_PATH"] = sys.argv[2]   # corpus path

import guardrails as g
from agent import classify_intent, run_tools

def check_correctness(answer, expected_keywords):
    answer_lower = answer.lower()
    hits = sum(1 for kw in expected_keywords if kw.lower() in answer_lower)
    score = hits / len(expected_keywords) if expected_keywords else 0.0
    return score >= 0.5, round(score, 2)

test_cases = json.loads(sys.argv[3])
results = []

for tc in test_cases:
    start = time.time()
    try:
        intent = classify_intent(tc["query"])
        tools_called, context, docs_retrieved, docs_denied = run_tools(
            intent, tc["query"], tc["user_role"], tc["user_id"]
        )
        latency_ms    = (time.time() - start) * 1000
        passed, score = check_correctness(context, tc["expected_keywords"])
        input_tokens  = g.estimate_tokens(tc["query"] + context)
        output_tokens = 200
        cost          = ((input_tokens + output_tokens) / 1000) * g.COST_PER_1K_TOKENS

        results.append({
            **tc,
            "status":          "ok",
            "context_snippet": context[:300],
            "tools_called":    [t.tool for t in tools_called],
            "tokens_input":    input_tokens,
            "tokens_output":   output_tokens,
            "total_tokens":    input_tokens + output_tokens,
            "cost_usd":        round(cost, 6),
            "latency_ms":      round(latency_ms, 1),
            "correct":         passed,
            "correct_score":   score,
            "detected_intent": intent,
            "docs_retrieved":  docs_retrieved,
            "docs_denied":     docs_denied,
        })
    except g.AuthorizationError as e:
        latency_ms = (time.time() - start) * 1000
        results.append({**tc, "status": "auth_error", "error": str(e),
                        "latency_ms": latency_ms, "correct": False, "correct_score": 0.0})
    except Exception as e:
        latency_ms = (time.time() - start) * 1000
        results.append({**tc, "status": "error", "error": str(e),
                        "latency_ms": latency_ms, "correct": False, "correct_score": 0.0})

print(json.dumps(results))
"""

# ---------------------------------------------------------------------------
# Run evaluation in an isolated subprocess
# ---------------------------------------------------------------------------

def run_eval_offline(docs_path: str, label: str) -> list:
    """
    Spawn a fresh Python process with DOCS_PATH pointing at docs_path so the
    TF-IDF index is built from scratch for this corpus.
    """
    print(f"\n{'='*70}")
    print(f"  Evaluating: {label}")
    print(f"  Corpus:     {docs_path}")
    print(f"  Documents:  ", end="", flush=True)

    with open(docs_path) as f:
        ndocs = len(json.load(f))
    print(ndocs)
    print(f"{'='*70}")

    # Write worker to a temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as tf:
        tf.write(_WORKER_SCRIPT)
        worker_path = tf.name

    try:
        proc = subprocess.run(
            [
                sys.executable,
                worker_path,
                WEEK6_APP,
                docs_path,
                json.dumps(TEST_CASES),
            ],
            capture_output=True,
            text=True,
        )

        if proc.returncode != 0:
            print(f"  Worker stderr:\n{proc.stderr[:1000]}")
            return []

        results = json.loads(proc.stdout.strip())

        for r in results:
            status = "✓" if r["status"] == "ok" else "✗"
            if r["status"] == "ok":
                print(f"  [{r['id']:2d}/10] {r['query'][:50]:<50} "
                      f"{status} score={r['correct_score']:.2f} "
                      f"tokens={r['total_tokens']:,} "
                      f"cost=${r['cost_usd']:.5f} "
                      f"latency={r['latency_ms']:.0f}ms")
            else:
                print(f"  [{r['id']:2d}/10] {r['query'][:50]:<50} "
                      f"{status} {r.get('error', r['status'])[:60]}")

        return results

    finally:
        os.unlink(worker_path)


# ---------------------------------------------------------------------------
# Aggregate summary stats
# ---------------------------------------------------------------------------

def summarize(results: list) -> dict:
    ok = [r for r in results if r["status"] == "ok"]
    if not ok:
        return {"error": "no successful results"}

    latencies   = sorted(r["latency_ms"] for r in ok)
    costs       = [r["cost_usd"] for r in ok]
    tokens_list = [r["total_tokens"] for r in ok]
    correct     = sum(1 for r in ok if r.get("correct", False))

    p50 = statistics.median(latencies)
    p95 = latencies[max(0, int(len(latencies) * 0.95) - 1)]
    p99 = latencies[-1]

    avg_cost   = statistics.mean(costs)
    avg_tokens = statistics.mean(tokens_list)

    return {
        "n":                  len(ok),
        "accuracy":           round(correct / len(ok), 3),
        "correct_count":      correct,
        "avg_tokens":         round(avg_tokens, 1),
        "avg_cost_usd":       round(avg_cost, 6),
        "total_cost_usd":     round(sum(costs), 6),
        "latency_p50_ms":     round(p50, 1),
        "latency_p95_ms":     round(p95, 1),
        "latency_p99_ms":     round(p99, 1),
        "daily_cost_proj":    round(avg_cost * 1000, 2),
        "monthly_cost_proj":  round(avg_cost * 30000, 2),
    }


# ---------------------------------------------------------------------------
# Corpus quality analysis
# ---------------------------------------------------------------------------

def analyze_corpus(docs_path: str) -> dict:
    with open(docs_path) as f:
        docs = json.load(f)

    total        = len(docs)
    quality_docs = [d for d in docs if d.get("is_quality")]

    problem_counts   = Counter()
    problem_examples = defaultdict(list)

    for doc in docs:
        doc_id = doc.get("id", "?")

        if doc.get("is_high_retrieval_waste"):
            problem_counts["misleading_high_retrieval"] += 1
            if len(problem_examples["misleading_high_retrieval"]) < 3:
                problem_examples["misleading_high_retrieval"].append(
                    {"id": doc_id, "title": doc.get("title", ""),
                     "content_preview": doc.get("content", "")[:80]}
                )

        if "incomplete" in doc_id:
            problem_counts["incomplete_draft"] += 1
            if len(problem_examples["incomplete_draft"]) < 3:
                problem_examples["incomplete_draft"].append(
                    {"id": doc_id, "title": doc.get("title", ""),
                     "content_preview": doc.get("content", "")[:80]}
                )

        if "_summary_" in doc_id:
            problem_counts["near_duplicate"] += 1
            if len(problem_examples["near_duplicate"]) < 3:
                problem_examples["near_duplicate"].append(
                    {"id": doc_id, "title": doc.get("title", ""),
                     "original_id": doc_id.split("_summary_")[0]}
                )

        if "_v" in doc_id and not doc.get("is_quality"):
            problem_counts["old_version"] += 1
            if len(problem_examples["old_version"]) < 3:
                problem_examples["old_version"].append(
                    {"id": doc_id, "version": doc.get("version", ""),
                     "last_updated": doc.get("last_updated", "")}
                )

        if "_miscat" in doc_id:
            problem_counts["miscategorized"] += 1
            if len(problem_examples["miscategorized"]) < 3:
                problem_examples["miscategorized"].append(
                    {"id": doc_id, "category": doc.get("category", ""),
                     "problem": doc.get("_problem", "")}
                )

        if doc.get("valid_only"):
            problem_counts["seasonal_temporal"] += 1
            if len(problem_examples["seasonal_temporal"]) < 3:
                problem_examples["seasonal_temporal"].append(
                    {"id": doc_id, "title": doc.get("title", ""),
                     "valid_only": doc.get("valid_only", "")}
                )

        if "_old_contradicts" in doc_id:
            problem_counts["contradicting_policy"] += 1
            if len(problem_examples["contradicting_policy"]) < 3:
                problem_examples["contradicting_policy"].append(
                    {"id": doc_id, "version": doc.get("version", ""),
                     "content_preview": doc.get("content", "")[:80]}
                )

    junk_total   = total - len(quality_docs)
    signal_ratio = len(quality_docs) / total if total else 0

    return {
        "total_documents":   total,
        "quality_documents": len(quality_docs),
        "junk_documents":    junk_total,
        "signal_ratio":      round(signal_ratio, 4),
        "problem_counts":    dict(problem_counts),
        "problem_examples":  dict(problem_examples),
    }


# ---------------------------------------------------------------------------
# Build comparison report
# ---------------------------------------------------------------------------

def build_report(clean_results: list, bloated_results: list,
                 corpus_analysis: dict) -> dict:
    clean_summary   = summarize(clean_results)
    bloated_summary = summarize(bloated_results)

    clean_by_id   = {r["id"]: r for r in clean_results   if r["status"] == "ok"}
    bloated_by_id = {r["id"]: r for r in bloated_results if r["status"] == "ok"}

    query_deltas = []
    for qid in sorted(set(clean_by_id) & set(bloated_by_id)):
        c = clean_by_id[qid]
        b = bloated_by_id[qid]
        token_pct = ((b["total_tokens"] - c["total_tokens"])
                     / max(c["total_tokens"], 1) * 100)
        query_deltas.append({
            "id":                 qid,
            "query":              c["query"][:60],
            "intent":             c["intent"],
            "clean_tokens":       c["total_tokens"],
            "bloated_tokens":     b["total_tokens"],
            "token_increase_pct": round(token_pct, 1),
            "clean_cost":         c["cost_usd"],
            "bloated_cost":       b["cost_usd"],
            "clean_correct":      c["correct_score"],
            "bloated_correct":    b["correct_score"],
            "clean_latency_ms":   c["latency_ms"],
            "bloated_latency_ms": b["latency_ms"],
        })

    cost_x    = round(bloated_summary["avg_cost_usd"]   / max(clean_summary["avg_cost_usd"],   1e-9), 2)
    latency_x = round(bloated_summary["latency_p50_ms"] / max(clean_summary["latency_p50_ms"], 1e-9), 2)
    token_x   = round(bloated_summary["avg_tokens"]     / max(clean_summary["avg_tokens"],     1),    2)
    acc_delta = round(bloated_summary["accuracy"] - clean_summary["accuracy"], 3)

    corpus = corpus_analysis
    root_cause = {
        "hypothesis": "Corpus bloat is the primary root cause of all three symptoms.",
        "evidence": {
            "cost_increase":
                f"Avg tokens/query increased {token_x}x "
                f"({clean_summary['avg_tokens']} → {bloated_summary['avg_tokens']}). "
                "More junk docs retrieved → more tokens sent to LLM → higher cost.",
            "latency_increase":
                f"Latency p50 increased {latency_x}x. "
                f"Larger TF-IDF index ({corpus['total_documents']} docs vs "
                f"{corpus['quality_documents']} quality docs) slows retrieval.",
            "accuracy_drop":
                f"Accuracy changed {acc_delta:+.1%}. "
                f"Misleading ({corpus['problem_counts'].get('misleading_high_retrieval', 0)}), "
                f"contradicting ({corpus['problem_counts'].get('contradicting_policy', 0)}), "
                f"and incomplete ({corpus['problem_counts'].get('incomplete_draft', 0)}) "
                "docs retrieved alongside real docs confuse the answer context.",
        },
        "signal_to_noise":
            f"{corpus['signal_ratio']:.1%} of corpus is quality content "
            f"({corpus['quality_documents']}/{corpus['total_documents']} docs)",
        "biggest_problem_category":
            max(corpus["problem_counts"], key=corpus["problem_counts"].get)
            if corpus["problem_counts"] else "unknown",
    }

    return {
        "report_generated":       time.strftime("%Y-%m-%dT%H:%M:%S"),
        "clean_corpus_summary":   clean_summary,
        "bloated_corpus_summary": bloated_summary,
        "degradation_multipliers": {
            "cost_x":        cost_x,
            "latency_x":     latency_x,
            "token_x":       token_x,
            "accuracy_delta": acc_delta,
        },
        "per_query_deltas": query_deltas,
        "corpus_analysis":  corpus,
        "root_cause":       root_cause,
    }


# ---------------------------------------------------------------------------
# Print summary
# ---------------------------------------------------------------------------

def print_report(report: dict):
    c      = report["clean_corpus_summary"]
    b      = report["bloated_corpus_summary"]
    d      = report["degradation_multipliers"]
    corpus = report["corpus_analysis"]
    rc     = report["root_cause"]

    print("\n" + "=" * 72)
    print("  WEEK 7 DEGRADATION DIAGNOSIS REPORT")
    print("=" * 72)

    print(f"\n{'Metric':<28} {'Clean':>12} {'Bloated':>12} {'Change':>12}")
    print("-" * 68)
    print(f"  {'Accuracy':<26} {c['accuracy']:>11.1%} {b['accuracy']:>11.1%} "
          f"{d['accuracy_delta']:>+11.1%}")
    print(f"  {'Avg tokens/query':<26} {c['avg_tokens']:>12,.0f} {b['avg_tokens']:>12,.0f} "
          f"{d['token_x']:>11.1f}x")
    print(f"  {'Avg cost/query':<26} ${c['avg_cost_usd']:>10.5f} ${b['avg_cost_usd']:>10.5f} "
          f"{d['cost_x']:>11.1f}x")
    print(f"  {'Latency p50 (ms)':<26} {c['latency_p50_ms']:>12.1f} {b['latency_p50_ms']:>12.1f} "
          f"{d['latency_x']:>11.1f}x")
    print(f"  {'Monthly cost (30k q/mo)':<26} ${c['monthly_cost_proj']:>10,.2f} "
          f"${b['monthly_cost_proj']:>10,.2f}")

    print(f"\nCORPUS ANALYSIS")
    print(f"  Total docs in bloated corpus:  {corpus['total_documents']:,}")
    print(f"  Quality docs:                  {corpus['quality_documents']:,}")
    print(f"  Junk docs:                     {corpus['junk_documents']:,}")
    print(f"  Signal-to-noise ratio:         {corpus['signal_ratio']:.1%}")
    print(f"\n  Problem breakdown:")
    for cat, count in sorted(corpus["problem_counts"].items(), key=lambda x: -x[1]):
        print(f"    {cat:<38} {count:>5}")

    print(f"\nROOT CAUSE")
    print(f"  Hypothesis: {rc['hypothesis']}")
    print(f"\n  Evidence:")
    for symptom, evidence in rc["evidence"].items():
        print(f"    [{symptom}]")
        print(f"      {evidence}")
    print(f"\n  Signal-to-noise: {rc['signal_to_noise']}")
    print(f"  Biggest problem: {rc['biggest_problem_category']}")

    print("\n" + "=" * 72)
    print("  PER-QUERY BREAKDOWN")
    print("=" * 72)
    print(f"  {'#':>2}  {'Query':<44} {'Intent':<10} {'Token Δ':>8} {'Acc Δ':>7}")
    print("  " + "-" * 74)
    for q in report["per_query_deltas"]:
        acc_delta = q["bloated_correct"] - q["clean_correct"]
        print(f"  {q['id']:>2}  {q['query']:<44} {q['intent']:<10} "
              f"{q['token_increase_pct']:>+7.0f}% {acc_delta:>+6.2f}")

    print("=" * 72)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Diagnose Week 7 degradation")
    parser.add_argument("--clean-corpus",   default=DEFAULT_CLEAN)
    parser.add_argument("--bloated-corpus", default=DEFAULT_BLOATED)
    parser.add_argument("--output",         default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    for label, path in [("clean corpus",   args.clean_corpus),
                         ("bloated corpus", args.bloated_corpus)]:
        if not os.path.exists(path):
            print(f"ERROR: {label} not found at {path}")
            sys.exit(1)

    print("TechCorp Week 7 — Degradation Diagnosis")
    print(f"  Clean corpus:   {args.clean_corpus}")
    print(f"  Bloated corpus: {args.bloated_corpus}")

    clean_results   = run_eval_offline(args.clean_corpus,   "clean")
    bloated_results = run_eval_offline(args.bloated_corpus, "bloated")

    print(f"\nAnalysing corpus composition...")
    corpus_analysis = analyze_corpus(args.bloated_corpus)

    report = build_report(clean_results, bloated_results, corpus_analysis)
    print_report(report)

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nFull report saved to: {args.output}")
