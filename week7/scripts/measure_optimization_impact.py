"""
measure_optimization_impact.py — Week 7: Before/after comparison after corpus cleanup.

Runs the same 10 evaluation queries against three corpora:
  1. clean (original documents.json — the baseline)
  2. bloated (documents_week7_bloated.json — the degraded state)
  3. cleaned (documents_week7_cleaned.json — after cost_optimization.py)

Produces a side-by-side report and saves matplotlib graphs:
  - cost_latency_comparison.png  (bar charts: avg cost and latency)
  - accuracy_comparison.png      (bar chart: accuracy %)
  - token_comparison.png         (bar chart: avg tokens/query)
  - per_query_heatmap.png        (token delta across queries × corpora)

Usage:
    python week7/scripts/measure_optimization_impact.py

    # Explicit paths:
    python week7/scripts/measure_optimization_impact.py \\
        --baseline  data/raw/techcorp/documents.json \\
        --bloated   data/raw/techcorp/documents_week7_bloated.json \\
        --cleaned   data/raw/techcorp/documents_week7_cleaned.json \\
        --output    week7/optimization_impact_report.json \\
        --plots-dir week7/plots
"""

import sys
import os
import json
import time
import argparse
import statistics
import subprocess
import tempfile
from collections import defaultdict

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
WEEK7_APP = os.path.join(REPO_ROOT, "week7", "app")

DEFAULT_BASELINE = os.path.join(REPO_ROOT, "data", "raw", "techcorp", "documents.json")
DEFAULT_BLOATED  = os.path.join(REPO_ROOT, "data", "raw", "techcorp", "documents_week7_bloated.json")
DEFAULT_CLEANED  = os.path.join(REPO_ROOT, "data", "raw", "techcorp", "documents_week7_cleaned.json")
DEFAULT_OUTPUT   = os.path.join(os.path.dirname(__file__), "..", "optimization_impact_report.json")
DEFAULT_PLOTS    = os.path.join(os.path.dirname(__file__), "..", "plots")

# ---------------------------------------------------------------------------
# Test cases (same as diagnose_degradation.py)
# ---------------------------------------------------------------------------

TEST_CASES = [
    {"id": 1,  "query": "What is the company travel expense reimbursement limit?",
     "user_role": "engineer", "user_id": "eval_user_1",  "intent": "policy",
     "expected_keywords": ["travel", "policy", "reimburse", "expense", "limit", "approval"]},
    {"id": 2,  "query": "How many employees are in each department?",
     "user_role": "manager",  "user_id": "eval_user_2",  "intent": "employee",
     "expected_keywords": ["department", "employees", "headcount", "human resources", "engineering"]},
    {"id": 3,  "query": "What is the total amount spent on expenses across the company?",
     "user_role": "finance",  "user_id": "eval_user_3",  "intent": "expense",
     "expected_keywords": ["total", "expense", "amount", "spent"]},
    {"id": 4,  "query": "Show me the budget status of all projects.",
     "user_role": "manager",  "user_id": "eval_user_4",  "intent": "project",
     "expected_keywords": ["budget", "project", "spent", "remaining"]},
    {"id": 5,  "query": "What health insurance plans does TechCorp offer?",
     "user_role": "hr",       "user_id": "eval_user_5",  "intent": "benefits",
     "expected_keywords": ["health", "plan", "insurance", "dental", "vision"]},
    {"id": 6,  "query": "What is the average PTO days employees get per year?",
     "user_role": "hr",       "user_id": "eval_user_6",  "intent": "benefits",
     "expected_keywords": ["pto", "days", "average", "vacation", "time off"]},
    {"id": 7,  "query": "What are the expense categories with the highest total spending?",
     "user_role": "finance",  "user_id": "eval_user_7",  "intent": "expense",
     "expected_keywords": ["category", "conference", "software", "travel", "total"]},
    {"id": 8,  "query": "What is TechCorp's remote work policy?",
     "user_role": "engineer", "user_id": "eval_user_8",  "intent": "policy",
     "expected_keywords": ["remote", "work", "policy", "home", "office"]},
    {"id": 9,  "query": "Which projects are currently active?",
     "user_role": "engineer", "user_id": "eval_user_9",  "intent": "project",
     "expected_keywords": ["active", "project", "status"]},
    {"id": 10, "query": "What is the GDPR compliance policy at TechCorp?",
     "user_role": "engineer", "user_id": "eval_user_10", "intent": "policy",
     "expected_keywords": ["gdpr", "data", "compliance", "privacy", "personal"]},
]

# ---------------------------------------------------------------------------
# Worker script (identical pattern to diagnose_degradation.py)
# ---------------------------------------------------------------------------

_WORKER_SCRIPT = """
import sys, os, json, time
sys.path.insert(0, sys.argv[1])
os.environ["DOCS_PATH"] = sys.argv[2]

import guardrails as g
from agent import classify_intent, run_tools

def check_correctness(answer, keywords):
    hits = sum(1 for kw in keywords if kw.lower() in answer.lower())
    score = hits / len(keywords) if keywords else 0.0
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
    except Exception as e:
        latency_ms = (time.time() - start) * 1000
        results.append({**tc, "status": "error", "error": str(e),
                        "latency_ms": latency_ms, "correct": False, "correct_score": 0.0})

print(json.dumps(results))
"""

# ---------------------------------------------------------------------------
# Run one corpus in isolation
# ---------------------------------------------------------------------------

def run_eval(docs_path: str, label: str) -> list:
    with open(docs_path) as f:
        ndocs = len(json.load(f))

    print(f"\n  [{label}]  {ndocs} docs — {docs_path}")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as tf:
        tf.write(_WORKER_SCRIPT)
        worker_path = tf.name

    try:
        proc = subprocess.run(
            [sys.executable, worker_path, WEEK7_APP, docs_path, json.dumps(TEST_CASES)],
            capture_output=True, text=True,
        )
        if proc.returncode != 0:
            print(f"  ERROR: {proc.stderr[:500]}")
            return []
        results = json.loads(proc.stdout.strip())
        ok = sum(1 for r in results if r["status"] == "ok")
        correct = sum(1 for r in results if r.get("correct"))
        avg_tokens = statistics.mean(r["total_tokens"] for r in results if r["status"] == "ok") if ok else 0
        avg_cost   = statistics.mean(r["cost_usd"]     for r in results if r["status"] == "ok") if ok else 0
        print(f"         accuracy={correct}/{ok}  avg_tokens={avg_tokens:,.0f}  avg_cost=${avg_cost:.5f}")
        return results
    finally:
        os.unlink(worker_path)


# ---------------------------------------------------------------------------
# Summarise a result set
# ---------------------------------------------------------------------------

def summarize(results: list) -> dict:
    ok = [r for r in results if r["status"] == "ok"]
    if not ok:
        return {}
    latencies = sorted(r["latency_ms"] for r in ok)
    costs     = [r["cost_usd"]     for r in ok]
    tokens    = [r["total_tokens"] for r in ok]
    correct   = sum(1 for r in ok if r.get("correct"))
    return {
        "n":                 len(ok),
        "accuracy":          round(correct / len(ok), 3),
        "avg_tokens":        round(statistics.mean(tokens), 1),
        "avg_cost_usd":      round(statistics.mean(costs),  6),
        "total_cost_usd":    round(sum(costs), 6),
        "latency_p50_ms":    round(statistics.median(latencies), 1),
        "latency_p95_ms":    round(latencies[max(0, int(len(latencies) * 0.95) - 1)], 1),
        "monthly_cost_proj": round(statistics.mean(costs) * 30_000, 2),
    }


# ---------------------------------------------------------------------------
# Compute recovery percentages vs bloated baseline
# ---------------------------------------------------------------------------

def recovery_stats(bloated: dict, cleaned: dict, baseline: dict) -> dict:
    def pct_recovered(bloated_val, cleaned_val, baseline_val):
        """How much of the gap from baseline→bloated was closed by cleaning?"""
        gap = bloated_val - baseline_val
        if abs(gap) < 1e-9:
            return 100.0
        improvement = bloated_val - cleaned_val
        return round(improvement / gap * 100, 1)

    return {
        "cost_recovered_pct":     pct_recovered(bloated["avg_cost_usd"],   cleaned["avg_cost_usd"],   baseline["avg_cost_usd"]),
        "tokens_recovered_pct":   pct_recovered(bloated["avg_tokens"],     cleaned["avg_tokens"],     baseline["avg_tokens"]),
        "latency_recovered_pct":  pct_recovered(bloated["latency_p50_ms"], cleaned["latency_p50_ms"], baseline["latency_p50_ms"]),
        "accuracy_recovered_pct": pct_recovered(
            1 - bloated["accuracy"], 1 - cleaned["accuracy"], 1 - baseline["accuracy"]
        ),
        "cost_reduction_vs_bloated_pct": round(
            (bloated["avg_cost_usd"] - cleaned["avg_cost_usd"]) / max(bloated["avg_cost_usd"], 1e-9) * 100, 1
        ),
        "token_reduction_vs_bloated_pct": round(
            (bloated["avg_tokens"] - cleaned["avg_tokens"]) / max(bloated["avg_tokens"], 1) * 100, 1
        ),
    }


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def make_plots(summaries: dict, per_query: dict, plots_dir: str):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        import numpy as np
    except ImportError:
        print("  matplotlib not available — skipping plots. Install with: pip install matplotlib")
        return []

    os.makedirs(plots_dir, exist_ok=True)
    saved = []

    labels = ["Baseline\n(clean)", "Bloated\n(degraded)", "Cleaned\n(recovered)"]
    colours = ["#2ecc71", "#e74c3c", "#3498db"]
    keys    = ["baseline", "bloated", "cleaned"]

    # ── 1. Cost & Latency side-by-side ──────────────────────────────────────
    fig, axes = plt.subplots(1, 2, figsize=(10, 5))
    fig.suptitle("Cost & Latency: Before vs After Corpus Cleanup", fontweight="bold")

    costs    = [summaries[k]["avg_cost_usd"]   * 1000 for k in keys]  # millidollars
    latencies = [summaries[k]["latency_p50_ms"] for k in keys]

    for ax, values, ylabel, title, fmt in [
        (axes[0], costs,     "Cost (milli-USD / query)", "Avg Cost per Query",   "{:.3f}"),
        (axes[1], latencies, "Latency p50 (ms)",         "Latency p50",          "{:.1f}"),
    ]:
        bars = ax.bar(labels, values, color=colours, width=0.5, edgecolor="white")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.set_ylim(0, max(values) * 1.3)
        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(values) * 0.02,
                    fmt.format(val), ha="center", va="bottom", fontsize=9, fontweight="bold")

    plt.tight_layout()
    path = os.path.join(plots_dir, "cost_latency_comparison.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    saved.append(path)

    # ── 2. Accuracy ──────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(7, 5))
    accuracies = [summaries[k]["accuracy"] * 100 for k in keys]
    bars = ax.bar(labels, accuracies, color=colours, width=0.4, edgecolor="white")
    ax.set_ylabel("Accuracy (%)")
    ax.set_title("Answer Accuracy: Before vs After Corpus Cleanup", fontweight="bold")
    ax.set_ylim(0, 110)
    for bar, val in zip(bars, accuracies):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f"{val:.0f}%", ha="center", va="bottom", fontsize=11, fontweight="bold")
    plt.tight_layout()
    path = os.path.join(plots_dir, "accuracy_comparison.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    saved.append(path)

    # ── 3. Avg Tokens per Query ──────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(7, 5))
    avg_tokens = [summaries[k]["avg_tokens"] for k in keys]
    bars = ax.bar(labels, avg_tokens, color=colours, width=0.4, edgecolor="white")
    ax.set_ylabel("Avg Tokens / Query")
    ax.set_title("Token Usage: Before vs After Corpus Cleanup", fontweight="bold")
    ax.set_ylim(0, max(avg_tokens) * 1.3)
    for bar, val in zip(bars, avg_tokens):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(avg_tokens) * 0.02,
                f"{val:,.0f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
    plt.tight_layout()
    path = os.path.join(plots_dir, "token_comparison.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    saved.append(path)

    # ── 4. Per-query token heatmap ───────────────────────────────────────────
    query_ids = [r["id"] for r in per_query["baseline"] if r["status"] == "ok"]
    base_by_id    = {r["id"]: r for r in per_query["baseline"] if r["status"] == "ok"}
    bloated_by_id = {r["id"]: r for r in per_query["bloated"]  if r["status"] == "ok"}
    cleaned_by_id = {r["id"]: r for r in per_query["cleaned"]  if r["status"] == "ok"}

    matrix = []
    row_labels = []
    for qid in query_ids:
        if qid in base_by_id and qid in bloated_by_id and qid in cleaned_by_id:
            b_tok  = base_by_id[qid]["total_tokens"]
            bl_tok = bloated_by_id[qid]["total_tokens"]
            cl_tok = cleaned_by_id[qid]["total_tokens"]
            matrix.append([b_tok, bl_tok, cl_tok])
            q_short = base_by_id[qid]["query"][:38] + "…"
            row_labels.append(f"Q{qid}: {q_short}")

    if matrix:
        fig, ax = plt.subplots(figsize=(9, len(matrix) * 0.55 + 1.5))
        data = np.array(matrix, dtype=float)
        im = ax.imshow(data, aspect="auto", cmap="YlOrRd")
        ax.set_xticks([0, 1, 2])
        ax.set_xticklabels(["Baseline", "Bloated", "Cleaned"])
        ax.set_yticks(range(len(row_labels)))
        ax.set_yticklabels(row_labels, fontsize=8)
        for i in range(len(matrix)):
            for j in range(3):
                ax.text(j, i, f"{int(data[i, j]):,}", ha="center", va="center",
                        fontsize=7, color="black" if data[i, j] < data.max() * 0.7 else "white")
        plt.colorbar(im, ax=ax, label="Tokens")
        ax.set_title("Tokens per Query × Corpus", fontweight="bold", pad=10)
        plt.tight_layout()
        path = os.path.join(plots_dir, "per_query_heatmap.png")
        plt.savefig(path, dpi=150, bbox_inches="tight")
        plt.close()
        saved.append(path)

    return saved


# ---------------------------------------------------------------------------
# Print terminal report
# ---------------------------------------------------------------------------

def print_report(summaries: dict, recovery: dict):
    b  = summaries["baseline"]
    bl = summaries["bloated"]
    cl = summaries["cleaned"]

    print("\n" + "=" * 76)
    print("  OPTIMIZATION IMPACT REPORT")
    print("=" * 76)
    print(f"\n{'Metric':<28} {'Baseline':>11} {'Bloated':>11} {'Cleaned':>11} {'Recovery':>10}")
    print("-" * 76)

    rows = [
        ("Accuracy",          f"{b['accuracy']:.1%}",       f"{bl['accuracy']:.1%}",
         f"{cl['accuracy']:.1%}",  f"{recovery['accuracy_recovered_pct']:+.0f}%"),
        ("Avg tokens/query",  f"{b['avg_tokens']:,.0f}",    f"{bl['avg_tokens']:,.0f}",
         f"{cl['avg_tokens']:,.0f}", f"{recovery['token_reduction_vs_bloated_pct']:+.0f}%"),
        ("Avg cost/query",    f"${b['avg_cost_usd']:.5f}",  f"${bl['avg_cost_usd']:.5f}",
         f"${cl['avg_cost_usd']:.5f}", f"{recovery['cost_reduction_vs_bloated_pct']:+.0f}%"),
        ("Latency p50 (ms)",  f"{b['latency_p50_ms']:.1f}", f"{bl['latency_p50_ms']:.1f}",
         f"{cl['latency_p50_ms']:.1f}", f"{recovery['latency_recovered_pct']:+.0f}%"),
        ("Monthly cost proj", f"${b['monthly_cost_proj']:,.2f}", f"${bl['monthly_cost_proj']:,.2f}",
         f"${cl['monthly_cost_proj']:,.2f}", ""),
    ]

    for label, bv, blv, clv, rec in rows:
        print(f"  {label:<26} {bv:>11} {blv:>11} {clv:>11} {rec:>10}")

    print("\n" + "=" * 76)
    print("  RECOVERY SUMMARY (how much of degradation was reversed)")
    print("=" * 76)
    print(f"  Cost reduction vs bloated:      {recovery['cost_reduction_vs_bloated_pct']:+.1f}%")
    print(f"  Token reduction vs bloated:     {recovery['token_reduction_vs_bloated_pct']:+.1f}%")
    print(f"  Accuracy recovered:             {recovery['accuracy_recovered_pct']:+.1f}% of gap closed")
    print(f"  Latency recovered:              {recovery['latency_recovered_pct']:+.1f}% of gap closed")
    print("=" * 76)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Measure corpus cleanup impact")
    parser.add_argument("--baseline",  default=DEFAULT_BASELINE)
    parser.add_argument("--bloated",   default=DEFAULT_BLOATED)
    parser.add_argument("--cleaned",   default=DEFAULT_CLEANED)
    parser.add_argument("--output",    default=DEFAULT_OUTPUT)
    parser.add_argument("--plots-dir", default=DEFAULT_PLOTS)
    args = parser.parse_args()

    for label, path in [("baseline", args.baseline),
                         ("bloated",  args.bloated),
                         ("cleaned",  args.cleaned)]:
        if not os.path.exists(path):
            print(f"ERROR: {label} corpus not found at {path}")
            sys.exit(1)

    print("TechCorp Week 7 — Optimization Impact Measurement")
    print("Running 10 eval queries against 3 corpora...\n")

    baseline_results = run_eval(args.baseline, "baseline")
    bloated_results  = run_eval(args.bloated,  "bloated")
    cleaned_results  = run_eval(args.cleaned,  "cleaned")

    summaries = {
        "baseline": summarize(baseline_results),
        "bloated":  summarize(bloated_results),
        "cleaned":  summarize(cleaned_results),
    }

    recovery = recovery_stats(summaries["bloated"], summaries["cleaned"], summaries["baseline"])

    print_report(summaries, recovery)

    # Save plots
    print("\nGenerating plots...")
    per_query = {"baseline": baseline_results, "bloated": bloated_results, "cleaned": cleaned_results}
    saved_plots = make_plots(summaries, per_query, args.plots_dir)
    for p in saved_plots:
        print(f"  Saved: {p}")

    # Save JSON report
    report = {
        "generated":  time.strftime("%Y-%m-%dT%H:%M:%S"),
        "summaries":  summaries,
        "recovery":   recovery,
        "per_query":  {k: v for k, v in per_query.items()},
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nFull report saved to: {args.output}")
