#!/usr/bin/env python3
"""
check_drift_thresholds.py
Week 4 — Monitoring, Drift Detection & Retraining Strategy

Reads monitoring_metrics.json (output of compute_monitoring_metrics.py)
and evaluates each metric against defined thresholds.  Exits with code 1
if retraining is recommended so CI/CD pipelines can act on the result.

Thresholds
----------
Metric                               | Warning     | Critical (retrain)
-------------------------------------|-------------|--------------------
PSI (any lag feature, global)        | > 0.10      | > 0.20
PSI (roll_mean_1day, Manhattan)      | > 0.10      | > 0.20
PSI (trip_count, global)             | > 0.10      | > 0.20
Correlation drop (Queens/Brooklyn)   | > 0.15      | > 0.35
Peak ratio shift                     | > 0.50      | > 1.00
Manhattan weekend ratio shift        | < -0.05     | < -0.10
Manhattan lag_1day mean ratio        | < 0.80      | < 0.60

PSI interpretation:
  < 0.10  → stable, no action
  0.10–0.20 → moderate shift, monitor
  > 0.20  → significant shift → trigger retraining

Usage
-----
python scripts/check_drift_thresholds.py \
    --metrics data/processed/monitoring_metrics.json \
    [--strict]   # exit 1 on WARNING, not just CRITICAL

Exit codes
----------
0 — all metrics within thresholds (no retraining needed)
1 — one or more CRITICAL thresholds breached (retraining recommended)
2 — input file not found or malformed
"""

import argparse
import json
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

# ── ANSI colours (disabled if not a TTY) ─────────────────────────────────────
import os
_USE_COLOR = sys.stdout.isatty() or os.environ.get("FORCE_COLOR")
def _red(s):    return f"\033[91m{s}\033[0m" if _USE_COLOR else s
def _yellow(s): return f"\033[93m{s}\033[0m" if _USE_COLOR else s
def _green(s):  return f"\033[92m{s}\033[0m" if _USE_COLOR else s
def _bold(s):   return f"\033[1m{s}\033[0m"  if _USE_COLOR else s


# ── Threshold definitions ─────────────────────────────────────────────────────
@dataclass
class Threshold:
    metric_key: str
    description: str
    warning_fn:  any  # callable(value) -> bool
    critical_fn: any  # callable(value) -> bool
    warning_msg: str
    critical_msg: str
    interpretation: str


THRESHOLDS = [
    # ── PSI: lag features (global) ──────────────────────────────────────────
    Threshold(
        metric_key="psi_lag_1day_global",
        description="PSI lag_1day (all zones)",
        warning_fn=lambda v: v > 0.10,
        critical_fn=lambda v: v > 0.20,
        warning_msg="Moderate distribution shift in lag_1day",
        critical_msg="Severe distribution shift in lag_1day — lag features unreliable",
        interpretation="Drift 2 (Manhattan lag deflation) propagating globally",
    ),
    Threshold(
        metric_key="psi_lag_1week_global",
        description="PSI lag_1week (all zones)",
        warning_fn=lambda v: v > 0.10,
        critical_fn=lambda v: v > 0.20,
        warning_msg="Moderate distribution shift in lag_1week",
        critical_msg="Severe distribution shift in lag_1week — lag features unreliable",
        interpretation="Weekly lag patterns have changed significantly",
    ),
    Threshold(
        metric_key="psi_roll_mean_1day_global",
        description="PSI roll_mean_1day (all zones)",
        warning_fn=lambda v: v > 0.10,
        critical_fn=lambda v: v > 0.20,
        warning_msg="Moderate distribution shift in roll_mean_1day",
        critical_msg="Severe distribution shift in roll_mean_1day — rolling averages unreliable",
        interpretation="Rolling demand signal has structurally changed",
    ),
    Threshold(
        metric_key="psi_roll_mean_1day_manhattan",
        description="PSI roll_mean_1day (Manhattan)",
        warning_fn=lambda v: v > 0.10,
        critical_fn=lambda v: v > 0.20,
        warning_msg="Moderate Manhattan roll_mean_1day shift",
        critical_msg="Severe Manhattan roll_mean_1day shift — model likely mis-predicting Manhattan",
        interpretation="Drift 2: Manhattan lag deflation is severe (PSI > 1.0)",
    ),
    Threshold(
        metric_key="psi_trip_count_global",
        description="PSI trip_count (all zones)",
        warning_fn=lambda v: v > 0.10,
        critical_fn=lambda v: v > 0.20,
        warning_msg="Moderate shift in trip_count distribution",
        critical_msg="Severe shift in trip_count distribution — target variable unstable",
        interpretation="Overall demand level has shifted, affecting model accuracy",
    ),
    # ── Correlation drop ────────────────────────────────────────────────────
    Threshold(
        metric_key="corr_baseline_vs_actual_queens",
        description="Correlation drop: zone_slot_baseline vs trip_count (Queens)",
        warning_fn=lambda v: v < 0.35,   # low current correlation
        critical_fn=lambda v: v < 0.10,
        warning_msg="zone_slot_baseline weakly correlated with trip_count in Queens",
        critical_msg="zone_slot_baseline essentially uncorrelated with trip_count in Queens",
        interpretation="Drift 3: outer-borough baseline scramble detected",
    ),
    Threshold(
        metric_key="corr_baseline_vs_actual_brooklyn",
        description="Correlation drop: zone_slot_baseline vs trip_count (Brooklyn)",
        warning_fn=lambda v: v < 0.35,
        critical_fn=lambda v: v < 0.10,
        warning_msg="zone_slot_baseline weakly correlated with trip_count in Brooklyn",
        critical_msg="zone_slot_baseline essentially uncorrelated with trip_count in Brooklyn",
        interpretation="Drift 3: outer-borough baseline scramble detected",
    ),
    # ── Peak shift ratio ─────────────────────────────────────────────────────
    Threshold(
        metric_key="peak_ratio_shift",
        description="Peak ratio shift (5-7am / 9-11am delta)",
        warning_fn=lambda v: v > 0.50,
        critical_fn=lambda v: v > 1.00,
        warning_msg="Morning demand pattern has shifted noticeably",
        critical_msg="Morning demand pattern has inverted — 5-7am now busier than 9-11am",
        interpretation="Drift 1: temporal peak shift — demand moved ~2 hours earlier",
    ),
    # ── Manhattan weekend concept drift ─────────────────────────────────────
    Threshold(
        metric_key="manhattan_weekend_ratio_shift",
        description="Manhattan weekend/weekday ratio shift",
        warning_fn=lambda v: v < -0.05,
        critical_fn=lambda v: v < -0.10,
        warning_msg="Manhattan weekends are becoming relatively quieter",
        critical_msg="Manhattan weekend demand pattern has significantly changed — concept drift",
        interpretation="Drift 4: Manhattan weekend concept drift (weekends now ~17% quieter)",
    ),
    # ── Manhattan lag mean ratio ─────────────────────────────────────────────
    Threshold(
        metric_key="manhattan_lag1day_mean_ratio",
        description="Manhattan lag_1day mean ratio (current/baseline)",
        warning_fn=lambda v: v < 0.80,
        critical_fn=lambda v: v < 0.60,
        warning_msg="Manhattan lag_1day mean has dropped >20% from baseline",
        critical_msg="Manhattan lag_1day mean has dropped >40% — lag features severely deflated",
        interpretation="Drift 2: Manhattan lag deflation (lag features reduced by ~55%)",
    ),
]


# ── Evaluation ────────────────────────────────────────────────────────────────
@dataclass
class CheckResult:
    metric_key: str
    description: str
    value: float
    status: str        # "OK", "WARNING", "CRITICAL"
    message: str
    interpretation: str


def evaluate(metrics: dict, strict: bool = False) -> list[CheckResult]:
    results = []
    for t in THRESHOLDS:
        entry = metrics.get(t.metric_key)
        if entry is None:
            results.append(CheckResult(
                metric_key=t.metric_key,
                description=t.description,
                value=float("nan"),
                status="MISSING",
                message=f"Metric '{t.metric_key}' not found in metrics file",
                interpretation=t.interpretation,
            ))
            continue

        val = entry.get("value") if isinstance(entry, dict) else entry
        if val is None or (isinstance(val, float) and val != val):
            results.append(CheckResult(
                metric_key=t.metric_key,
                description=t.description,
                value=float("nan"),
                status="MISSING",
                message="Metric value is null/NaN",
                interpretation=t.interpretation,
            ))
            continue

        val = float(val)
        if t.critical_fn(val):
            status, msg = "CRITICAL", t.critical_msg
        elif t.warning_fn(val):
            status, msg = "WARNING", t.warning_msg
        else:
            status, msg = "OK", "Within expected range"

        results.append(CheckResult(
            metric_key=t.metric_key,
            description=t.description,
            value=val,
            status=status,
            message=msg,
            interpretation=t.interpretation,
        ))
    return results


# ── Reporting ─────────────────────────────────────────────────────────────────
def print_report(results: list[CheckResult], strict: bool) -> None:
    print()
    print(_bold("═" * 72))
    print(_bold("  DRIFT THRESHOLD CHECK REPORT — Week 4"))
    print(_bold("═" * 72))
    print()

    status_counts = {"OK": 0, "WARNING": 0, "CRITICAL": 0, "MISSING": 0}

    for r in results:
        status_counts[r.status] = status_counts.get(r.status, 0) + 1

        if r.status == "CRITICAL":
            badge = _red(f"[{r.status}]")
        elif r.status == "WARNING":
            badge = _yellow(f"[{r.status}]")
        elif r.status == "OK":
            badge = _green(f"[{r.status}    ]")
        else:
            badge = f"[{r.status}]"

        val_str = f"{r.value:.4f}" if r.value == r.value else "N/A"
        print(f"  {badge}  {r.description}")
        print(f"           value={val_str}  →  {r.message}")
        if r.status in ("WARNING", "CRITICAL"):
            print(f"           interpretation: {r.interpretation}")
        print()

    print(_bold("─" * 72))
    print(f"  Summary:  OK={status_counts['OK']}  "
          f"WARNING={status_counts['WARNING']}  "
          f"CRITICAL={status_counts['CRITICAL']}")

    needs_retrain = status_counts["CRITICAL"] > 0 or (strict and status_counts["WARNING"] > 0)
    if needs_retrain:
        print()
        print(_red(_bold("  ⚠  RETRAINING RECOMMENDED")))
        print(_red(  "     One or more CRITICAL thresholds breached."))
        print(_red(  "     Action: trigger retraining pipeline immediately."))
    else:
        print()
        print(_green(_bold("  ✓  No retraining required at this time.")))
    print(_bold("═" * 72))
    print()


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Check drift metrics against thresholds")
    parser.add_argument(
        "--metrics",
        default="data/processed/monitoring_metrics.json",
        help="Path to monitoring_metrics.json",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat WARNINGs as failures (exit code 1)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional: write check results as JSON to this path",
    )
    args = parser.parse_args()

    if not Path(args.metrics).exists():
        print(f"ERROR: metrics file not found: {args.metrics}", file=sys.stderr)
        sys.exit(2)

    try:
        with open(args.metrics) as f:
            payload = json.load(f)
        metrics = payload.get("metrics", payload)  # support both wrapped and flat
    except (json.JSONDecodeError, KeyError) as e:
        print(f"ERROR: could not parse metrics file: {e}", file=sys.stderr)
        sys.exit(2)

    results = evaluate(metrics, strict=args.strict)
    print_report(results, strict=args.strict)

    if args.output:
        out = [
            {
                "metric": r.metric_key,
                "description": r.description,
                "value": r.value if r.value == r.value else None,
                "status": r.status,
                "message": r.message,
            }
            for r in results
        ]
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(out, f, indent=2)
        print(f"Check results written to {args.output}")

    needs_retrain = any(r.status == "CRITICAL" for r in results)
    if args.strict:
        needs_retrain = needs_retrain or any(r.status == "WARNING" for r in results)

    sys.exit(1 if needs_retrain else 0)


if __name__ == "__main__":
    main()
