"""
Hourly runner: fetches metrics, runs Margin Optimizer, and updates margin.
Run via cron every hour:
  python run_optimizer.py

Or run with CSV data only:
  python run_optimizer.py --csv "path/to/your.csv" --arm LowMar
"""
import argparse
import csv
import sys
from pathlib import Path

from api_client import fetch_hourly_metrics, update_margin
from margin_optimizer import MarginOptimizer


def load_metrics_from_csv(csv_path: str, arm_contains: str) -> dict:
    """Load metrics for one arm from CSV. arm_contains matches Demand Name (e.g. LowMar)."""
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    arm_lower = arm_contains.lower()
    for r in rows:
        if arm_lower in str(r.get("Demand Name", "")).lower():
            impr = float(r.get("Supply Impressions", 0) or 0)
            rev = float(r.get("Revenue", 0) or 0)
            cost = float(r.get("Cost", 0) or 0)
            margin = float(r.get("Margin %", 0) or 0)
            bid_rate = float(r.get("Demand Bid Rate %", 0) or 0)
            resp = float(r.get("Supply Responses", 0) or 0)
            return {
                "impressions": impr,
                "revenue": rev,
                "cost": cost,
                "margin": margin,
                "bid_rate": bid_rate,
                "responses": resp,
            }
    raise ValueError(f"No row matching '{arm_contains}' in {csv_path}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", help="Use CSV file instead of API. Provide path to CSV.")
    ap.add_argument("--arm", default="LowMar", help="Arm to use when --csv (e.g. LowMar, HighMar, MidMar)")
    args = ap.parse_args()

    if args.csv:
        data = load_metrics_from_csv(args.csv, args.arm)
    else:
        data = fetch_hourly_metrics()

    # 2. Initialize optimizer (loads state from JSON/S3 if exists; use fresh state when --csv)
    state_path = Path(__file__).parent / "optimizer_state.json"
    if args.csv:
        state_path = Path(__file__).parent / "optimizer_state_csv_run.json"
    opt = MarginOptimizer(
        baseline_margin=35.0,  # Start near LowMar
        step=1.0,
        min_step=0.25,
        min_impressions_per_decision=0,
        min_profit_per_decision=0.0,
        guardrail_drop_pct=10.0,
        min_profit_improvement_pct=2.0,
        state_path=state_path,
    )

    # 3. Suggest next margin
    current_margin = data.get("margin", 35.0)
    next_margin = opt.suggest_next_margin(
        margin=current_margin,
        impressions=data["impressions"],
        revenue=data["revenue"],
        cost=data["cost"],
        bid_rate=data["bid_rate"],
        responses=data.get("responses", 0),
    )

    # 4. Apply via API
    success = update_margin(next_margin)
    if not success:
        print("Warning: failed to update margin", file=sys.stderr)
        return 1

    # 5. Save run log to S3 (if S3_BUCKET is set)
    try:
        from s3_storage import save_run_log
        save_run_log(
            current_margin=current_margin,
            next_margin=next_margin,
            metrics=data,
            success=success,
        )
    except ImportError:
        pass

    print(f"Margin updated: {current_margin}% -> {next_margin}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
