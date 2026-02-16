#!/usr/bin/env python3
"""
Pull CSV from S3, filter to last hour, run analysis + optimizer,
generate margin recommendations CSV, upload results back to S3.
"""
import os
import csv
import json
from pathlib import Path
from dotenv import load_dotenv
import boto3

load_dotenv(Path(__file__).parent / ".env")

client = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))
bucket = os.environ.get("S3_BUCKET")
prefix = os.environ.get("S3_PREFIX", "margin-optimizer/")

# ── STEP 0: Clean stale test state from S3 ──
print("=== STEP 0: Cleaning stale test state from S3 ===")
try:
    client.delete_object(Bucket=bucket, Key=prefix.rstrip("/") + "/optimizer_state.json")
    print("  Deleted stale optimizer_state.json from S3")
except Exception:
    print("  No stale state to clean")

# ── STEP 1: Download the data CSV from S3 ──
data_key = "MarginT/Margin Data - S3 file_analytics_report.csv"
print(f"\n=== STEP 1: Downloading {data_key} from S3 ===")
local_csv = Path(__file__).parent / "margin_data_from_s3.csv"
resp = client.get_object(Bucket=bucket, Key=data_key)
csv_bytes = resp["Body"].read()
local_csv.write_bytes(csv_bytes)
print(f"  Downloaded ({len(csv_bytes)} bytes)")

# ── STEP 2: Filter to last hour only ──
print("\n=== STEP 2: Filtering to last hour with data ===")
with open(local_csv, newline="", encoding="utf-8") as f:
    all_rows = list(csv.DictReader(f))

hours_with_data = set()
for r in all_rows:
    impr = float(r.get("Supply Impressions", 0) or 0)
    if impr > 0:
        hours_with_data.add(int(r["Hour"]))

last_hour = max(hours_with_data)
print(f"  Hours with data: {sorted(hours_with_data)}")
print(f"  Using last hour: {last_hour}")

last_hour_rows = [r for r in all_rows if int(r["Hour"]) == last_hour]
print(f"  Rows for hour {last_hour}: {len(last_hour_rows)}")
for r in last_hour_rows:
    print(f"    {r['Demand Name']}  impr={r['Supply Impressions']} rev={r['Revenue']} cost={r['Cost']} margin={r['Margin %']}")

# Write filtered CSV for the analysis logic
filtered_csv = Path(__file__).parent / "margin_data_last_hour.csv"
with open(filtered_csv, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=last_hour_rows[0].keys())
    writer.writeheader()
    writer.writerows(last_hour_rows)

# ── STEP 3: Run analysis on last hour data ──
print("\n=== STEP 3: Running analysis on last hour ===")
from analyze_margin_test import load_rows, compute_metrics, pick_winner, find_control, pick_recommended_winner

rows = load_rows(str(filtered_csv))
ms = compute_metrics(rows)
ms_sorted = sorted(ms, key=lambda x: x.profit_per_1k_impr, reverse=True)
winner = pick_winner(ms_sorted)
control = find_control(ms, "LowMar")
recommended = pick_recommended_winner(ms, control, 90.0) if control else winner

print("\nDerived KPIs (sorted by profit/1k impressions):")
for m in ms_sorted:
    print(
        f"  - {m.name}\n"
        f"    impressions={int(m.impressions)} responses={int(m.responses)}\n"
        f"    margin%={m.margin_pct:.2f} win%={m.win_rate_pct:.2f}\n"
        f"    profit=${m.profit:.4f} profit/1k=${m.profit_per_1k_impr:.4f}\n"
        f"    sRPM=${m.srpm:.4f}"
    )

print(f"\nWinner by profit/1k: {winner.name} (profit/1k=${winner.profit_per_1k_impr:.4f})")
if recommended:
    print(f"Recommended (with sRPM guardrail): {recommended.name}")
else:
    print(f"Recommended: KEEP CONTROL ({control.name if control else 'N/A'})")

# ── STEP 4: Run optimizer per arm and generate recommendations ──
print("\n=== STEP 4: Generating margin recommendations ===")
from margin_optimizer import MarginOptimizer

# Build a lookup: demand_name -> row from last_hour_rows
row_by_name = {}
for r in last_hour_rows:
    row_by_name[r["Demand Name"].strip()] = r

recommendations = []
for m in ms_sorted:
    safe_name = m.name.replace(" ", "_").replace("/", "_")
    state_path = Path(__file__).parent / f"optimizer_state_{safe_name}.json"
    # Write a fresh initial state locally so optimizer does NOT fall back to
    # the shared S3 state key (which would mix up per-arm baselines).
    fresh_state = {
        "baseline_margin": m.margin_pct,
        "last_safe_margin": m.margin_pct,
        "current_margin": m.margin_pct,
        "step": 1.0,
        "baseline_srpm": None,
        "baseline_bid_rate": None,
        "baseline_profit": None,
        "history": [],
    }
    with open(state_path, "w", encoding="utf-8") as sf:
        json.dump(fresh_state, sf, indent=2)

    opt = MarginOptimizer(
        baseline_margin=m.margin_pct,
        step=1.0,
        min_step=0.25,
        min_impressions_per_decision=0,
        min_profit_per_decision=0.0,
        guardrail_drop_pct=10.0,
        min_profit_improvement_pct=2.0,
        state_path=state_path,
    )

    row = row_by_name.get(m.name, last_hour_rows[0])
    bid_rate = float(row.get("Demand Bid Rate %", 0) or 0)
    revenue = float(row.get("Revenue", 0) or 0)
    cost = float(row.get("Cost", 0) or 0)
    demand_id = row.get("Demand ID", "")

    next_margin = opt.suggest_next_margin(
        margin=m.margin_pct,
        impressions=m.impressions,
        revenue=revenue,
        cost=cost,
        bid_rate=bid_rate,
        responses=m.responses,
    )

    recommendations.append({
        "demand_id": demand_id,
        "demand_name": m.name,
        "current_margin_pct": m.margin_pct,
        "recommended_margin_pct": round(next_margin, 2),
        "profit_per_1k": round(m.profit_per_1k_impr, 4),
        "srpm": round(m.srpm, 4),
        "impressions": int(m.impressions),
    })
    print(f"  {m.name}: current={m.margin_pct:.2f}% -> recommended={next_margin:.2f}%")

# ── STEP 5: Write recommendations CSV locally ──
print("\n=== STEP 5: Writing recommendations CSV ===")
reco_csv = Path(__file__).parent / "margin_recommendations_s3.csv"
fieldnames = ["demand_id", "demand_name", "current_margin_pct", "recommended_margin_pct", "profit_per_1k", "srpm", "impressions"]
with open(reco_csv, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(recommendations)

print(f"  Saved locally: {reco_csv}")
print("  Contents:")
for line in reco_csv.read_text(encoding="utf-8").splitlines():
    print(f"    {line}")

# ── STEP 6: Upload recommendations back to S3 ──
print("\n=== STEP 6: Uploading results to S3 ===")
reco_key = prefix + "margin_recommendations_s3.csv"
client.put_object(Bucket=bucket, Key=reco_key, Body=reco_csv.read_bytes(), ContentType="text/csv")
print(f"  Uploaded to s3://{bucket}/{reco_key}")

analysis_key = prefix + "analysis_results.json"
analysis = {
    "source_file": data_key,
    "hour_used": last_hour,
    "winner": winner.name,
    "recommended": recommended.name if recommended else (control.name if control else "N/A"),
    "arms": [
        {
            "name": m.name,
            "margin_pct": m.margin_pct,
            "impressions": int(m.impressions),
            "profit": round(m.profit, 4),
            "profit_per_1k": round(m.profit_per_1k_impr, 4),
            "srpm": round(m.srpm, 4),
        }
        for m in ms_sorted
    ],
    "recommendations": recommendations,
}
client.put_object(Bucket=bucket, Key=analysis_key, Body=json.dumps(analysis, indent=2).encode("utf-8"), ContentType="application/json")
print(f"  Uploaded analysis to s3://{bucket}/{analysis_key}")

print("\n ALL DONE!")
