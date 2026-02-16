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

# ── STEP 4: Generate margin recommendations using cross-arm analysis ──
print("\n=== STEP 4: Generating margin recommendations (cross-arm analysis) ===")

# Build a lookup: demand_name -> row from last_hour_rows
row_by_name = {}
for r in last_hour_rows:
    row_by_name[r["Demand Name"].strip()] = r

# Current margins = actual margins from the data CSV (what's running on the endpoints)
print("\n  Current margins running on endpoints:")
for m in ms_sorted:
    print(f"    {m.name}: {m.margin_pct}%")

# Sort arms by margin% ascending to analyze the trend
arms_by_margin = sorted(ms_sorted, key=lambda x: x.margin_pct)

# Compute profit-per-margin-point between adjacent arms
print("\n  Cross-arm profit trend:")
deltas = []
for i in range(1, len(arms_by_margin)):
    prev_arm = arms_by_margin[i - 1]
    curr_arm = arms_by_margin[i]
    margin_gap = curr_arm.margin_pct - prev_arm.margin_pct
    profit_gap = curr_arm.profit_per_1k_impr - prev_arm.profit_per_1k_impr
    profit_per_point = profit_gap / margin_gap if margin_gap > 0 else 0
    deltas.append(profit_per_point)
    print(f"    {prev_arm.name} ({prev_arm.margin_pct:.1f}%) -> {curr_arm.name} ({curr_arm.margin_pct:.1f}%): "
          f"margin +{margin_gap:.2f}pp, profit/1k +${profit_gap:.4f} "
          f"(${profit_per_point:.4f}/pp)")

# Determine trend: is profit still growing with margin?
profit_still_growing = all(d > 0 for d in deltas) if deltas else False
avg_margin_gap = (arms_by_margin[-1].margin_pct - arms_by_margin[0].margin_pct) / max(len(arms_by_margin) - 1, 1)

# sRPM guardrail: check if highest-margin arm's sRPM is still acceptable vs control
srpm_guardrail_pct = 90.0  # sRPM must stay above 90% of control
control_srpm = control.srpm if control else arms_by_margin[0].srpm
best_arm = arms_by_margin[-1]  # highest margin arm
srpm_ratio = (best_arm.srpm / control_srpm * 100) if control_srpm > 0 else 100.0

print(f"\n  Winner: {recommended.name if recommended else winner.name}")
print(f"  Profit trend: {'still growing ↑' if profit_still_growing else 'plateauing/declining'}")
print(f"  sRPM ratio (best vs control): {srpm_ratio:.1f}% (guardrail: >={srpm_guardrail_pct}%)")
print(f"  Avg margin gap between arms: {avg_margin_gap:.2f}pp")

print(f"\n  Next round strategy:")

# Build next-round bracket AROUND the winner:
# - LowMar:  below the winner (confirm floor)
# - MidMar:  at the winner (confirm it holds)
# - HighMar: above the winner (explore higher)
low_margin = round(best_arm.margin_pct - avg_margin_gap, 0)
mid_margin = round(best_arm.margin_pct, 0)
high_margin = round(best_arm.margin_pct + avg_margin_gap, 0)
print(f"  Bracketing around winner ({best_arm.margin_pct:.1f}%): below / at / above")

bracket = [low_margin, mid_margin, high_margin]
print(f"  Next round bracket: LowMar={low_margin}%, MidMar={mid_margin}%, HighMar={high_margin}%")

recommendations = []
for i, m in enumerate(arms_by_margin):  # sorted by margin ascending
    row = row_by_name.get(m.name, last_hour_rows[0])
    demand_id = row.get("Demand ID", "")
    next_margin = bracket[i]
    recommendations.append({
        "demand_id": demand_id,
        "demand_name": m.name,
        "recommended_margin_pct": next_margin,
    })
    print(f"  {m.name}: current={m.margin_pct:.2f}% -> recommended={next_margin}%")

# ── STEP 5: Write recommendations CSV locally ──
print("\n=== STEP 5: Writing recommendations CSV ===")
reco_csv = Path(__file__).parent / "margin_recommendations_s3.csv"
fieldnames = ["demand_id", "demand_name", "recommended_margin_pct"]
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
