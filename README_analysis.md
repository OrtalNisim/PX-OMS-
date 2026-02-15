# Margin / Profitability A-B test analysis

## Current margin config (Option A - next test round)

| Endpoint | New margin | Replaces |
|----------|------------|----------|
| Endpoint 1 | **42%** | LowMar |
| Endpoint 2 | **45%** | MidMar |
| Endpoint 3 | **48%** | HighMar |

Config file: `margin_config_next_round.json`

## Quick run (CSV analysis)

PowerShell:

```powershell
python .\analyze_margin_test.py --csv ".\Margin Data 14.02_analytics_report (2).csv" --control-contains "LowMar"
```

## Margin Optimizer (hourly)

```powershell
python run_optimizer.py
```

Run via cron every hour. Uses mock data by default.

### S3 storage (optional)

Set env vars to persist state and run logs to S3:

- `S3_BUCKET` - your bucket name (set when you share it)
- `S3_PREFIX` - optional, default `margin-optimizer/`

State: `{prefix}optimizer_state.json`, logs: `{prefix}runs/{timestamp}.json`. Requires `pip install boto3`.

## What "enough data" means here

The CSV you exported is **fully aggregated** per arm (one row per endpoint/arm). With only those aggregates, we can do:

- Derived KPIs (profit, profit per 1k, revenue per 1k, cost per 1k, impression rate).
- Simple "minimum volume" thresholds (impressions/profit per arm).
- Guardrails vs control (e.g., sRPM drop). **Recommendation**: highest profit among arms with sRPM >= 90% of control (sRPM = supply/revenue performance).

But we **cannot** compute proper statistical significance or a sequential stopping rule without variance.

For that, export at least one of:

- **Event-level** rows (one row per auction/response/impression), or
- **Time-bucketed** rows (per hour/day) for each arm, so we can bootstrap / run a time-series t-test and stop safely.
