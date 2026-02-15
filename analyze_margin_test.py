import argparse
import csv
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from metrics import compute_derived_metrics


NUMERIC_COLS = [
    "Cost",
    "Revenue",
    "Profit $",
    "Margin %",
    "Demand Bid Rate %",
    "Supply Responses",
    "Supply Impressions",
    "Demand Win Rate %",
    "sRPM $",
    "Supply Bidfloor",
    "Our Bidfloor",
    "Demand eCPM",
]


@dataclass(frozen=True)
class RowMetrics:
    name: str
    impressions: float
    responses: float
    margin_pct: float
    win_rate_pct: float
    profit: float
    profit_per_1k_impr: float
    revenue_per_1k_impr: float
    cost_per_1k_impr: float
    impression_rate: float
    our_bidfloor: float
    supply_bidfloor: float
    demand_ecpm: float
    srpm: float


def _to_float(v: Any) -> float:
    if v is None:
        return 0.0
    s = str(v).strip()
    if s == "":
        return 0.0
    return float(s)


def load_rows(csv_path: str) -> List[Dict[str, Any]]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise ValueError("CSV contains no data rows.")
    for r in rows:
        for c in NUMERIC_COLS:
            if c in r:
                r[c] = _to_float(r[c])
    return rows


def compute_metrics(rows: List[Dict[str, Any]]) -> List[RowMetrics]:
    out: List[RowMetrics] = []
    for r in rows:
        impr = float(r.get("Supply Impressions", 0.0) or 0.0)
        resp = float(r.get("Supply Responses", 0.0) or 0.0)
        cost = float(r.get("Cost", 0.0) or 0.0)
        rev = float(r.get("Revenue", 0.0) or 0.0)

        derived = compute_derived_metrics(impr, rev, cost, resp)

        out.append(
            RowMetrics(
                name=str(r.get("Demand Name", "")).strip() or "<unnamed>",
                impressions=impr,
                responses=resp,
                margin_pct=float(r.get("Margin %", 0.0) or 0.0),
                win_rate_pct=float(r.get("Demand Win Rate %", 0.0) or 0.0),
                profit=derived["profit"],
                profit_per_1k_impr=derived["profit_per_1k"],
                revenue_per_1k_impr=derived["revenue_per_1k"],
                cost_per_1k_impr=derived["cost_per_1k"],
                impression_rate=derived["impression_rate"],
                our_bidfloor=float(r.get("Our Bidfloor", 0.0) or 0.0),
                supply_bidfloor=float(r.get("Supply Bidfloor", 0.0) or 0.0),
                demand_ecpm=float(r.get("Demand eCPM", 0.0) or 0.0),
                srpm=derived["srpm"],
            )
        )
    return out


def pick_winner(ms: List[RowMetrics]) -> RowMetrics:
    """Highest profit/1k among all arms."""
    return max(ms, key=lambda x: x.profit_per_1k_impr)


def pick_recommended_winner(
    ms: List[RowMetrics],
    control: Optional[RowMetrics],
    min_srpm_pct_of_control: float = 90.0,
) -> Optional[RowMetrics]:
    """
    Recommended winner: highest profit/1k among arms that pass sRPM guardrail.
    sRPM = revenue per 1k impressions; high sRPM means supply performance is not hurt
    even if total revenue/impressions drop.
    Returns None if no arm passes; then recommend keeping control.
    """
    if not control or control.srpm <= 0:
        return pick_winner(ms)
    threshold = control.srpm * (min_srpm_pct_of_control / 100.0)
    qualified = [m for m in ms if m.srpm >= threshold]
    if not qualified:
        return None
    return max(qualified, key=lambda x: x.profit_per_1k_impr)


def find_control(ms: List[RowMetrics], control_contains: Optional[str]) -> Optional[RowMetrics]:
    if not control_contains:
        return None
    control_contains_l = control_contains.lower()
    for m in ms:
        if control_contains_l in m.name.lower():
            return m
    return None


def assess_enough_data(
    ms: List[RowMetrics],
    min_impressions_per_arm: int,
    min_profit_per_arm: float,
) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    ok = True
    for m in ms:
        if m.impressions < min_impressions_per_arm:
            ok = False
            reasons.append(
                f"'{m.name}': impressions {int(m.impressions)} < min {min_impressions_per_arm}"
            )
        profit_dollars = m.profit
        if profit_dollars < min_profit_per_arm:
            ok = False
            reasons.append(
                f"'{m.name}': profit ${profit_dollars:.4f} < min ${min_profit_per_arm:.4f}"
            )
    return ok, reasons


def assess_guardrails_vs_control(
    ms: List[RowMetrics],
    control: RowMetrics,
    max_impr_drop_pct: float,
    max_srpm_drop_pct: float,
) -> List[str]:
    warnings: List[str] = []
    for m in ms:
        if m.name == control.name:
            continue
        if control.impressions > 0:
            impr_drop = (control.impressions - m.impressions) / control.impressions * 100.0
            if impr_drop > max_impr_drop_pct:
                warnings.append(
                    f"Guardrail: '{m.name}' impressions drop {impr_drop:.1f}% vs control (>{max_impr_drop_pct:.1f}%)"
                )
        if control.srpm > 0:
            srpm_drop = (control.srpm - m.srpm) / control.srpm * 100.0
            if srpm_drop > max_srpm_drop_pct:
                warnings.append(
                    f"Guardrail: '{m.name}' sRPM drop {srpm_drop:.1f}% vs control (>{max_srpm_drop_pct:.1f}%)"
                )
    return warnings


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Analyze 3-arm margin/bidfloor A/B test CSV and print derived KPIs + basic 'enough data' checks."
    )
    ap.add_argument("--csv", required=True, help="Path to analytics CSV export")
    ap.add_argument(
        "--control-contains",
        default=None,
        help="Substring to identify control arm by Demand Name (e.g. 'LowMar'). Optional.",
    )
    ap.add_argument("--min-impressions", type=int, default=50000, help="Minimum impressions per arm")
    ap.add_argument("--min-profit", type=float, default=50.0, help="Minimum profit dollars per arm")
    ap.add_argument(
        "--max-impr-drop-pct",
        type=float,
        default=10.0,
        help="Guardrail: max allowed impressions drop vs control (%)",
    )
    ap.add_argument(
        "--max-srpm-drop-pct",
        type=float,
        default=10.0,
        help="Guardrail: max allowed sRPM drop vs control (%)",
    )
    ap.add_argument(
        "--min-srpm-pct-of-control",
        type=float,
        default=90.0,
        help="Recommended winner must have sRPM >= this %% of control (supply performance)",
    )
    args = ap.parse_args()

    rows = load_rows(args.csv)
    ms = compute_metrics(rows)
    ms_sorted = sorted(ms, key=lambda x: x.profit_per_1k_impr, reverse=True)
    winner = pick_winner(ms_sorted)
    control = find_control(ms, args.control_contains)
    recommended = pick_recommended_winner(ms, control, args.min_srpm_pct_of_control) if control else winner

    print("Derived KPIs (sorted by profit/1k impressions):")
    for m in ms_sorted:
        print(
            f"- {m.name}\n"
            f"  impressions={int(m.impressions)} responses={int(m.responses)} impression_rate={m.impression_rate:.4%}\n"
            f"  margin%={m.margin_pct:.2f} win%={m.win_rate_pct:.2f}\n"
            f"  profit={m.profit:.4f} profit/1k={m.profit_per_1k_impr:.4f} rev/1k={m.revenue_per_1k_impr:.4f} cost/1k={m.cost_per_1k_impr:.4f}\n"
            f"  our_bidfloor={m.our_bidfloor:.2f} supply_bidfloor={m.supply_bidfloor:.2f} demand_eCPM={m.demand_ecpm:.2f} sRPM={m.srpm:.4f}"
        )

    print("\nWinner by profit/1k impressions:")
    print(f"- {winner.name} (profit/1k={winner.profit_per_1k_impr:.4f}, profit={winner.profit:.4f}, margin%={winner.margin_pct:.2f})")

    print("\nRecommendation (profit + sRPM guardrail):")
    if control:
        if recommended:
            srpm_vs_control = (recommended.srpm / control.srpm * 100.0) if control.srpm > 0 else 100.0
            print(
                f"- RECOMMEND: {recommended.name}\n"
                f"  Reason: highest profit among arms with sRPM at/above {args.min_srpm_pct_of_control:.0f}% of control.\n"
                f"  sRPM={recommended.srpm:.4f} ({srpm_vs_control:.1f}% of control) - supply/revenue performance preserved."
            )
        else:
            print(
                f"- KEEP CONTROL: {control.name}\n"
                f"  Reason: no arm has sRPM >= {args.min_srpm_pct_of_control:.0f}% of control. "
                f"Winner ({winner.name}) would hurt supply performance."
            )
    else:
        print(f"- No control specified; raw winner = {winner.name}")

    enough, reasons = assess_enough_data(ms, args.min_impressions, args.min_profit)
    print("\nEnough data check:")
    if enough:
        print("- PASS: meets minimum per-arm thresholds")
    else:
        print("- FAIL: not enough data yet")
        for r in reasons:
            print(f"  - {r}")

    if control:
        warnings = assess_guardrails_vs_control(
            ms, control, args.max_impr_drop_pct, args.max_srpm_drop_pct
        )
        print("\nGuardrails vs control:")
        if warnings:
            for w in warnings:
                print(f"- {w}")
        else:
            print("- OK")
    else:
        print("\nGuardrails vs control: skipped (no control arm provided)")

    print(
        "\nNote: This script cannot compute statistical significance from fully-aggregated rows.\n"
        "For real stopping rules, export event-level or time-bucketed data (e.g., per hour/day) per arm."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

