"""
Single source of truth for derived metrics (profit, profit_per_1k, revenue_per_1k, cost_per_1k, srpm, etc.).
Used by both CSV analysis and the Margin Optimizer.
"""
from dataclasses import dataclass
from typing import Any, Dict


def compute_derived_metrics(
    impressions: float,
    revenue: float,
    cost: float,
    responses: float = 0.0,
) -> Dict[str, float]:
    """
    Core formula for all derived metrics. Avoids division by zero.

    Args:
        impressions: Supply Impressions
        revenue: Revenue in $
        cost: Cost in $
        responses: Supply Responses (optional, for impression_rate)

    Returns:
        Dict with profit, profit_per_1k, revenue_per_1k, cost_per_1k, srpm, impression_rate
    """
    denom_impr = impressions if impressions > 0 else 1.0
    denom_resp = responses if responses > 0 else 1.0

    profit = revenue - cost
    profit_per_1k = (profit / denom_impr) * 1000.0
    revenue_per_1k = (revenue / denom_impr) * 1000.0
    cost_per_1k = (cost / denom_impr) * 1000.0
    srpm = (revenue / denom_impr) * 1000.0
    impression_rate = (impressions / denom_resp) if denom_resp else 0.0

    return {
        "profit": profit,
        "profit_per_1k": profit_per_1k,
        "revenue_per_1k": revenue_per_1k,
        "cost_per_1k": cost_per_1k,
        "srpm": srpm,
        "impression_rate": impression_rate,
    }


@dataclass
class WindowMetrics:
    """Per-window metrics for the Margin Optimizer."""

    profit: float
    profit_per_1k: float
    revenue_per_1k: float
    cost_per_1k: float
    srpm: float
    impressions: float
    responses: float
    bid_rate: float
    margin: float
    impression_rate: float = 0.0


def compute_window_metrics(window: Dict[str, Any]) -> WindowMetrics:
    """
    Convert a per-window dict (from API or mock) into WindowMetrics.
    Uses compute_derived_metrics for consistency.

    Expected window keys: impressions, revenue, cost, bid_rate, margin,
    and optionally responses.
    """
    impressions = float(window.get("impressions", 0) or 0)
    revenue = float(window.get("revenue", 0) or 0)
    cost = float(window.get("cost", 0) or 0)
    bid_rate = float(window.get("bid_rate", 0) or 0)
    margin = float(window.get("margin", 0) or 0)
    responses = float(window.get("responses", 0) or 0)

    derived = compute_derived_metrics(impressions, revenue, cost, responses)

    return WindowMetrics(
        profit=derived["profit"],
        profit_per_1k=derived["profit_per_1k"],
        revenue_per_1k=derived["revenue_per_1k"],
        cost_per_1k=derived["cost_per_1k"],
        srpm=derived["srpm"],
        impressions=impressions,
        responses=responses,
        bid_rate=bid_rate,
        margin=margin,
        impression_rate=derived["impression_rate"],
    )
