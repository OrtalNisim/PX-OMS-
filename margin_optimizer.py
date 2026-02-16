"""
Margin Optimizer: maximizes total Profit while respecting guardrails
on sRPM and BidRate (-10% vs baseline). Uses safe hill-climb with rollback.
"""
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import List, Optional

from metrics import WindowMetrics, compute_window_metrics


@dataclass
class OptimizerState:
    baseline_margin: float
    last_safe_margin: float
    current_margin: float
    step: float
    baseline_srpm: Optional[float] = None
    baseline_bid_rate: Optional[float] = None
    baseline_profit: Optional[float] = None
    history: List[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "OptimizerState":
        return cls(
            baseline_margin=float(d.get("baseline_margin", 35)),
            last_safe_margin=float(d.get("last_safe_margin", 35)),
            current_margin=float(d.get("current_margin", 35)),
            step=float(d.get("step", 1.0)),
            baseline_srpm=float(d["baseline_srpm"]) if d.get("baseline_srpm") is not None else None,
            baseline_bid_rate=float(d["baseline_bid_rate"]) if d.get("baseline_bid_rate") is not None else None,
            baseline_profit=float(d["baseline_profit"]) if d.get("baseline_profit") is not None else None,
            history=d.get("history", [])[-100:],  # keep last 100
        )


class MarginOptimizer:
    """
    Maximizes total Profit with guardrails (so we don't hurt supply/revenue performance):
    - sRPM >= 0.9 * baseline_srpm  (primary: revenue per 1k impressions - supply performance)
    - bid_rate >= 0.9 * baseline_bid_rate
    High sRPM = good revenue per impression even if total revenue/impressions drop.
    """

    def __init__(
        self,
        baseline_margin: float = 35.0,
        step: float = 1.0,
        min_step: float = 0.25,
        min_impressions_per_decision: int = 0,
        min_profit_per_decision: float = 0.0,
        guardrail_drop_pct: float = 10.0,
        min_profit_improvement_pct: float = 2.0,
        state_path: Optional[Path] = None,
    ):
        self.baseline_margin = baseline_margin
        self.step = step
        self.min_step = min_step
        self.min_impressions = min_impressions_per_decision
        self.min_profit = min_profit_per_decision
        self.guardrail_drop_pct = guardrail_drop_pct
        self.min_profit_improvement_pct = min_profit_improvement_pct
        self.state_path = state_path or Path("optimizer_state.json")

        self._state = OptimizerState(
            baseline_margin=baseline_margin,
            last_safe_margin=baseline_margin,
            current_margin=baseline_margin,
            step=step,
        )
        self._load_state()

    def _load_state(self) -> None:
        if self.state_path.exists():
            try:
                with open(self.state_path, encoding="utf-8") as f:
                    self._state = OptimizerState.from_dict(json.load(f))
            except (json.JSONDecodeError, KeyError):
                pass
        else:
            # Try S3 if local file missing (e.g. first run on new machine)
            try:
                from s3_storage import load_state
                s3_state = load_state()
                if s3_state:
                    self._state = OptimizerState.from_dict(s3_state)
            except ImportError:
                pass

    def _save_state(self) -> None:
        state_dict = self._state.to_dict()
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(state_dict, f, indent=2)
        # Sync to S3 if configured
        try:
            from s3_storage import save_state
            save_state(state_dict)
        except ImportError:
            pass

    def update(
        self,
        margin: float,
        impressions: float,
        revenue: float,
        cost: float,
        bid_rate: float,
        responses: float = 0.0,
    ) -> WindowMetrics:
        """Ingest per-window metrics for the given margin arm."""
        window = {
            "margin": margin,
            "impressions": impressions,
            "revenue": revenue,
            "cost": cost,
            "bid_rate": bid_rate,
            "responses": responses,
        }
        wm = compute_window_metrics(window)
        self._state.history.append({
            "margin": margin,
            "impressions": impressions,
            "revenue": revenue,
            "cost": cost,
            "bid_rate": bid_rate,
            "profit": wm.profit,
            "profit_per_1k": wm.profit_per_1k,
            "revenue_per_1k": wm.revenue_per_1k,
            "cost_per_1k": wm.cost_per_1k,
            "srpm": wm.srpm,
        })
        self._state.history = self._state.history[-100:]
        self._save_state()
        return wm

    def suggest_next_margin(
        self,
        margin: float,
        impressions: float,
        revenue: float,
        cost: float,
        bid_rate: float,
        responses: float = 0.0,
    ) -> float:
        """
        Process latest window and return the margin to run next.
        Implements safe hill-climb: increase margin if guardrails pass and profit improves,
        else rollback and shrink step.
        """
        wm = self.update(margin, impressions, revenue, cost, bid_rate, responses)

        # Initialize baseline from first window and propose first exploration step
        if self._state.baseline_srpm is None:
            self._state.baseline_srpm = wm.srpm
            self._state.baseline_bid_rate = wm.bid_rate
            self._state.baseline_profit = wm.profit
            self._state.last_safe_margin = margin
            self._state.current_margin = margin + self._state.step
            self._save_state()
            return self._state.current_margin

        threshold = 1.0 - (self.guardrail_drop_pct / 100.0)  # e.g. 0.9 for 10%

        # Guardrails: sRPM and bid_rate must not drop more than X% vs baseline
        srpm_ok = wm.srpm >= threshold * (self._state.baseline_srpm or 0)
        bid_rate_ok = wm.bid_rate >= threshold * (self._state.baseline_bid_rate or 0)

        if not (srpm_ok and bid_rate_ok):
            # Rollback: this margin fails guardrails
            self._state.current_margin = self._state.last_safe_margin
            self._state.step = max(self._state.step / 2, self.min_step)
            self._save_state()
            return self._state.current_margin

        # Guardrails pass. Check if total profit improved enough
        base_profit = self._state.baseline_profit or 0
        if base_profit > 0:
            improvement = (wm.profit - base_profit) / base_profit * 100.0
        else:
            improvement = 100.0 if wm.profit > 0 else 0.0

        if improvement >= self.min_profit_improvement_pct:
            # Accept: this margin is better, try going higher
            self._state.last_safe_margin = margin
            self._state.baseline_srpm = wm.srpm
            self._state.baseline_bid_rate = wm.bid_rate
            self._state.baseline_profit = wm.profit
            self._state.current_margin = margin + self._state.step
            self._save_state()
            return self._state.current_margin
        else:
            # Profit didn't improve enough; stay at last safe, optionally shrink step
            self._state.current_margin = self._state.last_safe_margin
            self._state.step = max(self._state.step / 2, self.min_step)
            self._save_state()
            return self._state.current_margin
