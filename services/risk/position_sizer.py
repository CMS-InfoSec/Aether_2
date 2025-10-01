"""Position sizing utilities for the risk service."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Protocol

from services.common.adapters import RedisFeastAdapter, TimescaleAdapter

logger = logging.getLogger(__name__)


DEFAULT_TARGET_VOLATILITY = 0.25
DEFAULT_MIN_VOLATILITY = 1e-4
DEFAULT_FALLBACK_VOLATILITY = 0.35


class SupportsRiskBudget(Protocol):
    """Protocol representing the subset of risk limit attributes we require."""

    max_nav_pct_per_trade: float
    notional_cap: float


@dataclass(slots=True)
class PositionSizeResult:
    """Result returned by :class:`PositionSizer.suggest_max_position`."""

    account_id: str
    symbol: str
    volatility: float
    nav: float
    risk_budget: float
    max_position: float
    timestamp: datetime


class PositionSizer:
    """Calculate adaptive position size limits for an account."""

    def __init__(
        self,
        account_id: str,
        *,
        limits: SupportsRiskBudget,
        timescale: TimescaleAdapter | None = None,
        feature_store: RedisFeastAdapter | None = None,
        target_volatility: float = DEFAULT_TARGET_VOLATILITY,
        fallback_volatility: float = DEFAULT_FALLBACK_VOLATILITY,
        min_volatility: float = DEFAULT_MIN_VOLATILITY,
        log_callback: Callable[[str, str, float, float, datetime], None] | None = None,
    ) -> None:
        self.account_id = account_id
        self._limits = limits
        self._timescale = timescale or TimescaleAdapter(account_id=account_id)
        self._feature_store = feature_store or RedisFeastAdapter(account_id=account_id)
        self._target_volatility = max(float(target_volatility), 0.0)
        self._fallback_volatility = max(float(fallback_volatility), DEFAULT_MIN_VOLATILITY)
        self._min_volatility = max(float(min_volatility), DEFAULT_MIN_VOLATILITY)
        self._log_callback = log_callback

    def suggest_max_position(
        self,
        symbol: str,
        *,
        nav: float | None = None,
        risk_budget: float | None = None,
    ) -> PositionSizeResult:
        """Return the suggested maximum notional for ``symbol``."""

        resolved_nav = self._resolve_nav(nav)
        base_budget = self._resolve_budget(resolved_nav, risk_budget)
        volatility = self._instrument_volatility(symbol)
        adjusted = self._apply_volatility_adjustment(base_budget, volatility)
        timestamp = datetime.now(timezone.utc)

        if self._log_callback is not None:
            try:
                self._log_callback(self.account_id, symbol, volatility, adjusted, timestamp)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(
                    "Failed to record position size adjustment for account=%s symbol=%s",
                    self.account_id,
                    symbol,
                )

        return PositionSizeResult(
            account_id=self.account_id,
            symbol=symbol,
            volatility=volatility,
            nav=resolved_nav,
            risk_budget=base_budget,
            max_position=adjusted,
            timestamp=timestamp,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _resolve_nav(self, nav: float | None) -> float:
        if nav is not None and nav > 0:
            return float(nav)
        config = self._timescale.load_risk_config()
        raw_nav = config.get("nav") if isinstance(config, dict) else None
        try:
            value = float(raw_nav) if raw_nav is not None else 0.0
        except (TypeError, ValueError):  # pragma: no cover - defensive casting
            value = 0.0
        return max(value, 0.0)

    def _resolve_budget(self, nav: float, risk_budget: float | None) -> float:
        limits = self._limits
        nav_cap = max(float(getattr(limits, "max_nav_pct_per_trade", 0.0)), 0.0)
        base_budget = max(nav * nav_cap, 0.0)
        if base_budget <= 0.0 and risk_budget is not None:
            base_budget = max(float(risk_budget), 0.0)
        elif risk_budget is not None:
            base_budget = min(base_budget, max(float(risk_budget), 0.0))

        notional_cap = max(float(getattr(limits, "notional_cap", 0.0)), 0.0)
        if notional_cap > 0.0:
            if base_budget <= 0.0:
                base_budget = notional_cap
            else:
                base_budget = min(base_budget, notional_cap)

        return max(base_budget, 0.0)

    def _instrument_volatility(self, symbol: str) -> float:
        payload = self._feature_store.fetch_online_features(symbol)
        volatility: Optional[float] = None
        if isinstance(payload, dict):
            state = payload.get("state")
            if isinstance(state, dict):
                raw = state.get("volatility")
                try:
                    if raw is not None:
                        volatility = float(raw)
                except (TypeError, ValueError):  # pragma: no cover - defensive casting
                    volatility = None
        if volatility is None or volatility <= 0.0:
            volatility = self._fallback_volatility
        return float(max(volatility, self._min_volatility))

    def _apply_volatility_adjustment(self, budget: float, volatility: float) -> float:
        if budget <= 0.0:
            return 0.0
        vol = max(volatility, self._min_volatility)
        if self._target_volatility <= 0.0:
            return float(budget)
        ratio = self._target_volatility / vol
        scale = min(1.0, max(ratio, 0.0))
        notional_cap = max(float(getattr(self._limits, "notional_cap", 0.0)), 0.0)
        adjusted = float(budget) * scale
        if notional_cap > 0.0:
            adjusted = min(adjusted, notional_cap)
        return max(adjusted, 0.0)
