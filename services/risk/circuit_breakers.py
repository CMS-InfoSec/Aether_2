"""Adaptive circuit breakers monitoring market health for the risk engine."""

from __future__ import annotations

import logging
import math
from collections import deque
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar, Deque, Dict, Iterable, List, Mapping, MutableMapping, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from services.common.adapters import TimescaleAdapter
from services.common.schemas import RiskValidationRequest
from services.common.security import require_admin_account
from shared.audit import (
    AuditLogEntry,
    AuditLogStore,
    SensitiveActionRecorder,
    TimescaleAuditLogger,
)


LOGGER = logging.getLogger(__name__)


DEFAULT_LOOKBACK_SECONDS = 60
DEFAULT_MAX_SAMPLES = 600


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _realized_volatility(prices: Iterable[float]) -> float:
    values = [value for value in prices if value and value > 0]
    if len(values) < 2:
        return 0.0
    returns: List[float] = []
    previous = values[0]
    for current in values[1:]:
        if current <= 0 or previous <= 0:
            previous = current
            continue
        try:
            returns.append(math.log(current / previous))
        except (ValueError, ZeroDivisionError):
            LOGGER.debug("Failed to compute log return for prices %.6f -> %.6f", previous, current)
        previous = current
    if not returns:
        return 0.0
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / max(len(returns) - 1, 1)
    return math.sqrt(max(variance, 0.0))


@dataclass(frozen=True)
class SymbolThresholds:
    """Configuration thresholds for a trading symbol."""

    max_spread_bps: Optional[float] = None
    max_volatility: Optional[float] = None
    trigger_safe_mode: bool = False


@dataclass
class _QuoteWindow:
    """Rolling window of quote observations used to compute metrics."""

    lookback: timedelta
    midpoints: Deque[tuple[datetime, float]] = field(default_factory=lambda: deque(maxlen=DEFAULT_MAX_SAMPLES))
    spread_bps: float = 0.0
    volatility: float = 0.0

    def observe(
        self,
        *,
        mid_price: Optional[float],
        spread_bps: Optional[float],
        timestamp: Optional[datetime] = None,
    ) -> None:
        ts = timestamp or _utcnow()
        if spread_bps is not None:
            try:
                numeric = float(spread_bps)
            except (TypeError, ValueError):
                numeric = 0.0
            if not math.isfinite(numeric):
                numeric = 0.0
            self.spread_bps = max(0.0, numeric)
        if mid_price is not None:
            try:
                midpoint = float(mid_price)
            except (TypeError, ValueError):
                midpoint = 0.0
            if midpoint > 0:
                self.midpoints.append((ts, midpoint))
                cutoff = ts - self.lookback
                while self.midpoints and self.midpoints[0][0] < cutoff:
                    self.midpoints.popleft()
                prices = [price for _, price in self.midpoints]
                self.volatility = _realized_volatility(prices)


@dataclass(frozen=True)
class CircuitBreakerDecision:
    """Result of evaluating an intent against current circuit breaker state."""

    symbol: str
    reason: str
    spread_bps: float
    volatility: float
    triggered_at: datetime
    safe_mode_engaged: bool = False


@dataclass
class _CircuitBreakerRecord:
    symbol: str
    reason: str
    spread_bps: float
    volatility: float
    triggered_at: datetime
    safe_mode_engaged: bool = False


class CircuitBreakerConfigStore:
    """Manage per-symbol circuit breaker configuration with audit logging."""

    CONFIG_KEY: ClassVar[str] = "circuit_breakers"
    _audit_store: ClassVar[AuditLogStore] = AuditLogStore()
    _audit_logger: ClassVar[TimescaleAuditLogger] = TimescaleAuditLogger(_audit_store)

    def __init__(
        self,
        account_id: str,
        *,
        timescale: TimescaleAdapter | None = None,
        auditor: SensitiveActionRecorder | None = None,
    ) -> None:
        self.account_id = account_id
        self._timescale = timescale or TimescaleAdapter(account_id=account_id)
        self._auditor = auditor or SensitiveActionRecorder(self._audit_logger)

    def _load_bucket(self) -> tuple[Dict[str, Any], MutableMapping[str, Dict[str, object]]]:
        config = self._timescale.load_risk_config()
        bucket = config.setdefault(self.CONFIG_KEY, {})
        if not isinstance(bucket, dict):
            bucket = {}
            config[self.CONFIG_KEY] = bucket
        return config, bucket

    def thresholds(self) -> Dict[str, SymbolThresholds]:
        _, payload = self._load_bucket()
        return {symbol: self._deserialize(entry) for symbol, entry in payload.items()}

    def threshold_for(self, symbol: str) -> Optional[SymbolThresholds]:
        _, bucket = self._load_bucket()
        entry = bucket.get(_normalize_symbol(symbol))
        if entry is None:
            return None
        return self._deserialize(entry)

    def upsert(
        self,
        symbol: str,
        *,
        max_spread_bps: Optional[float],
        max_volatility: Optional[float],
        trigger_safe_mode: bool,
        actor_id: str,
    ) -> None:
        normalized = _normalize_symbol(symbol)
        config, bucket = self._load_bucket()
        before = bucket.get(normalized)
        after: Dict[str, object] = {}
        if max_spread_bps is not None:
            after["max_spread_bps"] = float(max_spread_bps)
        if max_volatility is not None:
            after["max_volatility"] = float(max_volatility)
        after["trigger_safe_mode"] = bool(trigger_safe_mode)
        bucket[normalized] = dict(after)
        self._timescale.save_risk_config(config)
        self._auditor.record(
            action="risk.circuit_breaker.update",
            actor_id=actor_id,
            before=deepcopy(before) if isinstance(before, dict) else None,
            after=deepcopy(after),
        )

    def remove(self, symbol: str, *, actor_id: str) -> None:
        normalized = _normalize_symbol(symbol)
        config, bucket = self._load_bucket()
        before = bucket.pop(normalized, None)
        if before is None:
            return
        self._timescale.save_risk_config(config)
        self._auditor.record(
            action="risk.circuit_breaker.delete",
            actor_id=actor_id,
            before=deepcopy(before) if isinstance(before, dict) else None,
            after=None,
        )

    @classmethod
    def audit_entries(cls) -> Iterable[AuditLogEntry]:
        return tuple(cls._audit_store.all())

    @classmethod
    def reset(cls) -> None:
        cls._audit_store = AuditLogStore()
        cls._audit_logger = TimescaleAuditLogger(cls._audit_store)

    def _deserialize(self, payload: Mapping[str, object]) -> SymbolThresholds:
        max_spread = payload.get("max_spread_bps")
        max_vol = payload.get("max_volatility")
        safe_mode = payload.get("trigger_safe_mode", False)
        try:
            spread_value = float(max_spread) if max_spread is not None else None
        except (TypeError, ValueError):
            spread_value = None
        try:
            vol_value = float(max_vol) if max_vol is not None else None
        except (TypeError, ValueError):
            vol_value = None
        return SymbolThresholds(
            max_spread_bps=spread_value,
            max_volatility=vol_value,
            trigger_safe_mode=bool(safe_mode),
        )


class CircuitBreakerMonitor:
    """Track market health and determine whether intents should be blocked."""

    def __init__(
        self,
        account_id: str,
        *,
        timescale: TimescaleAdapter | None = None,
        config_store: CircuitBreakerConfigStore | None = None,
        lookback_seconds: int = DEFAULT_LOOKBACK_SECONDS,
    ) -> None:
        self.account_id = account_id
        self._timescale = timescale or TimescaleAdapter(account_id=account_id)
        self._lookback = timedelta(seconds=max(int(lookback_seconds), 1))
        self._quotes: Dict[str, _QuoteWindow] = {}
        self._blocked: Dict[str, _CircuitBreakerRecord] = {}
        self._config = config_store or CircuitBreakerConfigStore(
            account_id, timescale=self._timescale
        )

    def bind_timescale(self, timescale: TimescaleAdapter) -> None:
        self._timescale = timescale
        self._config = CircuitBreakerConfigStore(
            self.account_id, timescale=timescale
        )

    def record_quote(
        self,
        symbol: str,
        *,
        bid: Optional[float] = None,
        ask: Optional[float] = None,
        mid_price: Optional[float] = None,
        spread_bps: Optional[float] = None,
        timestamp: Optional[datetime] = None,
    ) -> None:
        normalized = _normalize_symbol(symbol)
        quote = self._quotes.setdefault(
            normalized, _QuoteWindow(lookback=self._lookback)
        )
        ts = timestamp or _utcnow()
        resolved_mid: Optional[float] = None
        if bid is not None and ask is not None:
            try:
                bid_value = float(bid)
                ask_value = float(ask)
            except (TypeError, ValueError):
                bid_value = ask_value = 0.0
            if bid_value > 0 and ask_value > 0 and ask_value >= bid_value:
                resolved_mid = (ask_value + bid_value) / 2.0
                gap = max(ask_value - bid_value, 0.0)
                if resolved_mid > 0:
                    spread_bps = (gap / resolved_mid) * 10_000.0
        if resolved_mid is None:
            resolved_mid = mid_price
        quote.observe(mid_price=resolved_mid, spread_bps=spread_bps, timestamp=ts)
        self._evaluate_symbol(normalized, quote)

    def observe_request(self, request: RiskValidationRequest) -> None:
        symbol = str(request.instrument)
        book = request.intent.book_snapshot
        spread = request.spread_bps
        mid: Optional[float] = None
        if book is not None:
            mid = book.mid_price
            spread = book.spread_bps
        elif request.intent.policy_decision.request.price is not None:
            mid = request.intent.policy_decision.request.price
        self.record_quote(symbol, mid_price=mid, spread_bps=spread)

    def evaluate(self, request: RiskValidationRequest) -> Optional[CircuitBreakerDecision]:
        self.observe_request(request)
        symbol = _normalize_symbol(request.instrument)
        record = self._blocked.get(symbol)
        if record is None:
            return None
        if self._is_hedge(request):
            return None
        return CircuitBreakerDecision(
            symbol=symbol,
            reason=record.reason,
            spread_bps=record.spread_bps,
            volatility=record.volatility,
            triggered_at=record.triggered_at,
            safe_mode_engaged=record.safe_mode_engaged,
        )

    def status(self) -> List["CircuitBreakerSymbolStatus"]:
        return [
            CircuitBreakerSymbolStatus(
                symbol=record.symbol,
                reason=record.reason,
                spread_bps=record.spread_bps,
                volatility=record.volatility,
                triggered_at=record.triggered_at,
                safe_mode=record.safe_mode_engaged,
            )
            for record in sorted(self._blocked.values(), key=lambda entry: entry.symbol)
        ]

    def _evaluate_symbol(self, symbol: str, quote: _QuoteWindow) -> None:
        thresholds = self._config.threshold_for(symbol)
        if thresholds is None:
            return
        reasons: List[str] = []
        if (
            thresholds.max_spread_bps is not None
            and quote.spread_bps > float(thresholds.max_spread_bps)
        ):
            reasons.append(
                "Spread %.2f bps exceeds limit %.2f bps"
                % (quote.spread_bps, float(thresholds.max_spread_bps))
            )
        if (
            thresholds.max_volatility is not None
            and quote.volatility > float(thresholds.max_volatility)
        ):
            reasons.append(
                "Realized volatility %.6f exceeds limit %.6f"
                % (quote.volatility, float(thresholds.max_volatility))
            )
        if reasons:
            reason_text = "; ".join(reasons)
            previous = self._blocked.get(symbol)
            triggered_at = previous.triggered_at if previous else _utcnow()
            safe_mode_engaged = previous.safe_mode_engaged if previous else False
            self._blocked[symbol] = _CircuitBreakerRecord(
                symbol=symbol,
                reason=reason_text,
                spread_bps=quote.spread_bps,
                volatility=quote.volatility,
                triggered_at=triggered_at,
                safe_mode_engaged=safe_mode_engaged or thresholds.trigger_safe_mode,
            )
            if previous is None:
                self._timescale.record_event(
                    "circuit_breaker.engaged",
                    {
                        "symbol": symbol,
                        "reason": reason_text,
                        "spread_bps": quote.spread_bps,
                        "volatility": quote.volatility,
                    },
                )
                if thresholds.trigger_safe_mode and not safe_mode_engaged:
                    try:
                        self._timescale.set_safe_mode(
                            engaged=True,
                            reason=f"Circuit breaker engaged for {symbol}",
                            actor="circuit-breaker",
                        )
                    except Exception:  # pragma: no cover - defensive fallback
                        LOGGER.exception(
                            "Failed to trigger safe mode for account %s symbol %s",
                            self.account_id,
                            symbol,
                        )
        elif symbol in self._blocked:
            record = self._blocked.pop(symbol)
            self._timescale.record_event(
                "circuit_breaker.cleared",
                {
                    "symbol": symbol,
                    "reason": record.reason,
                    "spread_bps": record.spread_bps,
                    "volatility": record.volatility,
                },
            )

    def _is_hedge(self, request: RiskValidationRequest) -> bool:
        decision_request = request.intent.policy_decision.request
        metadata = request.portfolio_state.metadata or {}
        bool_flags = (
            metadata.get("hedge"),
            metadata.get("is_hedge"),
            metadata.get("hedging"),
            decision_request.reduce_only,
        )
        if any(isinstance(flag, bool) and flag for flag in bool_flags):
            return True
        textual_candidates = [
            metadata.get(key)
            for key in ("intent_type", "type", "category", "strategy", "purpose")
        ]
        for candidate in textual_candidates:
            if isinstance(candidate, str) and candidate.lower() in {"hedge", "hedging"}:
                return True
        exposures = request.portfolio_state.instrument_exposure or {}
        current = exposures.get(request.instrument, 0.0)
        try:
            current_value = float(current)
            projected = float(request.net_exposure)
        except (TypeError, ValueError):
            return False
        return abs(projected) <= abs(current_value)


class CircuitBreakerSymbolStatus(BaseModel):
    symbol: str
    reason: str
    spread_bps: float = Field(..., ge=0.0)
    volatility: float = Field(..., ge=0.0)
    triggered_at: datetime
    safe_mode: bool = False


class CircuitBreakerStatusResponse(BaseModel):
    account_id: str
    blocked: List[CircuitBreakerSymbolStatus]


router = APIRouter()


@router.get("/risk/circuit/status", response_model=CircuitBreakerStatusResponse)
def circuit_breaker_status(
    account_id: str = Query(..., description="Trading account identifier"),
    caller: str = Depends(require_admin_account),
) -> CircuitBreakerStatusResponse:
    if caller != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and query parameter.",
        )
    monitor = get_circuit_breaker(account_id)
    return CircuitBreakerStatusResponse(account_id=account_id, blocked=monitor.status())


_MONITORS: Dict[str, CircuitBreakerMonitor] = {}


def get_circuit_breaker(
    account_id: str,
    *,
    timescale: TimescaleAdapter | None = None,
) -> CircuitBreakerMonitor:
    monitor = _MONITORS.get(account_id)
    if monitor is None:
        monitor = CircuitBreakerMonitor(account_id, timescale=timescale)
        _MONITORS[account_id] = monitor
    elif timescale is not None:
        monitor.bind_timescale(timescale)
    return monitor


def reset_circuit_breakers(account_id: str | None = None) -> None:
    if account_id is None:
        _MONITORS.clear()
    else:
        _MONITORS.pop(account_id, None)


__all__ = [
    "CircuitBreakerConfigStore",
    "CircuitBreakerDecision",
    "CircuitBreakerMonitor",
    "CircuitBreakerStatusResponse",
    "CircuitBreakerSymbolStatus",
    "get_circuit_breaker",
    "reset_circuit_breakers",
    "router",
]

