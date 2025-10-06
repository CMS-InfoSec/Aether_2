"""Pre-trade invariant checks executed before order submission."""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from threading import Lock
from typing import TYPE_CHECKING, Deque, Dict, MutableMapping, Protocol

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from services.common.security import require_admin_account
from shared.spot import is_spot_symbol, normalize_spot_symbol

__all__ = [
    "OrderContext",
    "SanityDecision",
    "PretradeSanityChecker",
    "InMemoryMarketDataFeed",
    "InMemoryCircuitBreakerMonitor",
    "InMemoryRateLimitGuard",
    "PRETRADE_SANITY",
    "router",
]


if TYPE_CHECKING:  # pragma: no cover - typing only
    from services.common.schemas import RiskValidationRequest


# ---------------------------------------------------------------------------
# Protocols describing the external dependencies for the sanity checks
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class MarketSnapshot:
    """Minimal market data snapshot used for sanity checks."""

    symbol: str
    spread_bps: float | None
    last_update: datetime | None


class MarketDataProvider(Protocol):
    """Protocol describing the required market data interface."""

    def latest_snapshot(self, symbol: str) -> MarketSnapshot | None:
        """Return the most recent :class:`MarketSnapshot` for ``symbol``."""


class CircuitBreakerMonitor(Protocol):
    """Protocol that indicates whether a circuit breaker has tripped."""

    def is_tripped(self, account_id: str, symbol: str) -> bool:
        """Return ``True`` if trading should be halted for ``symbol``."""


class RateLimitGuard(Protocol):
    """Protocol representing the final rate limit check prior to execution."""

    def allow(self, account_id: str, symbol: str, notional: float) -> bool:
        """Return ``True`` if the order should be allowed to proceed."""


# ---------------------------------------------------------------------------
# Dataclasses describing request and decision payloads
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class OrderContext:
    """Context about the order being evaluated for sanity checks."""

    account_id: str
    symbol: str
    side: str
    notional: float
    is_hedge: bool = False


@dataclass(slots=True)
class SanityDecision:
    """Decision returned by :class:`PretradeSanityChecker.check`."""

    action: str
    reasons: list[str]
    timestamp: datetime

    @property
    def permitted(self) -> bool:
        """Return ``True`` if the order may proceed unchanged."""

        return self.action == "proceed"


@dataclass(slots=True)
class FailureRecord:
    """Internal record describing a failed invariant evaluation."""

    account_id: str
    symbol: str
    invariant: str
    action: str
    reason: str
    notional: float | None
    timestamp: datetime


@dataclass(slots=True)
class PretradeStatusSnapshot:
    """Snapshot returned for the status endpoint."""

    account_id: str
    generated_at: datetime
    counts: Dict[str, int]
    recent_failures: list[FailureRecord]


# ---------------------------------------------------------------------------
# In-memory helpers and default "null" implementations
# ---------------------------------------------------------------------------


class InMemoryMarketDataFeed(MarketDataProvider):
    """Thread-safe in-memory store for market data snapshots."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._snapshots: Dict[str, MarketSnapshot] = {}

    def update(
        self,
        symbol: str,
        *,
        spread_bps: float | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        ts = timestamp or datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        snapshot = MarketSnapshot(symbol=symbol, spread_bps=spread_bps, last_update=ts)
        with self._lock:
            self._snapshots[symbol] = snapshot

    def latest_snapshot(self, symbol: str) -> MarketSnapshot | None:
        with self._lock:
            snapshot = self._snapshots.get(symbol)
            if snapshot is None:
                return None
            return MarketSnapshot(
                symbol=snapshot.symbol,
                spread_bps=snapshot.spread_bps,
                last_update=snapshot.last_update,
            )


class InMemoryCircuitBreakerMonitor(CircuitBreakerMonitor):
    """In-memory circuit breaker state tracker for tests and defaults."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._flags: Dict[tuple[str, str], bool] = {}
        self._global_flags: Dict[str, bool] = {}

    def set_tripped(
        self,
        symbol: str,
        *,
        account_id: str | None = None,
        tripped: bool = True,
    ) -> None:
        key = (account_id or "*", symbol)
        with self._lock:
            if account_id is None:
                if tripped:
                    self._global_flags[symbol] = True
                else:
                    self._global_flags.pop(symbol, None)
            else:
                if tripped:
                    self._flags[key] = True
                else:
                    self._flags.pop(key, None)

    def is_tripped(self, account_id: str, symbol: str) -> bool:
        with self._lock:
            if self._global_flags.get(symbol, False):
                return True
            return self._flags.get((account_id, symbol), False)


class InMemoryRateLimitGuard(RateLimitGuard):
    """Simple in-memory rate limit guard supporting blocks and notional caps."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._blocked_accounts: set[str] = set()
        self._blocked_symbols: set[str] = set()
        self._notional_limits: Dict[str, float] = {}

    def block_account(self, account_id: str, *, blocked: bool = True) -> None:
        with self._lock:
            if blocked:
                self._blocked_accounts.add(account_id)
            else:
                self._blocked_accounts.discard(account_id)

    def block_symbol(self, symbol: str, *, blocked: bool = True) -> None:
        with self._lock:
            if blocked:
                self._blocked_symbols.add(symbol)
            else:
                self._blocked_symbols.discard(symbol)

    def set_notional_limit(
        self, account_id: str, limit: float | None
    ) -> None:
        with self._lock:
            if limit is None:
                self._notional_limits.pop(account_id, None)
            else:
                self._notional_limits[account_id] = max(float(limit), 0.0)

    def allow(self, account_id: str, symbol: str, notional: float) -> bool:
        with self._lock:
            if account_id in self._blocked_accounts:
                return False
            if symbol in self._blocked_symbols:
                return False
            limit = self._notional_limits.get(account_id)
            if limit is not None and notional > limit:
                return False
        return True


# ---------------------------------------------------------------------------
# Core sanity checker implementation
# ---------------------------------------------------------------------------


class PretradeSanityChecker:
    """Evaluate last-second invariants prior to order placement."""

    FEED_FRESHNESS = "feed_freshness"
    SPREAD_LIMIT = "spread_limit"
    CIRCUIT_BREAKER = "circuit_breaker"
    RATE_LIMIT = "rate_limit"
    SPOT_ELIGIBILITY = "spot_eligibility"

    def __init__(
        self,
        *,
        market_data: MarketDataProvider | None = None,
        circuit_breakers: CircuitBreakerMonitor | None = None,
        rate_limit_guard: RateLimitGuard | None = None,
        max_feed_age_seconds: float = 1.0,
        max_spread_bps: float = 35.0,
        history_size: int = 200,
    ) -> None:
        self._market_data: MarketDataProvider = market_data or InMemoryMarketDataFeed()
        self._circuit_breakers: CircuitBreakerMonitor = (
            circuit_breakers or InMemoryCircuitBreakerMonitor()
        )
        self._rate_limit_guard: RateLimitGuard = rate_limit_guard or InMemoryRateLimitGuard()
        self._max_feed_age = max(float(max_feed_age_seconds), 0.0)
        self._max_spread_bps = max(float(max_spread_bps), 0.0)
        self._history: Deque[FailureRecord] = deque(maxlen=max(10, int(history_size)))
        self._failure_counts: MutableMapping[str, Counter[str]] = {}
        self._lock = Lock()

    # ------------------------------------------------------------------
    # Dependency configuration helpers
    # ------------------------------------------------------------------
    def set_market_data_provider(self, provider: MarketDataProvider) -> None:
        """Update the market data provider used for feed freshness checks."""

        with self._lock:
            self._market_data = provider

    def set_circuit_breaker_monitor(self, monitor: CircuitBreakerMonitor) -> None:
        """Update the circuit breaker monitor."""

        with self._lock:
            self._circuit_breakers = monitor

    def set_rate_limit_guard(self, guard: RateLimitGuard) -> None:
        """Update the rate limit guard implementation."""

        with self._lock:
            self._rate_limit_guard = guard

    def update_market_snapshot(
        self,
        symbol: str,
        *,
        spread_bps: float | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        """Convenience helper to update the backing market data provider."""

        provider = self._market_data
        update = getattr(provider, "update", None)
        if callable(update):  # pragma: no branch - fast path
            if timestamp is not None and timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            update(symbol, spread_bps=spread_bps, timestamp=timestamp)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def check(
        self,
        context: OrderContext,
        *,
        observed_spread_bps: float | None = None,
        feed_timestamp: datetime | None = None,
        now: datetime | None = None,
    ) -> SanityDecision:
        """Evaluate the configured invariants for ``context``."""

        evaluation_time = now or datetime.now(timezone.utc)
        action = "proceed"
        reasons: list[str] = []

        original_symbol = str(context.symbol)
        normalized_symbol = normalize_spot_symbol(original_symbol)
        if not normalized_symbol or not is_spot_symbol(normalized_symbol):
            reason = (
                f"Instrument {original_symbol} is not eligible for spot trading. Rejecting order."
            )
            self._record_failure(
                context,
                invariant=self.SPOT_ELIGIBILITY,
                action="reject",
                reason=reason,
            )
            return SanityDecision(action="reject", reasons=[reason], timestamp=evaluation_time)

        context = replace(context, symbol=normalized_symbol)

        spread_bps = observed_spread_bps
        snapshot: MarketSnapshot | None = None
        if feed_timestamp is None or spread_bps is None:
            snapshot = self._market_data.latest_snapshot(context.symbol)
            if snapshot is not None:
                if feed_timestamp is None:
                    feed_timestamp = snapshot.last_update
                if spread_bps is None:
                    spread_bps = snapshot.spread_bps

        if feed_timestamp is not None:
            age = (evaluation_time - feed_timestamp).total_seconds()
            if age > self._max_feed_age:
                reason = (
                    f"Market data stale for {context.symbol}: last update {age:.2f}s ago "
                    f"(limit {self._max_feed_age:.2f}s). Rejecting order."
                )
                action = "reject"
                reasons.append(reason)
                self._record_failure(
                    context,
                    invariant=self.FEED_FRESHNESS,
                    action=action,
                    reason=reason,
                )

        if spread_bps is not None and spread_bps > self._max_spread_bps:
            downgrade_reason = (
                f"Spread {spread_bps:.2f} bps exceeds limit {self._max_spread_bps:.2f} bps for "
                f"{context.symbol}. Downgrade to smaller or hedge order."
            )
            action = self._combine_action(action, "downgrade")
            reasons.append(downgrade_reason)
            self._record_failure(
                context,
                invariant=self.SPREAD_LIMIT,
                action="downgrade",
                reason=downgrade_reason,
            )

        if self._circuit_breakers.is_tripped(context.account_id, context.symbol):
            breaker_reason = (
                f"Circuit breaker engaged for {context.symbol}. Rejecting order."
            )
            action = "reject"
            reasons.append(breaker_reason)
            self._record_failure(
                context,
                invariant=self.CIRCUIT_BREAKER,
                action="reject",
                reason=breaker_reason,
            )

        if not self._rate_limit_guard.allow(
            context.account_id, context.symbol, context.notional
        ):
            throttle_reason = (
                f"Rate limit guard denied notional {context.notional:,.2f} on {context.symbol}. "
                "Submit hedge order or wait for window reset."
            )
            action = self._combine_action(action, "downgrade")
            reasons.append(throttle_reason)
            self._record_failure(
                context,
                invariant=self.RATE_LIMIT,
                action="downgrade",
                reason=throttle_reason,
            )

        if reasons:
            return SanityDecision(action=action, reasons=reasons, timestamp=evaluation_time)

        return SanityDecision(action="proceed", reasons=[], timestamp=evaluation_time)

    def evaluate_validation_request(
        self, request: "RiskValidationRequest"
    ) -> SanityDecision:
        """Convenience wrapper that extracts context from a validation request."""

        policy_request = request.intent.policy_decision.request
        raw_symbol = request.instrument or policy_request.instrument
        normalized_symbol = normalize_spot_symbol(raw_symbol)
        notional = request.gross_notional
        if notional is None:
            notional = request.intent.metrics.gross_notional
        context = OrderContext(
            account_id=request.account_id,
            symbol=normalized_symbol or str(raw_symbol),
            side=str(policy_request.side),
            notional=float(notional or 0.0),
            is_hedge=self._is_hedge_request(request),
        )

        if not normalized_symbol or not is_spot_symbol(normalized_symbol):
            reason = (
                f"Instrument {raw_symbol} is not eligible for spot trading. Rejecting order."
            )
            self._record_failure(
                context,
                invariant=self.SPOT_ELIGIBILITY,
                action="reject",
                reason=reason,
            )
            return SanityDecision(action="reject", reasons=[reason], timestamp=datetime.now(timezone.utc))

        spread = request.spread_bps
        if spread is None and request.intent.book_snapshot is not None:
            spread = request.intent.book_snapshot.spread_bps

        feed_timestamp = self._infer_feed_timestamp(request)
        return self.check(
            context,
            observed_spread_bps=spread,
            feed_timestamp=feed_timestamp,
        )

    def status(self, account_id: str, *, limit: int = 20) -> PretradeStatusSnapshot:
        """Return aggregated failure information for ``account_id``."""

        limit = max(1, min(int(limit), self._history.maxlen or 200))
        with self._lock:
            counts = dict(self._failure_counts.get(account_id, Counter()))
            failures = [
                record
                for record in reversed(self._history)
                if record.account_id == account_id
            ][:limit]
        return PretradeStatusSnapshot(
            account_id=account_id,
            generated_at=datetime.now(timezone.utc),
            counts=counts,
            recent_failures=list(failures),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _record_failure(
        self,
        context: OrderContext,
        *,
        invariant: str,
        action: str,
        reason: str,
    ) -> None:
        record = FailureRecord(
            account_id=context.account_id,
            symbol=context.symbol,
            invariant=invariant,
            action=action,
            reason=reason,
            notional=context.notional,
            timestamp=datetime.now(timezone.utc),
        )
        with self._lock:
            self._history.append(record)
            counter = self._failure_counts.setdefault(context.account_id, Counter())
            counter[invariant] += 1

    @staticmethod
    def _combine_action(current: str, candidate: str) -> str:
        """Combine two actions, preferring the most severe."""

        if current == "reject" or candidate == "reject":
            return "reject"
        if current == "downgrade" or candidate == "downgrade":
            return "downgrade"
        return "proceed"

    def _infer_feed_timestamp(
        self, request: "RiskValidationRequest"
    ) -> datetime | None:
        metadata = getattr(request.portfolio_state, "metadata", None)
        if isinstance(metadata, dict):
            for key in (
                "last_book_update",
                "book_timestamp",
                "book_ts",
                "market_data_ts",
                "last_update",
            ):
                if key not in metadata:
                    continue
                timestamp = self._coerce_timestamp(metadata.get(key))
                if timestamp is not None:
                    return timestamp
        return None

    @staticmethod
    def _coerce_timestamp(value: object) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if isinstance(value, (int, float)):
            numeric = float(value)
            if numeric > 1_000_000_000_000:
                numeric /= 1000.0
            return datetime.fromtimestamp(numeric, tz=timezone.utc)
        if isinstance(value, str):
            try:
                numeric = float(value)
            except ValueError:
                cleaned = value.replace("Z", "+00:00")
                try:
                    parsed = datetime.fromisoformat(cleaned)
                except ValueError:
                    return None
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            else:
                return PretradeSanityChecker._coerce_timestamp(numeric)
        return None

    @staticmethod
    def _is_hedge_request(request: "RiskValidationRequest") -> bool:
        response = request.intent.policy_decision.response
        if response is None:
            return False
        action = response.selected_action
        if action and "hedge" in action.lower():
            return True
        return False


# ---------------------------------------------------------------------------
# Global instance and FastAPI router
# ---------------------------------------------------------------------------


PRETRADE_SANITY = PretradeSanityChecker()


class PretradeFailurePayload(BaseModel):
    symbol: str
    invariant: str
    action: str
    reason: str
    timestamp: datetime
    notional: float | None


class PretradeStatusResponse(BaseModel):
    account_id: str
    generated_at: datetime
    counts: Dict[str, int]
    recent_failures: list[PretradeFailurePayload]

    @classmethod
    def from_snapshot(cls, snapshot: PretradeStatusSnapshot) -> "PretradeStatusResponse":
        return cls(
            account_id=snapshot.account_id,
            generated_at=snapshot.generated_at,
            counts=dict(snapshot.counts),
            recent_failures=[
                PretradeFailurePayload(
                    symbol=record.symbol,
                    invariant=record.invariant,
                    action=record.action,
                    reason=record.reason,
                    timestamp=record.timestamp,
                    notional=record.notional,
                )
                for record in snapshot.recent_failures
            ],
        )


router = APIRouter()


@router.get("/risk/pretrade/status", response_model=PretradeStatusResponse)
def get_pretrade_status(
    account_id: str = Query(..., description="Trading account identifier"),
    limit: int = Query(20, ge=1, le=200, description="Maximum recent failures to return"),
    caller: str = Depends(require_admin_account),
) -> PretradeStatusResponse:
    if caller != account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and query parameter.",
        )

    snapshot = PRETRADE_SANITY.status(account_id, limit=limit)
    return PretradeStatusResponse.from_snapshot(snapshot)

