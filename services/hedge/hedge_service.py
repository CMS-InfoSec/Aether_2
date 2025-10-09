"""Hedge management service with override support and diagnostics."""
from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    TypeVar,
    cast,
)

from fastapi import APIRouter, Depends, HTTPException, status

from services.common.security import require_admin_account

ValidatorFn = TypeVar("ValidatorFn", bound=Callable[..., Any])


if TYPE_CHECKING:  # pragma: no cover - static analysis stubs for optional dependency
    class BaseModel:  # noqa: D401 - lightweight stub for mypy
        model_config: Dict[str, Any] = {}

        def __init__(self, **data: Any) -> None: ...

        @classmethod
        def model_validate(cls, obj: Any) -> "BaseModel": ...

        def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]: ...

    def Field(default: Any = None, **kwargs: Any) -> Any: ...

    def validator(*_fields: str, **__kwargs: Any) -> Callable[[ValidatorFn], ValidatorFn]: ...
else:  # pragma: no cover - runtime import with graceful fallback
    try:
        from pydantic import BaseModel, Field, validator
    except ImportError:
        class BaseModel:
            """Fallback model providing attribute assignment behaviour."""

            model_config: Dict[str, Any] = {}

            def __init__(self, **data: Any) -> None:
                for key, value in data.items():
                    setattr(self, key, value)

            def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
                del args, kwargs
                return dict(self.__dict__)

        def Field(default: Any = None, **kwargs: Any) -> Any:
            default_factory = kwargs.get("default_factory")
            if callable(default_factory):
                return default_factory()
            return default

        def validator(*_fields: str, **__kwargs: Any) -> Callable[[ValidatorFn], ValidatorFn]:
            def decorator(func: ValidatorFn) -> ValidatorFn:
                return func

            return decorator


@dataclass
class HedgeOverride:
    """Operator supplied override for the hedge target percentage."""

    target_pct: float
    reason: str
    created_at: datetime

    def as_dict(self) -> Dict[str, object]:
        return {
            "target_pct": self.target_pct,
            "reason": self.reason,
            "created_at": self.created_at,
        }


@dataclass
class HedgeDiagnostics:
    """Diagnostics produced during auto hedge evaluation."""

    volatility: float
    drawdown: float
    stablecoin_price: float
    base_target_pct: float
    adjusted_target_pct: float
    guard_triggered: bool
    guard_reason: Optional[str]
    stablecoin_deviation: float
    components: Dict[str, float]

    def as_dict(self) -> Dict[str, object]:
        data = asdict(self)
        data["auto_target_pct"] = self.adjusted_target_pct
        return data


@dataclass
class HedgeHistoryRecord:
    """Record of hedge decisions for observability."""

    timestamp: datetime
    mode: Literal["auto", "override", "override_cleared"]
    target_pct: float
    reason: Optional[str]
    diagnostics: Optional[HedgeDiagnostics]
    override: Optional[HedgeOverride]

    def as_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "timestamp": self.timestamp,
            "mode": self.mode,
            "target_pct": self.target_pct,
        }
        if self.reason:
            payload["reason"] = self.reason
        if self.diagnostics:
            payload["diagnostics"] = self.diagnostics.as_dict()
        if self.override:
            payload["override"] = self.override.as_dict()
        return payload


@dataclass
class HedgeDecision:
    """Decision returned to callers when requesting hedge guidance."""

    timestamp: datetime
    target_pct: float
    mode: Literal["auto", "override"]
    reason: Optional[str]
    diagnostics: HedgeDiagnostics
    override: Optional[HedgeOverride]

    def as_dict(self) -> Dict[str, object]:
        payload = {
            "timestamp": self.timestamp,
            "target_pct": self.target_pct,
            "mode": self.mode,
            "diagnostics": self.diagnostics.as_dict(),
        }
        if self.reason:
            payload["reason"] = self.reason
        if self.override:
            payload["override"] = self.override.as_dict()
        return payload


class HedgeMetricsRequest(BaseModel):
    """Incoming request body containing market risk metrics."""

    volatility: float = Field(..., ge=0.0, description="Annualized volatility on [0, inf) scale")
    drawdown: float = Field(..., ge=0.0, description="Normalized drawdown where 1 represents max tolerance")
    stablecoin_price: float = Field(..., gt=0.0, description="Observed stablecoin price in USD")

    @validator("volatility")
    def _validate_volatility(cls, value: float) -> float:
        if value > 10.0:  # Guard obviously incorrect inputs
            raise ValueError("volatility appears unreasonably high")
        return value

    @validator("drawdown")
    def _validate_drawdown(cls, value: float) -> float:
        if value > 5.0:
            raise ValueError("drawdown appears unreasonably high")
        return value


class HedgeOverrideRequest(BaseModel):
    """Operator override payload."""

    target_pct: float = Field(..., ge=0.0, le=100.0)
    reason: str = Field(..., min_length=3, max_length=256)


class HedgeDecisionResponse(BaseModel):
    """Response envelope for hedge guidance."""

    timestamp: datetime
    target_pct: float
    mode: Literal["auto", "override"]
    reason: Optional[str]
    diagnostics: Dict[str, object]
    override: Optional[Dict[str, object]]


class HedgeHistoryResponse(BaseModel):
    """History response entry."""

    timestamp: datetime
    mode: Literal["auto", "override", "override_cleared"]
    target_pct: float
    reason: Optional[str] = None
    diagnostics: Optional[Dict[str, object]] = None
    override: Optional[Dict[str, object]] = None


class HedgeService:
    """Encapsulates hedge logic, overrides, and diagnostics."""

    def __init__(
        self,
        *,
        history_limit: int = 200,
        stablecoin_peg: float = 1.0,
        stablecoin_threshold: float = 0.02,
        guard_floor_pct: float = 85.0,
        volatility_reference: float = 1.5,
    ) -> None:
        self._override: Optional[HedgeOverride] = None
        self._history: Deque[HedgeHistoryRecord] = deque(maxlen=history_limit)
        self._last_diagnostics: Optional[HedgeDiagnostics] = None
        self._stablecoin_peg = stablecoin_peg
        self._stablecoin_threshold = stablecoin_threshold
        self._guard_floor_pct = guard_floor_pct
        self._volatility_reference = volatility_reference

    def evaluate(self, metrics: HedgeMetricsRequest) -> HedgeDecision:
        """Compute hedge target, applying overrides and safeguards."""

        diagnostics = self._build_diagnostics(metrics)
        self._last_diagnostics = diagnostics

        decision_reason: Optional[str] = None
        override = self._override
        if override:
            target_pct = override.target_pct
            mode: Literal["auto", "override"] = "override"
            decision_reason = override.reason
        else:
            target_pct = diagnostics.adjusted_target_pct
            mode = "auto"
            if diagnostics.guard_triggered:
                decision_reason = diagnostics.guard_reason

        decision = HedgeDecision(
            timestamp=_utcnow(),
            target_pct=target_pct,
            mode=mode,
            reason=decision_reason,
            diagnostics=diagnostics,
            override=override,
        )
        self._append_history(
            HedgeHistoryRecord(
                timestamp=decision.timestamp,
                mode=mode,
                target_pct=target_pct,
                reason=decision_reason,
                diagnostics=diagnostics,
                override=override,
            )
        )
        return decision

    def set_override(self, target_pct: float, reason: str) -> HedgeOverride:
        """Apply an operator override for the hedge target percentage."""

        override = HedgeOverride(target_pct=target_pct, reason=reason, created_at=_utcnow())
        self._override = override
        self._append_history(
            HedgeHistoryRecord(
                timestamp=override.created_at,
                mode="override",
                target_pct=target_pct,
                reason=reason,
                diagnostics=self._last_diagnostics,
                override=override,
            )
        )
        return override

    def clear_override(self) -> None:
        """Clear the current hedge override if one exists."""

        if not self._override:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No override to clear")
        cleared_override = self._override
        self._override = None
        self._append_history(
            HedgeHistoryRecord(
                timestamp=_utcnow(),
                mode="override_cleared",
                target_pct=cleared_override.target_pct,
                reason=cleared_override.reason,
                diagnostics=self._last_diagnostics,
                override=cleared_override,
            )
        )

    def get_override(self) -> Optional[HedgeOverride]:
        return self._override

    def get_history(self) -> Iterable[HedgeHistoryRecord]:
        return list(self._history)

    def get_last_diagnostics(self) -> Optional[HedgeDiagnostics]:
        return self._last_diagnostics

    def health_status(self) -> Dict[str, object]:
        """Return operational metadata used by service health checks."""

        override = self._override
        latest_record = self._history[0] if self._history else None
        diagnostics = self._last_diagnostics

        payload: Dict[str, object] = {
            "mode": "override" if override else "auto",
            "override_active": override is not None,
            "history_depth": len(self._history),
            "last_decision_at": latest_record.timestamp.isoformat() if latest_record else None,
            "last_target_pct": latest_record.target_pct if latest_record else None,
            "last_guard_triggered": bool(diagnostics.guard_triggered if diagnostics else False),
        }

        if diagnostics and diagnostics.guard_reason:
            payload["last_guard_reason"] = diagnostics.guard_reason

        return payload

    def _build_diagnostics(self, metrics: HedgeMetricsRequest) -> HedgeDiagnostics:
        volatility_score = self._clamp(metrics.volatility / self._volatility_reference, 0.0, 1.0)
        drawdown_score = self._clamp(metrics.drawdown, 0.0, 1.0)
        base_score = 0.6 * volatility_score + 0.4 * drawdown_score

        if metrics.drawdown > 0.5:
            base_score = min(base_score + 0.1, 1.0)

        base_target_pct = self._clamp(base_score * 100.0, 0.0, 100.0)

        stablecoin_deviation = abs(metrics.stablecoin_price - self._stablecoin_peg) / self._stablecoin_peg
        guard_triggered = stablecoin_deviation >= self._stablecoin_threshold
        adjusted_target_pct = base_target_pct
        guard_reason: Optional[str] = None
        if guard_triggered:
            adjusted_target_pct = max(base_target_pct, self._guard_floor_pct)
            guard_reason = (
                f"Stablecoin peg deviation {stablecoin_deviation:.2%} exceeds {self._stablecoin_threshold:.2%}"
            )

        diagnostics = HedgeDiagnostics(
            volatility=metrics.volatility,
            drawdown=metrics.drawdown,
            stablecoin_price=metrics.stablecoin_price,
            base_target_pct=round(base_target_pct, 2),
            adjusted_target_pct=round(adjusted_target_pct, 2),
            guard_triggered=guard_triggered,
            guard_reason=guard_reason,
            stablecoin_deviation=round(stablecoin_deviation, 4),
            components={
                "volatility_score": round(volatility_score, 4),
                "drawdown_score": round(drawdown_score, 4),
                "base_score": round(base_score, 4),
            },
        )
        return diagnostics

    def _append_history(self, record: HedgeHistoryRecord) -> None:
        self._history.appendleft(record)

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        return max(lower, min(upper, value))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


_service = HedgeService()


def get_hedge_service() -> HedgeService:
    return _service


router = APIRouter(prefix="/hedge", tags=["hedge"])

RouteFn = TypeVar("RouteFn", bound=Callable[..., Any])


def _router_post(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around ``router.post`` to satisfy strict type checking."""

    return cast(Callable[[RouteFn], RouteFn], router.post(*args, **kwargs))


def _router_delete(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around ``router.delete`` for optional dependency safety."""

    return cast(Callable[[RouteFn], RouteFn], router.delete(*args, **kwargs))


def _router_get(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    """Typed wrapper around ``router.get`` for mypy compliance."""

    return cast(Callable[[RouteFn], RouteFn], router.get(*args, **kwargs))


@_router_post("/evaluate", response_model=HedgeDecisionResponse)
async def evaluate_hedge(
    payload: HedgeMetricsRequest,
    service: HedgeService = Depends(get_hedge_service),
) -> Dict[str, object]:
    """Return the hedge target based on supplied risk metrics."""

    decision = service.evaluate(payload)
    return decision.as_dict()


@_router_post("/override", response_model=Dict[str, object], status_code=status.HTTP_200_OK)
async def set_override(
    payload: HedgeOverrideRequest,
    service: HedgeService = Depends(get_hedge_service),
    account_id: str = Depends(require_admin_account),
) -> Dict[str, object]:
    """Override the computed hedge target percentage."""

    del account_id  # Guard is enforced via dependency injection.
    override = service.set_override(target_pct=payload.target_pct, reason=payload.reason)
    return override.as_dict()


@_router_delete("/override", status_code=status.HTTP_204_NO_CONTENT)
async def clear_override(
    service: HedgeService = Depends(get_hedge_service),
    account_id: str = Depends(require_admin_account),
) -> None:
    """Clear the active hedge override."""

    del account_id
    service.clear_override()


@_router_get("/override", response_model=Optional[Dict[str, object]])
async def get_override(
    service: HedgeService = Depends(get_hedge_service),
    account_id: str = Depends(require_admin_account),
) -> Optional[Dict[str, object]]:
    """Return the active hedge override if present."""

    del account_id
    override = service.get_override()
    return override.as_dict() if override else None


@_router_get("/history", response_model=List[HedgeHistoryResponse])
async def get_history(
    service: HedgeService = Depends(get_hedge_service),
    account_id: str = Depends(require_admin_account),
) -> List[Dict[str, object]]:
    """Return hedge history records with diagnostics."""

    del account_id
    return [record.as_dict() for record in service.get_history()]


@_router_get("/diagnostics", response_model=Optional[Dict[str, object]])
async def get_last_diagnostics(
    service: HedgeService = Depends(get_hedge_service),
    account_id: str = Depends(require_admin_account),
) -> Optional[Dict[str, object]]:
    """Return the latest hedge diagnostics snapshot."""

    del account_id
    diagnostics = service.get_last_diagnostics()
    return diagnostics.as_dict() if diagnostics else None


__all__ = ["get_hedge_service", "router", "HedgeService"]
