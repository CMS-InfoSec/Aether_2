"""Hedge management service with override support and diagnostics."""
from __future__ import annotations

import json
import logging
import math
import os
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    TypeVar,
    cast,
)

try:  # pragma: no cover - prefer the real FastAPI implementation when available
    from fastapi import APIRouter, Depends, HTTPException, status
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[assignment]
        APIRouter,
        Depends,
        HTTPException,
        status,
    )

from services.common.security import require_admin_account
from shared.audit_hooks import AuditEvent, AuditHooks, load_audit_hooks

ValidatorFn = TypeVar("ValidatorFn", bound=Callable[..., Any])


KillSwitchHandler = Callable[["HedgeMetricsRequest", "HedgeDiagnostics"], None]


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


LOGGER = logging.getLogger(__name__)


TelemetrySink = Callable[["HedgeDecision", "HedgeMetricsRequest"], None]


def _parse_datetime(value: object) -> Optional[datetime]:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


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

    def to_json_dict(self) -> Dict[str, object]:
        return {
            "target_pct": self.target_pct,
            "reason": self.reason,
            "created_at": self.created_at.isoformat(),
        }

    @staticmethod
    def from_json_dict(payload: Mapping[str, object]) -> Optional["HedgeOverride"]:
        created_at = _parse_datetime(payload.get("created_at"))
        reason = payload.get("reason")
        target = payload.get("target_pct")

        if created_at is None or not isinstance(reason, str):
            return None

        try:
            target_pct = float(target)
        except (TypeError, ValueError):
            return None

        return HedgeOverride(target_pct=target_pct, reason=reason, created_at=created_at)


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
    kill_switch_recommended: bool
    kill_switch_reason: Optional[str]

    def as_dict(self) -> Dict[str, object]:
        data = asdict(self)
        data["auto_target_pct"] = self.adjusted_target_pct
        return data

    def to_json_dict(self) -> Dict[str, object]:
        payload = {
            "volatility": self.volatility,
            "drawdown": self.drawdown,
            "stablecoin_price": self.stablecoin_price,
            "base_target_pct": self.base_target_pct,
            "adjusted_target_pct": self.adjusted_target_pct,
            "guard_triggered": self.guard_triggered,
            "guard_reason": self.guard_reason,
            "stablecoin_deviation": self.stablecoin_deviation,
            "components": dict(self.components),
            "kill_switch_recommended": self.kill_switch_recommended,
            "kill_switch_reason": self.kill_switch_reason,
        }
        return payload

    @staticmethod
    def from_json_dict(payload: Mapping[str, object]) -> Optional["HedgeDiagnostics"]:
        try:
            volatility = float(payload["volatility"])
            drawdown = float(payload["drawdown"])
            stablecoin_price = float(payload["stablecoin_price"])
            base_target_pct = float(payload["base_target_pct"])
            adjusted_target_pct = float(payload["adjusted_target_pct"])
        except (KeyError, TypeError, ValueError):
            return None

        guard_triggered_raw = payload.get("guard_triggered", False)
        guard_triggered = bool(guard_triggered_raw)

        guard_reason_raw = payload.get("guard_reason")
        guard_reason = guard_reason_raw if isinstance(guard_reason_raw, str) else None

        deviation_raw = payload.get("stablecoin_deviation", 0.0)
        try:
            stablecoin_deviation = float(deviation_raw)
        except (TypeError, ValueError):
            stablecoin_deviation = 0.0

        components_payload = payload.get("components", {})
        components: Dict[str, float] = {}
        if isinstance(components_payload, Mapping):
            for key, value in components_payload.items():
                try:
                    components[str(key)] = float(value)
                except (TypeError, ValueError):
                    continue

        kill_switch_recommended = bool(payload.get("kill_switch_recommended", False))
        kill_switch_reason_raw = payload.get("kill_switch_reason")
        kill_switch_reason = (
            kill_switch_reason_raw if isinstance(kill_switch_reason_raw, str) else None
        )

        return HedgeDiagnostics(
            volatility=volatility,
            drawdown=drawdown,
            stablecoin_price=stablecoin_price,
            base_target_pct=base_target_pct,
            adjusted_target_pct=adjusted_target_pct,
            guard_triggered=guard_triggered,
            guard_reason=guard_reason,
            stablecoin_deviation=stablecoin_deviation,
            components=components,
            kill_switch_recommended=kill_switch_recommended,
            kill_switch_reason=kill_switch_reason,
        )



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

    def to_json_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "timestamp": self.timestamp.isoformat(),
            "mode": self.mode,
            "target_pct": self.target_pct,
        }
        if self.reason:
            payload["reason"] = self.reason
        if self.diagnostics:
            payload["diagnostics"] = self.diagnostics.to_json_dict()
        if self.override:
            payload["override"] = self.override.to_json_dict()
        return payload

    @staticmethod
    def from_json_dict(payload: Mapping[str, object]) -> Optional["HedgeHistoryRecord"]:
        timestamp = _parse_datetime(payload.get("timestamp"))
        mode = payload.get("mode")

        if timestamp is None or mode not in {"auto", "override", "override_cleared"}:
            return None

        target_pct_raw = payload.get("target_pct")
        try:
            target_pct = float(target_pct_raw)
        except (TypeError, ValueError):
            return None

        reason_raw = payload.get("reason")
        reason = reason_raw if isinstance(reason_raw, str) else None

        diagnostics_payload = payload.get("diagnostics")
        diagnostics = (
            HedgeDiagnostics.from_json_dict(diagnostics_payload)
            if isinstance(diagnostics_payload, Mapping)
            else None
        )

        override_payload = payload.get("override")
        override = (
            HedgeOverride.from_json_dict(override_payload)
            if isinstance(override_payload, Mapping)
            else None
        )

        return HedgeHistoryRecord(
            timestamp=timestamp,
            mode=cast(Literal["auto", "override", "override_cleared"], mode),
            target_pct=target_pct,
            reason=reason,
            diagnostics=diagnostics,
            override=override,
        )


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
    account_id: Optional[str] = Field(
        default=None,
        description="Optional account identifier for kill-switch integration.",
    )

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

    @validator("account_id")
    def _normalise_account(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("account_id must not be empty")
        return normalized


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


class HedgeOverrideStateStore:
    """Persist hedge overrides and history for crash recovery."""

    def __init__(self, *, history_limit: int, state_path: Optional[Path] = None) -> None:
        self._history_limit = max(int(history_limit), 1)
        self._path = state_path or self._default_state_path()
        self._lock = Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _default_state_path() -> Path:
        root = Path(os.getenv("AETHER_STATE_DIR", ".aether_state"))
        path = root / "hedge_service" / "override_state.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def load(self) -> Tuple[Optional[HedgeOverride], List[HedgeHistoryRecord]]:
        with self._lock:
            try:
                raw = self._path.read_text(encoding="utf-8")
            except FileNotFoundError:
                return None, []
            except OSError as exc:
                LOGGER.warning("Failed to read hedge override state from %s: %s", self._path, exc)
                return None, []

        if not raw.strip():
            return None, []

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            LOGGER.warning("Failed to decode hedge override state from %s: %s", self._path, exc)
            return None, []

        override_payload = payload.get("override")
        history_payload = payload.get("history")

        override = (
            HedgeOverride.from_json_dict(override_payload)
            if isinstance(override_payload, Mapping)
            else None
        )

        history: List[HedgeHistoryRecord] = []
        if isinstance(history_payload, list):
            for entry in history_payload[: self._history_limit]:
                if not isinstance(entry, Mapping):
                    continue
                record = HedgeHistoryRecord.from_json_dict(entry)
                if record is not None:
                    history.append(record)

        return override, history

    def persist(
        self, override: Optional[HedgeOverride], history: Iterable[HedgeHistoryRecord]
    ) -> None:
        history_payload = [
            record.to_json_dict() for record in list(history)[: self._history_limit]
        ]
        payload = {
            "override": override.to_json_dict() if override else None,
            "history": history_payload,
        }

        serialized = json.dumps(payload)

        with self._lock:
            try:
                self._path.write_text(serialized, encoding="utf-8")
            except OSError as exc:
                LOGGER.warning("Failed to persist hedge override state to %s: %s", self._path, exc)


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
        state_store: Optional[HedgeOverrideStateStore] = None,
        min_auto_target_pct: float = 20.0,
        max_auto_target_pct: float = 95.0,
        volatility_exponent: float = 0.75,
        drawdown_reference: float = 0.35,
        drawdown_warning_threshold: float = 0.35,
        drawdown_kill_threshold: float = 0.5,
        drawdown_recovery_threshold: float = 0.25,
        kill_switch_handler: Optional[KillSwitchHandler] = None,
        telemetry_sink: Optional[TelemetrySink] = None,
        audit_hooks: Optional[AuditHooks] = None,
    ) -> None:
        self._state_store = state_store or HedgeOverrideStateStore(history_limit=history_limit)
        persisted_override, persisted_history = self._state_store.load()

        self._override: Optional[HedgeOverride] = persisted_override
        self._history: Deque[HedgeHistoryRecord] = deque(persisted_history, maxlen=history_limit)
        self._last_diagnostics: Optional[HedgeDiagnostics] = (
            self._history[0].diagnostics if self._history else None
        )
        self._last_governance_snapshot: Optional[Dict[str, object]] = (
            self._snapshot_from_history(self._history[0]) if self._history else None
        )
        self._stablecoin_peg = stablecoin_peg
        self._stablecoin_threshold = stablecoin_threshold
        self._guard_floor_pct = guard_floor_pct
        self._volatility_reference = volatility_reference
        self._volatility_exponent = max(volatility_exponent, 0.1)
        self._drawdown_reference = max(drawdown_reference, 0.01)
        self._drawdown_warning_threshold = max(drawdown_warning_threshold, 0.0)
        self._min_auto_target_pct = max(0.0, min_auto_target_pct)
        self._max_auto_target_pct = max(self._min_auto_target_pct, max_auto_target_pct)
        self._drawdown_kill_threshold = max(0.0, drawdown_kill_threshold)
        self._drawdown_recovery_threshold = self._clamp(
            drawdown_recovery_threshold,
            0.0,
            self._drawdown_kill_threshold,
        )
        self._kill_switch_handler = kill_switch_handler
        self._kill_switch_engaged = False
        self._telemetry_sink = telemetry_sink or self._default_telemetry_sink
        self._audit_hooks = audit_hooks

    def evaluate(self, metrics: HedgeMetricsRequest) -> HedgeDecision:
        """Compute hedge target, applying overrides and safeguards."""

        diagnostics = self._build_diagnostics(metrics)
        self._last_diagnostics = diagnostics

        if diagnostics.kill_switch_recommended:
            newly_engaged = not self._kill_switch_engaged
            self._kill_switch_engaged = True
            if self._kill_switch_handler and newly_engaged:
                try:
                    self._kill_switch_handler(metrics, diagnostics)
                except Exception as exc:  # pragma: no cover - defensive logging path
                    LOGGER.warning("Kill switch handler failed: %s", exc)
        else:
            if metrics.drawdown <= self._drawdown_recovery_threshold:
                self._kill_switch_engaged = False

        decision_reason: Optional[str] = None
        override = self._override
        if override:
            target_pct = override.target_pct
            mode: Literal["auto", "override"] = "override"
            decision_reason = override.reason
        else:
            target_pct = diagnostics.adjusted_target_pct
            mode = "auto"
            if diagnostics.guard_triggered or diagnostics.kill_switch_recommended:
                decision_reason = diagnostics.guard_reason or diagnostics.kill_switch_reason

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
        self._emit_telemetry(decision, metrics)
        self._record_governance_decision(decision, metrics)
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
            "kill_switch_recommended": bool(
                diagnostics.kill_switch_recommended if diagnostics else False
            ),
            "kill_switch_engaged": self._kill_switch_engaged,
        }

        if diagnostics and diagnostics.guard_reason:
            payload["last_guard_reason"] = diagnostics.guard_reason
        if diagnostics and diagnostics.kill_switch_reason:
            payload["kill_switch_reason"] = diagnostics.kill_switch_reason

        return payload

    def _build_diagnostics(self, metrics: HedgeMetricsRequest) -> HedgeDiagnostics:
        volatility_ratio = (
            metrics.volatility / self._volatility_reference
            if self._volatility_reference > 0
            else metrics.volatility
        )
        volatility_ratio = max(volatility_ratio, 0.0)
        volatility_signal = self._clamp(
            math.pow(volatility_ratio, self._volatility_exponent),
            0.0,
            3.0,
        )
        normalized_volatility = self._clamp(volatility_signal / 3.0, 0.0, 1.0)

        drawdown_ratio = metrics.drawdown / self._drawdown_reference
        drawdown_ratio = max(drawdown_ratio, 0.0)
        drawdown_signal = 1.0 - math.exp(-drawdown_ratio)
        drawdown_signal = self._clamp(drawdown_signal, 0.0, 1.0)

        combined_risk = max(
            drawdown_signal,
            0.55 * normalized_volatility + 0.45 * drawdown_signal,
        )
        previous = self._last_diagnostics
        if previous is not None:
            drawdown_delta = metrics.drawdown - previous.drawdown
            volatility_delta = metrics.volatility - previous.volatility
            if drawdown_delta > 0.0:
                combined_risk = min(
                    combined_risk
                    + min(drawdown_delta / max(self._drawdown_reference, 0.01), 0.2),
                    1.0,
                )
            elif drawdown_delta < 0.0:
                combined_risk = max(
                    combined_risk
                    - min(abs(drawdown_delta) / max(self._drawdown_reference, 0.01) * 0.1, 0.15),
                    0.0,
                )

            if volatility_delta > 0.0:
                combined_risk = min(
                    combined_risk
                    + min(volatility_delta / max(self._volatility_reference, 0.1), 0.15),
                    1.0,
                )
            elif volatility_delta < 0.0:
                combined_risk = max(
                    combined_risk
                    - min(
                        abs(volatility_delta) / max(self._volatility_reference, 0.1) * 0.05,
                        0.1,
                    ),
                    0.0,
                )

        if metrics.drawdown >= self._drawdown_warning_threshold:
            combined_risk = min(combined_risk + 0.1, 1.0)

        base_target_pct = self._min_auto_target_pct + (
            self._max_auto_target_pct - self._min_auto_target_pct
        ) * combined_risk

        stablecoin_deviation = abs(metrics.stablecoin_price - self._stablecoin_peg) / self._stablecoin_peg
        guard_triggered = stablecoin_deviation >= self._stablecoin_threshold
        adjusted_target_pct = base_target_pct
        guard_reasons: List[str] = []
        if guard_triggered:
            adjusted_target_pct = max(base_target_pct, self._guard_floor_pct)
            guard_reasons.append(
                f"Stablecoin peg deviation {stablecoin_deviation:.2%} exceeds {self._stablecoin_threshold:.2%}"
            )

        kill_switch_recommended = metrics.drawdown >= self._drawdown_kill_threshold
        kill_switch_reason: Optional[str] = None
        if kill_switch_recommended:
            adjusted_target_pct = max(adjusted_target_pct, self._max_auto_target_pct)
            kill_switch_reason = (
                "Drawdown {drawdown:.0%} exceeds kill switch threshold {threshold:.0%}".format(
                    drawdown=metrics.drawdown,
                    threshold=self._drawdown_kill_threshold,
                )
            )
            guard_reasons.append(kill_switch_reason)

        guard_reason: Optional[str] = None
        if guard_reasons:
            guard_reason = "; ".join(guard_reasons)

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
                "volatility_signal": round(normalized_volatility, 4),
                "drawdown_signal": round(drawdown_signal, 4),
                "combined_risk_score": round(combined_risk, 4),
            },
            kill_switch_recommended=kill_switch_recommended,
            kill_switch_reason=kill_switch_reason,
        )
        return diagnostics

    def _append_history(self, record: HedgeHistoryRecord) -> None:
        self._history.appendleft(record)
        self._persist_state()
        self._last_governance_snapshot = self._snapshot_from_history(record)

    def _persist_state(self) -> None:
        try:
            self._state_store.persist(self._override, self._history)
        except Exception as exc:  # pragma: no cover - defensive guard
            LOGGER.warning("Failed to persist hedge override snapshot: %s", exc)

    def _default_telemetry_sink(
        self, decision: "HedgeDecision", metrics: "HedgeMetricsRequest"
    ) -> None:
        payload = {
            "mode": decision.mode,
            "target_pct": decision.target_pct,
            "reason": decision.reason,
            "volatility": decision.diagnostics.volatility,
            "drawdown": decision.diagnostics.drawdown,
            "guard_triggered": decision.diagnostics.guard_triggered,
            "kill_switch_recommended": decision.diagnostics.kill_switch_recommended,
            "kill_switch_engaged": self._kill_switch_engaged,
            "account_id": metrics.account_id,
        }
        LOGGER.info("hedge.decision", extra={"hedge": payload})

    def _emit_telemetry(
        self, decision: "HedgeDecision", metrics: "HedgeMetricsRequest"
    ) -> None:
        try:
            self._telemetry_sink(decision, metrics)
        except Exception as exc:  # pragma: no cover - defensive guard
            LOGGER.warning("Failed to emit hedge telemetry: %s", exc)

    def _ensure_audit_hooks(self) -> Optional[AuditHooks]:
        if self._audit_hooks is None:
            try:
                self._audit_hooks = load_audit_hooks()
            except Exception as exc:  # pragma: no cover - defensive guard
                LOGGER.warning("Failed to load audit hooks: %s", exc)
                return None
        return self._audit_hooks

    def _record_governance_decision(
        self, decision: "HedgeDecision", metrics: "HedgeMetricsRequest"
    ) -> None:
        hooks = self._ensure_audit_hooks()
        if hooks is None:
            return

        account = metrics.account_id or "global"
        entity = self._governance_entity(account)
        before_snapshot = dict(self._last_governance_snapshot or {})
        after_snapshot = self._build_governance_snapshot(decision)

        context = {
            "account_id": account,
            "volatility": decision.diagnostics.volatility,
            "drawdown": decision.diagnostics.drawdown,
            "stablecoin": decision.diagnostics.stablecoin_price,
            "kill_switch_engaged": self._kill_switch_engaged,
        }

        event = AuditEvent(
            actor="system",
            action=f"hedge.decision.{decision.mode}",
            entity=entity,
            before=before_snapshot,
            after=after_snapshot,
            context=context,
        )
        event.log_with_fallback(
            hooks,
            LOGGER,
            failure_message="Failed to record hedge governance decision",
            disabled_message="Audit logging disabled; skipping hedge decision event",
        )
        self._last_governance_snapshot = after_snapshot

    def _snapshot_from_history(self, record: HedgeHistoryRecord) -> Dict[str, object]:
        diagnostics = record.diagnostics
        payload: Dict[str, object] = {
            "mode": record.mode,
            "target_pct": record.target_pct,
            "timestamp": record.timestamp.isoformat(),
        }
        if record.reason:
            payload["reason"] = record.reason
        if diagnostics:
            payload.update(
                {
                    "volatility": diagnostics.volatility,
                    "drawdown": diagnostics.drawdown,
                    "guard_triggered": diagnostics.guard_triggered,
                    "kill_switch_recommended": diagnostics.kill_switch_recommended,
                    "kill_switch_reason": diagnostics.kill_switch_reason,
                }
            )
            if diagnostics.guard_reason:
                payload["guard_reason"] = diagnostics.guard_reason
        if record.override:
            payload["override"] = record.override.to_json_dict()
        return payload

    def _build_governance_snapshot(self, decision: "HedgeDecision") -> Dict[str, object]:
        diagnostics = decision.diagnostics
        snapshot: Dict[str, object] = {
            "mode": decision.mode,
            "target_pct": decision.target_pct,
            "timestamp": decision.timestamp.isoformat(),
            "guard_triggered": diagnostics.guard_triggered,
            "kill_switch_recommended": diagnostics.kill_switch_recommended,
            "kill_switch_engaged": self._kill_switch_engaged,
        }
        if decision.reason:
            snapshot["reason"] = decision.reason
        snapshot["volatility"] = diagnostics.volatility
        snapshot["drawdown"] = diagnostics.drawdown
        snapshot["stablecoin_price"] = diagnostics.stablecoin_price
        if diagnostics.guard_reason:
            snapshot["guard_reason"] = diagnostics.guard_reason
        if diagnostics.kill_switch_reason:
            snapshot["kill_switch_reason"] = diagnostics.kill_switch_reason
        if decision.override:
            snapshot["override"] = decision.override.to_json_dict()
        return snapshot

    @staticmethod
    def _governance_entity(account_id: str) -> str:
        normalized = (account_id or "").strip().lower()
        return f"hedge:{normalized or 'global'}"

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


__all__ = ["get_hedge_service", "router", "HedgeService", "HedgeOverrideStateStore"]
