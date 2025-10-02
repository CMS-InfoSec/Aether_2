"""Audit mode API exposing read-only operational telemetry."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence
from uuid import UUID

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, status
from pydantic import BaseModel, ConfigDict

from services.common.adapters import TimescaleAdapter
from services.common.security import (
    AuthenticatedPrincipal,
    require_authenticated_principal,
)
from shared.audit import AuditLogEntry, AuditLogStore


DEFAULT_SYSTEM_CONFIG = Path(__file__).resolve().parent / "config" / "system.yaml"


@dataclass(frozen=True)
class AuditModeConfig:
    """Configuration governing audit mode behaviour."""

    enabled: bool = False
    auditor_accounts: Sequence[str] = ("auditor",)


@dataclass(frozen=True)
class AuditorPrincipal:
    """Identity extracted from request headers for audit mode access control."""

    account_id: str
    role: str = "auditor"


class AuditLogRecord(BaseModel):
    """Serialised view of an :class:`~shared.audit.AuditLogEntry`."""

    id: UUID
    action: str
    actor_id: str
    before: Dict[str, Any]
    after: Dict[str, Any]
    correlation_id: str | None = None
    created_at: datetime

    model_config = ConfigDict(frozen=True)

    @classmethod
    def from_entry(cls, entry: AuditLogEntry) -> "AuditLogRecord":
        return cls(
            id=entry.id,
            action=entry.action,
            actor_id=entry.actor_id,
            before=dict(entry.before),
            after=dict(entry.after),
            correlation_id=entry.correlation_id,
            created_at=entry.created_at,
        )


class RiskConfigSnapshot(BaseModel):
    """Read-only snapshot of the current risk configuration for an account."""

    account_id: str
    configuration: Dict[str, Any]

    model_config = ConfigDict(frozen=True)


class EventRecord(BaseModel):
    """Generic event record model used for risk events and fills."""

    model_config = ConfigDict(extra="allow", frozen=True)

    recorded_at: datetime | None = None
    timestamp: datetime | None = None
    payload: Dict[str, Any] | None = None
    event_type: str | None = None
    type: str | None = None


class AuditRepository:
    """Facade for retrieving immutable audit data exposed via the API."""

    def __init__(self, store: AuditLogStore) -> None:
        self._store = store

    def audit_logs(
        self,
        *,
        actor_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[AuditLogEntry]:
        entries = list(self._store.all())
        if actor_id:
            entries = [entry for entry in entries if entry.actor_id == actor_id]
        entries.sort(key=lambda entry: entry.created_at, reverse=True)
        if limit is not None:
            entries = entries[: int(limit)]
        return entries

    def risk_configuration(self, account_id: str) -> Dict[str, Any]:
        adapter = TimescaleAdapter(account_id=account_id)
        return adapter.load_risk_config()

    def risk_events(self, account_id: str, limit: Optional[int] = None) -> List[Mapping[str, Any]]:
        adapter = TimescaleAdapter(account_id=account_id)
        events = adapter.events().get("events", [])
        sorted_events = sorted(
            events,
            key=lambda entry: entry.get("timestamp", entry.get("recorded_at")),
            reverse=True,
        )
        if limit is not None:
            sorted_events = sorted_events[: int(limit)]
        return sorted_events

    def fills(self, account_id: str, limit: Optional[int] = None) -> List[Mapping[str, Any]]:
        adapter = TimescaleAdapter(account_id=account_id)
        fills = adapter.events().get("fills", [])
        sorted_fills = sorted(
            fills,
            key=lambda entry: entry.get("recorded_at", entry.get("timestamp")),
            reverse=True,
        )
        if limit is not None:
            sorted_fills = sorted_fills[: int(limit)]
        return sorted_fills


router = APIRouter(prefix="/audit", tags=["audit"])


def _normalize_accounts(raw_accounts: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    for account in raw_accounts:
        value = account.strip().lower()
        if value:
            normalized.append(value)
    return normalized


def _fallback_parse_system_config(lines: Iterable[str]) -> Dict[str, Any]:
    parsed: Dict[str, Any] = {}
    for raw_line in lines:
        line = raw_line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        lowered = value.lower()
        if lowered in {"true", "yes", "1", "on"}:
            parsed[key] = True
        elif lowered in {"false", "no", "0", "off"}:
            parsed[key] = False
        elif value.startswith("[") and value.endswith("]"):
            inner = value[1:-1].strip()
            if not inner:
                parsed[key] = []
            else:
                parsed[key] = [item.strip().strip("'\"") for item in inner.split(",")]
        else:
            parsed[key] = value.strip("'\"")
    return parsed


def load_audit_mode_config(path: Path | None = None) -> AuditModeConfig:
    """Load ``AuditModeConfig`` from ``system.yaml`` if present."""

    config_path = path or DEFAULT_SYSTEM_CONFIG
    if not config_path.exists():
        return AuditModeConfig()

    raw_text = config_path.read_text(encoding="utf-8")
    data: MutableMapping[str, Any]
    try:
        import yaml  # type: ignore
    except Exception:  # pragma: no cover - best effort fallback
        data = _fallback_parse_system_config(raw_text.splitlines())
    else:  # pragma: no cover - exercised indirectly via tests
        loaded = yaml.safe_load(raw_text) or {}
        if not isinstance(loaded, Mapping):
            raise ValueError("system.yaml must contain a mapping at the top level")
        data = dict(loaded)

    enabled = bool(data.get("enable_audit_mode", False))
    accounts_raw = data.get("auditor_accounts")

    if accounts_raw is None:
        accounts: Sequence[str] = ("auditor",)
    elif isinstance(accounts_raw, str):
        accounts = (accounts_raw,)
    elif isinstance(accounts_raw, Iterable):
        accounts = tuple(str(item) for item in accounts_raw)
    else:
        raise ValueError("auditor_accounts must be a string or list of strings")

    normalized_accounts = tuple(_normalize_accounts(accounts)) or ("auditor",)
    return AuditModeConfig(enabled=enabled, auditor_accounts=normalized_accounts)


def get_audit_mode_config(request: Request) -> AuditModeConfig:
    config = getattr(request.app.state, "audit_mode_config", None)
    if isinstance(config, AuditModeConfig):
        return config
    return AuditModeConfig()


def ensure_audit_mode_enabled(config: AuditModeConfig = Depends(get_audit_mode_config)) -> None:
    if not config.enabled:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Audit mode is disabled")


def require_auditor_identity(
    principal: AuthenticatedPrincipal = Depends(require_authenticated_principal),
    config: AuditModeConfig = Depends(get_audit_mode_config),
) -> AuditorPrincipal:
    account = principal.normalized_account
    allowed = {acct.strip().lower() for acct in config.auditor_accounts}
    if account not in allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is not authorized for audit access.",
        )

    return AuditorPrincipal(account_id=account)


def get_audit_repository(request: Request) -> AuditRepository:
    store = getattr(request.app.state, "audit_store", None)
    if not isinstance(store, AuditLogStore):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Audit log store is not configured.",
        )
    return AuditRepository(store)


@router.get(
    "/logs",
    response_model=list[AuditLogRecord],
    dependencies=[Depends(ensure_audit_mode_enabled)],
)
def list_audit_logs(
    _: AuditorPrincipal = Depends(require_auditor_identity),
    actor_id: str | None = Query(None, description="Filter logs by actor identifier"),
    limit: int = Query(
        100,
        ge=1,
        le=500,
        description="Maximum number of records to return",
    ),
    repository: AuditRepository = Depends(get_audit_repository),
) -> List[AuditLogRecord]:
    entries = repository.audit_logs(actor_id=actor_id, limit=limit)
    return [AuditLogRecord.from_entry(entry) for entry in entries]


@router.get(
    "/configs",
    response_model=RiskConfigSnapshot,
    dependencies=[Depends(ensure_audit_mode_enabled)],
)
def read_risk_configuration(
    _: AuditorPrincipal = Depends(require_auditor_identity),
    account_id: str = Query(..., min_length=1, description="Account identifier"),
    repository: AuditRepository = Depends(get_audit_repository),
) -> RiskConfigSnapshot:
    configuration = repository.risk_configuration(account_id=account_id)
    return RiskConfigSnapshot(account_id=account_id, configuration=configuration)


@router.get(
    "/risk-events",
    response_model=list[EventRecord],
    dependencies=[Depends(ensure_audit_mode_enabled)],
)
def read_risk_events(
    _: AuditorPrincipal = Depends(require_auditor_identity),
    account_id: str = Query(..., min_length=1, description="Account identifier"),
    limit: int = Query(
        100,
        ge=1,
        le=500,
        description="Maximum number of events to return",
    ),
    repository: AuditRepository = Depends(get_audit_repository),
) -> List[EventRecord]:
    events = repository.risk_events(account_id=account_id, limit=limit)
    return [EventRecord.model_validate(event) for event in events]


@router.get(
    "/fills",
    response_model=list[EventRecord],
    dependencies=[Depends(ensure_audit_mode_enabled)],
)
def read_fills(
    _: AuditorPrincipal = Depends(require_auditor_identity),
    account_id: str = Query(..., min_length=1, description="Account identifier"),
    limit: int = Query(
        100,
        ge=1,
        le=500,
        description="Maximum number of fills to return",
    ),
    repository: AuditRepository = Depends(get_audit_repository),
) -> List[EventRecord]:
    fills = repository.fills(account_id=account_id, limit=limit)
    return [EventRecord.model_validate(fill) for fill in fills]


def configure_audit_mode(app: FastAPI, *, config: AuditModeConfig | None = None) -> AuditModeConfig:
    """Attach the audit router to *app* when audit mode is enabled."""

    resolved_config = config or load_audit_mode_config()
    app.state.audit_mode_config = resolved_config
    if resolved_config.enabled:
        app.include_router(router)
    return resolved_config


__all__ = [
    "AuditModeConfig",
    "AuditorPrincipal",
    "AuditLogRecord",
    "RiskConfigSnapshot",
    "EventRecord",
    "configure_audit_mode",
    "load_audit_mode_config",
    "router",
]
