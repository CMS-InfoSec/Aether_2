"""FastAPI application exposing guarded configuration management endpoints."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from shared.audit import AuditLogEntry, AuditLogStore, TimescaleAuditLogger


# ---------------------------------------------------------------------------
# Domain models representing pending changes and applied versions
# ---------------------------------------------------------------------------


@dataclass
class PendingConfigChange:
    """In-memory representation of a ``config_pending`` table row."""

    request_id: str
    key: str
    new_value: Any
    requested_by: str
    approvals: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    applied: bool = False
    applied_at: Optional[datetime] = None

    def add_initial_signature(self, author: str) -> None:
        if author not in self.approvals:
            self.approvals.append(author)

    def approve(self, author: str) -> None:
        if author in self.approvals:
            raise PermissionError("duplicate_approval")
        self.approvals.append(author)

    def mark_applied(self) -> None:
        self.applied = True
        self.applied_at = datetime.now(timezone.utc)

    def to_payload(self, requires_second_approval: bool) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "key": self.key,
            "new_value": self.new_value,
            "requested_by": self.requested_by,
            "approvals": list(self.approvals),
            "created_at": self.created_at,
            "applied": self.applied,
            "applied_at": self.applied_at,
            "requires_second_approval": requires_second_approval,
        }


@dataclass(frozen=True)
class ConfigVersionRecord:
    """Representation of a ``config_versions`` table row."""

    config_key: str
    version: int
    value: Any
    applied_at: datetime
    approvals: Tuple[str, ...]
    applied_by: str


# ---------------------------------------------------------------------------
# Persistence layer (in-memory for testing) orchestrating audit logging
# ---------------------------------------------------------------------------


class ConfigChangeStore:
    """Coordinator for pending config changes and applied versions."""

    _guarded_keys: ClassVar[Tuple[str, ...]] = (
        "risk.max_notional",
        "risk.max_drawdown",
        "trading.kill_switch",
    )
    _pending: ClassVar[Dict[str, PendingConfigChange]] = {}
    _config_versions: ClassVar[List[ConfigVersionRecord]] = []
    _audit_store: ClassVar[AuditLogStore] = AuditLogStore()
    _audit_logger: ClassVar[TimescaleAuditLogger] = TimescaleAuditLogger(_audit_store)

    # ------------------------------------------------------------------
    # Guard list management (used in testing)
    # ------------------------------------------------------------------
    @classmethod
    def set_guarded_keys(cls, keys: Iterable[str]) -> None:
        cls._guarded_keys = tuple(sorted(set(keys)))

    @classmethod
    def guarded_keys(cls) -> Tuple[str, ...]:
        return cls._guarded_keys

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------
    @classmethod
    def reset(cls) -> None:
        cls._pending = {}
        cls._config_versions = []
        cls._audit_store = AuditLogStore()
        cls._audit_logger = TimescaleAuditLogger(cls._audit_store)

    @classmethod
    def audit_entries(cls) -> Tuple[AuditLogEntry, ...]:
        return tuple(cls._audit_store.all())

    @classmethod
    def requires_dual_signature(cls, key: str) -> bool:
        return key in cls._guarded_keys

    @classmethod
    def _current_value(cls, key: str) -> Any:
        for record in reversed(cls._config_versions):
            if record.config_key == key:
                return record.value
        return None

    @classmethod
    def _next_version(cls, key: str) -> int:
        versions = [record.version for record in cls._config_versions if record.config_key == key]
        return max(versions, default=0) + 1

    @classmethod
    def _ensure_no_pending(cls, key: str) -> None:
        for change in cls._pending.values():
            if change.key == key and not change.applied:
                raise RuntimeError("pending_change_exists")

    # ------------------------------------------------------------------
    # Core workflows
    # ------------------------------------------------------------------
    @classmethod
    def request_change(cls, key: str, new_value: Any, author: str) -> Tuple[PendingConfigChange, Optional[ConfigVersionRecord]]:
        cls._ensure_no_pending(key)

        request_id = str(uuid.uuid4())
        change = PendingConfigChange(
            request_id=request_id,
            key=key,
            new_value=new_value,
            requested_by=author,
        )
        change.add_initial_signature(author)
        cls._pending[request_id] = change

        cls._audit_logger.record(
            action="config_update_requested",
            actor_id=author,
            before={"key": key, "current_value": cls._current_value(key)},
            after={
                "key": key,
                "new_value": new_value,
                "request_id": request_id,
                "approvals": list(change.approvals),
            },
        )

        applied_record: Optional[ConfigVersionRecord] = None
        if not cls.requires_dual_signature(key):
            applied_record = cls._apply_change(change, actor=author)

        return change, applied_record

    @classmethod
    def approve_change(
        cls,
        request_id: str,
        author: str,
    ) -> Tuple[PendingConfigChange, Optional[ConfigVersionRecord]]:
        change = cls._pending.get(request_id)
        if not change:
            raise KeyError("change_missing")
        if change.applied:
            raise PermissionError("change_already_applied")

        before = {"approvals": list(change.approvals)}
        change.approve(author)
        cls._audit_logger.record(
            action="config_update_approved",
            actor_id=author,
            before={"request_id": request_id, **before},
            after={
                "request_id": request_id,
                "approvals": list(change.approvals),
            },
        )

        applied_record: Optional[ConfigVersionRecord] = None
        if not cls.requires_dual_signature(change.key) or len(change.approvals) >= 2:
            applied_record = cls._apply_change(change, actor=author)

        return change, applied_record

    @classmethod
    def pending_changes(cls) -> Tuple[PendingConfigChange, ...]:
        return tuple(change for change in cls._pending.values() if not change.applied)

    @classmethod
    def config_versions(cls) -> Tuple[ConfigVersionRecord, ...]:
        return tuple(cls._config_versions)

    @classmethod
    def _apply_change(cls, change: PendingConfigChange, actor: str) -> ConfigVersionRecord:
        before = {
            "key": change.key,
            "current_value": cls._current_value(change.key),
        }
        change.mark_applied()
        cls._pending.pop(change.request_id, None)

        record = ConfigVersionRecord(
            config_key=change.key,
            version=cls._next_version(change.key),
            value=change.new_value,
            applied_at=change.applied_at or datetime.now(timezone.utc),
            approvals=tuple(change.approvals),
            applied_by=actor,
        )
        cls._config_versions.append(record)

        cls._audit_logger.record(
            action="config_update_applied",
            actor_id=actor,
            before=before,
            after={
                "key": change.key,
                "value": change.new_value,
                "version": record.version,
                "approvals": list(change.approvals),
            },
        )
        return record


# ---------------------------------------------------------------------------
# API schema definitions
# ---------------------------------------------------------------------------


class ConfigUpdateRequest(BaseModel):
    key: str
    new_value: Any
    author: str


class ConfigChangeResponse(BaseModel):
    request_id: str
    key: str
    new_value: Any
    approvals: List[str]
    requested_by: str
    status: str = Field(..., pattern="^(pending|applied)$")
    requires_second_approval: bool
    version: Optional[int] = None
    applied_at: Optional[datetime] = None
    created_at: datetime


class ConfigApproveRequest(BaseModel):
    request_id: str
    author: str


# ---------------------------------------------------------------------------
# FastAPI wiring
# ---------------------------------------------------------------------------


app = FastAPI(title="Config Service")


def _response_from_change(
    change: PendingConfigChange,
    *,
    requires_second_approval: bool,
    applied_record: Optional[ConfigVersionRecord],
) -> ConfigChangeResponse:
    status_value = "applied" if applied_record else "pending"
    version = applied_record.version if applied_record else None
    applied_at = applied_record.applied_at if applied_record else change.applied_at
    return ConfigChangeResponse(
        request_id=change.request_id,
        key=change.key,
        new_value=change.new_value,
        approvals=list(change.approvals),
        requested_by=change.requested_by,
        status=status_value,
        requires_second_approval=requires_second_approval,
        version=version,
        applied_at=applied_at,
        created_at=change.created_at,
    )


@app.post("/config/update", response_model=ConfigChangeResponse)
def request_config_change(payload: ConfigUpdateRequest) -> ConfigChangeResponse:
    try:
        change, applied_record = ConfigChangeStore.request_change(
            payload.key,
            payload.new_value,
            payload.author,
        )
    except RuntimeError as exc:
        if str(exc) == "pending_change_exists":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="pending_change_exists",
            ) from exc
        raise

    requires_second = ConfigChangeStore.requires_dual_signature(payload.key)
    return _response_from_change(change, requires_second_approval=requires_second, applied_record=applied_record)


@app.get("/config/pending", response_model=List[ConfigChangeResponse])
def list_pending_changes() -> List[ConfigChangeResponse]:
    responses: List[ConfigChangeResponse] = []
    for change in ConfigChangeStore.pending_changes():
        requires_second = ConfigChangeStore.requires_dual_signature(change.key)
        responses.append(
            _response_from_change(change, requires_second_approval=requires_second, applied_record=None)
        )
    return responses


@app.post("/config/approve", response_model=ConfigChangeResponse)
def approve_config_change(payload: ConfigApproveRequest) -> ConfigChangeResponse:
    try:
        change, applied_record = ConfigChangeStore.approve_change(payload.request_id, payload.author)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="change_missing") from exc
    except PermissionError as exc:
        detail = str(exc)
        status_code = status.HTTP_400_BAD_REQUEST
        if detail == "change_already_applied":
            status_code = status.HTTP_409_CONFLICT
        raise HTTPException(status_code=status_code, detail=detail) from exc

    requires_second = ConfigChangeStore.requires_dual_signature(change.key)
    if requires_second and applied_record is None:
        # Guarded keys require two distinct approvals; ensure the payload reflects pending status.
        return _response_from_change(change, requires_second_approval=True, applied_record=None)

    return _response_from_change(change, requires_second_approval=requires_second, applied_record=applied_record)


__all__ = [
    "app",
    "ConfigChangeStore",
    "ConfigUpdateRequest",
    "ConfigApproveRequest",
    "ConfigChangeResponse",
]

