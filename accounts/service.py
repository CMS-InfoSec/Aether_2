"""Accounts service coordinating admin profiles and approval workflows."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from shared.audit import SensitiveActionRecorder


@dataclass
class AdminProfile:
    admin_id: str
    email: str
    display_name: str
    kraken_credentials_linked: bool = False
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class RiskConfigurationChange:
    request_id: str
    requested_by: str
    payload: dict
    approvals: List[str] = field(default_factory=list)
    executed: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def approve(self, admin_id: str) -> None:
        if admin_id in self.approvals:
            raise PermissionError("duplicate_approval")
        self.approvals.append(admin_id)

    @property
    def ready(self) -> bool:
        return len(self.approvals) >= 2 and not self.executed


class AccountsService:
    """Handles admin profile state, credential status, and risk workflows."""

    def __init__(self, recorder: SensitiveActionRecorder) -> None:
        self._recorder = recorder
        self._profiles: Dict[str, AdminProfile] = {}
        self._risk_changes: Dict[str, RiskConfigurationChange] = {}

    def upsert_profile(self, profile: AdminProfile) -> AdminProfile:
        before = self._profiles.get(profile.admin_id)
        self._profiles[profile.admin_id] = profile
        profile.last_updated = datetime.now(timezone.utc)
        self._recorder.record(
            action="profile_update",
            actor_id=profile.admin_id,
            before=_profile_snapshot(before),
            after=_profile_snapshot(profile),
        )
        return profile

    def get_profile(self, admin_id: str) -> Optional[AdminProfile]:
        return self._profiles.get(admin_id)

    def set_kraken_credentials_status(self, admin_id: str, linked: bool) -> AdminProfile:
        profile = self._profiles.get(admin_id)
        if not profile:
            raise KeyError("profile_missing")
        before = _profile_snapshot(profile)
        profile.kraken_credentials_linked = linked
        profile.last_updated = datetime.now(timezone.utc)
        self._recorder.record(
            action="kraken_credentials_update",
            actor_id=admin_id,
            before=before,
            after=_profile_snapshot(profile),
        )
        return profile

    def request_risk_configuration_change(self, admin_id: str, payload: dict) -> RiskConfigurationChange:
        request_id = str(uuid.uuid4())
        change = RiskConfigurationChange(
            request_id=request_id,
            requested_by=admin_id,
            payload=payload,
        )
        self._risk_changes[request_id] = change
        self._recorder.record(
            action="risk_change_requested",
            actor_id=admin_id,
            before=None,
            after={"request_id": request_id, "payload": payload},
        )
        return change

    def approve_risk_change(self, admin_id: str, request_id: str) -> RiskConfigurationChange:
        change = self._risk_changes.get(request_id)
        if not change:
            raise KeyError("request_missing")
        if change.executed:
            raise PermissionError("already_executed")
        change.approve(admin_id)
        if change.ready:
            self._finalize_risk_change(change)
        return change

    def _finalize_risk_change(self, change: RiskConfigurationChange) -> None:
        change.executed = True
        self._recorder.record(
            action="risk_change_executed",
            actor_id=change.requested_by,
            before={"payload": change.payload, "approvals": change.approvals[:-1]},
            after={"payload": change.payload, "approvals": change.approvals},
        )

    def rotate_secret(self, admin_id: str, secret_name: str) -> None:
        self._recorder.record(
            action="secret_rotation",
            actor_id=admin_id,
            before={"secret": secret_name, "status": "active"},
            after={"secret": secret_name, "status": "rotated"},
        )

    def trigger_kill_switch(self, admin_id: str, reason: str) -> None:
        self._recorder.record(
            action="kill_switch",
            actor_id=admin_id,
            before={"reason": reason, "active": False},
            after={"reason": reason, "active": True},
        )


def _profile_snapshot(profile: Optional[AdminProfile]) -> Optional[dict]:
    if not profile:
        return None
    return {
        "admin_id": profile.admin_id,
        "email": profile.email,
        "display_name": profile.display_name,
        "kraken_credentials_linked": profile.kraken_credentials_linked,
        "last_updated": profile.last_updated.isoformat(),
    }


__all__ = [
    "AdminProfile",
    "RiskConfigurationChange",
    "AccountsService",
]
