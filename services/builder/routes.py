"""FastAPI routes tailored for the Builder.io administrative UI."""
from __future__ import annotations

import logging
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

try:  # pragma: no cover - prefer the real FastAPI implementation when available
    from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[assignment]
        APIRouter,
        Depends,
        Header,
        HTTPException,
        Query,
        Request,
        status,
    )
from pydantic import BaseModel, EmailStr, Field

from accounts.service import AccountsService, AdminProfile
from services.builder.secrets_client import (
    SecretsServiceConfigurationError,
    SecretsServiceTransportError,
    request_json,
)
from services.common.security import (
    AuthenticatedPrincipal,
    require_authenticated_principal,
    require_mfa_context,
)
from services.hedge.hedge_service import get_hedge_service, HedgeService
from services.portfolio.balance_reader import BalanceReader, BalanceRetrievalError
from shared.audit import AuditLogEntry, AuditLogStore, SensitiveActionRecorder
from shared.sim_mode import SimModeStatus, sim_mode_repository
from shared.trade_logging import read_trade_log

router = APIRouter(prefix="/builder", tags=["builder"])
secrets_router = APIRouter(prefix="/secrets", tags=["secrets"])


_logger = logging.getLogger(__name__)


_ALLOW_INSECURE_HTTP = "pytest" in sys.modules or os.getenv("BUILDER_ALLOW_INSECURE_HTTP") == "1"


def _require_https(request: Request) -> None:
    """Enforce HTTPS access for sensitive Builder UI endpoints."""

    scheme = request.headers.get("X-Forwarded-Proto", request.url.scheme or "").lower()
    if scheme != "https" and not _ALLOW_INSECURE_HTTP:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="HTTPS is required for this endpoint.",
        )


def _get_accounts_service(request: Request) -> AccountsService:
    service = getattr(request.app.state, "accounts_service", None)
    if not isinstance(service, AccountsService):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Accounts service is not configured.",
        )
    return service


def _get_audit_store(request: Request) -> AuditLogStore:
    store = getattr(request.app.state, "audit_store", None)
    if not isinstance(store, AuditLogStore):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Audit log store is unavailable.",
        )
    return store


def _get_auditor(request: Request) -> SensitiveActionRecorder:
    recorder = getattr(request.app.state, "sensitive_recorder", None)
    if not isinstance(recorder, SensitiveActionRecorder):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Sensitive action recorder is unavailable.",
        )
    return recorder


def _ensure_balance_reader(request: Request) -> BalanceReader:
    reader = getattr(request.app.state, "builder_balance_reader", None)
    if isinstance(reader, BalanceReader):
        return reader
    reader = BalanceReader()
    request.app.state.builder_balance_reader = reader
    return reader


class OnboardingStep(BaseModel):
    id: str = Field(..., description="Stable identifier for the onboarding step")
    title: str
    description: str
    completed: bool
    completed_at: datetime | None = None


class OnboardingState(BaseModel):
    account_id: str
    steps: list[OnboardingStep]
    all_completed: bool


class ProfileUpdateRequest(BaseModel):
    email: EmailStr
    display_name: str = Field(..., min_length=1, max_length=255)


class CredentialsStatusRequest(BaseModel):
    linked: bool = Field(..., description="True when API credentials have been provisioned")


class GovernanceAcknowledgeRequest(BaseModel):
    acknowledged: bool = Field(True, description="Flag confirming the governance policy acknowledgement")


class SimulationStatusModel(BaseModel):
    active: bool
    reason: str | None = None
    since: datetime | None = None


class HedgeStatusModel(BaseModel):
    mode: str = Field(..., description="auto when computed targets are used, manual when override active")
    target_pct: float | None = Field(None, description="Effective hedge target percentage")
    override: Mapping[str, Any] | None = None
    diagnostics: Mapping[str, Any] | None = None


class TradeEntryModel(BaseModel):
    timestamp: datetime
    symbol: str
    side: str
    quantity: float
    price: float
    pnl: float | None = None
    client_order_id: str
    exchange_order_id: str | None = None
    transport: str | None = None
    simulated: bool = False


class BalanceSnapshotModel(BaseModel):
    available_usd: float | None = None
    deployed_usd: float | None = None
    total_nav_usd: float | None = None
    stablecoin_depeg_bps: float | None = None


class DashboardResponse(BaseModel):
    account_id: str
    balances: BalanceSnapshotModel | None = None
    simulation: SimulationStatusModel
    hedge: HedgeStatusModel
    recent_trades: list[TradeEntryModel]


class AuditLogEntryModel(BaseModel):
    id: str
    action: str
    actor: str
    created_at: datetime
    before: Mapping[str, Any]
    after: Mapping[str, Any]
    correlation_id: str | None = None


class AuditLogResponse(BaseModel):
    account_id: str
    entries: list[AuditLogEntryModel]


class ApiKeyUploadRequest(BaseModel):
    api_key: str = Field(..., min_length=10, max_length=128)
    api_secret: str = Field(..., min_length=20, max_length=512)


class ApiKeyUploadResponse(BaseModel):
    account_id: str
    secret_name: str
    rotated_at: datetime


class SecretsStatusModel(BaseModel):
    last_rotated_at: str | None = Field(
        default=None,
        description="ISO-8601 timestamp for the most recent credential rotation.",
    )
    last_rotated_by: str | None = Field(
        default=None,
        description="Identifier for the actor that last rotated the credentials.",
    )
    status: str | None = Field(
        default=None,
        description="Human friendly message describing the credential freshness.",
    )


class SecretsAuditEntryModel(BaseModel):
    actor: str = Field(..., description="Actor responsible for the rotation event.")
    rotated_at: datetime = Field(
        ..., description="ISO-8601 timestamp describing when rotation completed."
    )
    notes: str | None = Field(
        default=None, description="Optional contextual note for the rotation event."
    )


class SecretsRotateResponseModel(BaseModel):
    secret_name: str = Field(..., description="Kubernetes secret storing the credentials.")
    last_rotated: datetime = Field(
        ..., description="Timestamp describing when the credentials were rotated."
    )


def _profile_step(profile: AdminProfile | None) -> OnboardingStep:
    completed = bool(profile and profile.display_name and profile.email)
    completed_at = profile.last_updated if completed and profile else None
    return OnboardingStep(
        id="profile",
        title="Account profile",
        description="Configure contact details and account display name.",
        completed=completed,
        completed_at=completed_at,
    )


def _credentials_step(profile: AdminProfile | None) -> OnboardingStep:
    completed = bool(profile and profile.kraken_credentials_linked)
    completed_at = profile.last_updated if completed and profile else None
    return OnboardingStep(
        id="credentials",
        title="Link trading API credentials",
        description="Upload encrypted API keys for the connected exchange.",
        completed=completed,
        completed_at=completed_at,
    )


def _governance_step(entries: Iterable[AuditLogEntry], account_id: str) -> OnboardingStep:
    completed_at: datetime | None = None
    for entry in entries:
        if entry.actor_id != account_id:
            continue
        if entry.action == "governance.acknowledge":
            completed_at = entry.created_at
    return OnboardingStep(
        id="governance",
        title="Review governance controls",
        description="Acknowledge policy, audit, and escalation procedures.",
        completed=completed_at is not None,
        completed_at=completed_at,
    )


def _build_onboarding_state(request: Request, account_id: str) -> OnboardingState:
    accounts_service = _get_accounts_service(request)
    audit_store = _get_audit_store(request)
    profile = accounts_service.get_profile(account_id)
    audit_entries = tuple(audit_store.all())
    steps = [
        _profile_step(profile),
        _credentials_step(profile),
        _governance_step(audit_entries, account_id),
    ]
    all_completed = all(step.completed for step in steps)
    return OnboardingState(account_id=account_id, steps=steps, all_completed=all_completed)


def _serialize_audit_entry(entry: AuditLogEntry) -> AuditLogEntryModel:
    return AuditLogEntryModel(
        id=str(entry.id),
        action=entry.action,
        actor=entry.actor_id,
        created_at=entry.created_at,
        before=dict(entry.before),
        after=dict(entry.after),
        correlation_id=entry.correlation_id,
    )


def _load_recent_trades(account_id: str, *, limit: int = 25) -> list[TradeEntryModel]:
    rows = read_trade_log(account_id=account_id)
    entries: list[TradeEntryModel] = []
    for row in rows:
        timestamp_raw = row.get("timestamp")
        timestamp = None
        if isinstance(timestamp_raw, str):
            try:
                timestamp = datetime.fromisoformat(timestamp_raw)
            except ValueError:
                timestamp = None
        if timestamp is None:
            continue
        try:
            quantity = float(row.get("quantity") or 0.0)
        except (TypeError, ValueError):
            quantity = 0.0
        try:
            price = float(row.get("price") or 0.0)
        except (TypeError, ValueError):
            price = 0.0
        pnl_value = row.get("pnl")
        pnl = None
        if pnl_value is not None:
            try:
                pnl = float(pnl_value)
            except (TypeError, ValueError):
                pnl = None
        entry = TradeEntryModel(
            timestamp=timestamp,
            symbol=str(row.get("symbol") or "").upper(),
            side=str(row.get("side") or "").lower() or "unknown",
            quantity=quantity,
            price=price,
            pnl=pnl,
            client_order_id=str(row.get("client_order_id") or ""),
            exchange_order_id=row.get("exchange_order_id") or None,
            transport=(row.get("transport") or None),
            simulated=str(row.get("simulated") or "").lower() == "true",
        )
        entries.append(entry)
    entries.sort(key=lambda item: item.timestamp, reverse=True)
    return entries[:limit]


async def _load_balances(request: Request, account_id: str) -> BalanceSnapshotModel | None:
    reader = _ensure_balance_reader(request)
    try:
        payload = await reader.get_account_balances(account_id)
    except BalanceRetrievalError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    if not isinstance(payload, Mapping):
        raise HTTPException(status_code=502, detail="Balance reader returned an invalid payload")

    def _to_float(value: Any) -> float | None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if math.isnan(number) or math.isinf(number):
            return None
        return number

    return BalanceSnapshotModel(
        available_usd=_to_float(payload.get("available_usd")),
        deployed_usd=_to_float(payload.get("deployed_usd")),
        total_nav_usd=_to_float(payload.get("total_nav_usd")),
        stablecoin_depeg_bps=_to_float(payload.get("stablecoin_depeg_bps")),
    )


async def _load_sim_status(account_id: str) -> SimulationStatusModel:
    status: SimModeStatus = await sim_mode_repository.get_status_async(account_id)
    return SimulationStatusModel(active=status.active, reason=status.reason, since=status.ts)


def _load_hedge_status(service: HedgeService) -> HedgeStatusModel:
    override = service.get_override()
    diagnostics = service.get_last_diagnostics()
    mode = "manual" if override else "auto"
    target: float | None = None
    if override:
        target = float(override.target_pct)
    elif diagnostics:
        payload = diagnostics.as_dict()
        adjusted = payload.get("auto_target_pct") or payload.get("adjusted_target_pct")
        try:
            target = float(adjusted) if adjusted is not None else None
        except (TypeError, ValueError):
            target = None
        diagnostics = payload  # type: ignore[assignment]
    diagnostics_payload: Mapping[str, Any] | None
    if diagnostics and isinstance(diagnostics, Mapping):
        diagnostics_payload = diagnostics
    elif diagnostics:
        diagnostics_payload = diagnostics.as_dict()  # type: ignore[assignment]
    else:
        diagnostics_payload = None
    override_payload: Mapping[str, Any] | None = None
    if override:
        override_payload = override.as_dict()
    return HedgeStatusModel(
        mode=mode,
        target_pct=target,
        override=override_payload,
        diagnostics=diagnostics_payload,
    )


def _parse_iso_timestamp(raw: str | None) -> datetime:
    if not raw:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _extract_error_detail(response_json: Any, fallback: str) -> str:
    if isinstance(response_json, dict):
        detail = response_json.get("detail")
        if isinstance(detail, str) and detail:
            return detail
    if isinstance(response_json, list) and response_json:
        first = response_json[0]
        if isinstance(first, dict):
            detail = first.get("msg") or first.get("detail")
            if isinstance(detail, str) and detail:
                return detail
        if isinstance(first, str) and first:
            return first
    return fallback


def _require_mfa_header(raw_header: str | None) -> str:
    value = (raw_header or "").strip()
    if not value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="X-MFA-Context header is required for secrets operations.",
        )
    return value


async def _call_secrets_service(
    method: str,
    path: str,
    *,
    account_id: str,
    mfa_context: str,
    payload: Mapping[str, Any] | None = None,
) -> Mapping[str, Any] | list[Any]:
    try:
        response = await request_json(
            method,
            path,
            account_id=account_id,
            mfa_context=mfa_context,
            json=payload,
        )
    except SecretsServiceConfigurationError as exc:
        _logger.error("Secrets service configuration error: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Secrets service configuration is incomplete.",
        ) from exc
    except SecretsServiceTransportError as exc:
        _logger.error("Secrets service transport failure", exc_info=exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unable to contact the secrets service.",
        ) from exc

    if response.status_code >= 400:
        try:
            payload_body = response.json()
        except ValueError:
            payload_body = response.text
        detail = (
            _extract_error_detail(
                payload_body,
                str(payload_body or "Secrets service request failed."),
            )
            if not isinstance(payload_body, str)
            else payload_body or "Secrets service request failed."
        )
        raise HTTPException(status_code=response.status_code, detail=detail)

    content_type = response.headers.get("content-type", "")
    if "application/json" not in content_type:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Secrets service returned an unexpected response type.",
        )

    try:
        return response.json()
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Secrets service returned invalid JSON.",
        ) from exc


@router.get("/onboarding", response_model=OnboardingState)
async def get_onboarding_state(
    request: Request,
    principal: AuthenticatedPrincipal = Depends(require_authenticated_principal),
    _: None = Depends(_require_https),
) -> OnboardingState:
    return _build_onboarding_state(request, principal.account_id)


@router.post("/onboarding/profile", response_model=OnboardingState, status_code=status.HTTP_200_OK)
async def update_profile(
    payload: ProfileUpdateRequest,
    request: Request,
    principal: AuthenticatedPrincipal = Depends(require_authenticated_principal),
    _: None = Depends(_require_https),
) -> OnboardingState:
    accounts_service = _get_accounts_service(request)
    existing = accounts_service.get_profile(principal.account_id)
    profile = AdminProfile(
        admin_id=principal.account_id,
        email=payload.email,
        display_name=payload.display_name,
        kraken_credentials_linked=existing.kraken_credentials_linked if existing else False,
        last_updated=datetime.now(timezone.utc),
    )
    accounts_service.upsert_profile(profile)
    return _build_onboarding_state(request, principal.account_id)


@router.post("/onboarding/credentials", response_model=OnboardingState)
async def mark_credentials_linked(
    payload: CredentialsStatusRequest,
    request: Request,
    principal: AuthenticatedPrincipal = Depends(require_authenticated_principal),
    _: None = Depends(_require_https),
) -> OnboardingState:
    accounts_service = _get_accounts_service(request)
    accounts_service.set_kraken_credentials_status(principal.account_id, payload.linked)
    return _build_onboarding_state(request, principal.account_id)


@router.post("/onboarding/governance", response_model=OnboardingState)
async def acknowledge_governance(
    payload: GovernanceAcknowledgeRequest,
    request: Request,
    principal: AuthenticatedPrincipal = Depends(require_authenticated_principal),
    _: None = Depends(_require_https),
) -> OnboardingState:
    if not payload.acknowledged:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Acknowledgement flag must be true")
    auditor = _get_auditor(request)
    auditor.record(
        action="governance.acknowledge",
        actor_id=principal.account_id,
        before=None,
        after={"acknowledged": True, "timestamp": datetime.now(timezone.utc).isoformat()},
    )
    return _build_onboarding_state(request, principal.account_id)


@router.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard(
    request: Request,
    principal: AuthenticatedPrincipal = Depends(require_authenticated_principal),
    _: None = Depends(_require_https),
    trade_limit: int = Query(25, ge=1, le=250, description="Maximum number of recent trades to return"),
) -> DashboardResponse:
    balances = await _load_balances(request, principal.account_id)
    simulation = await _load_sim_status(principal.account_id)
    hedge_status = _load_hedge_status(get_hedge_service())
    trades = _load_recent_trades(principal.account_id, limit=trade_limit)
    return DashboardResponse(
        account_id=principal.account_id,
        balances=balances,
        simulation=simulation,
        hedge=hedge_status,
        recent_trades=trades,
    )


@router.get("/logs", response_model=AuditLogResponse)
async def get_audit_logs(
    request: Request,
    principal: AuthenticatedPrincipal = Depends(require_authenticated_principal),
    _: None = Depends(_require_https),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of audit entries to return"),
) -> AuditLogResponse:
    audit_store = _get_audit_store(request)
    entries = [
        _serialize_audit_entry(entry)
        for entry in audit_store.all()
        if entry.actor_id == principal.account_id
    ]
    entries.sort(key=lambda item: item.created_at, reverse=True)
    return AuditLogResponse(account_id=principal.account_id, entries=entries[:limit])


@router.post("/credentials/api-key", response_model=ApiKeyUploadResponse, status_code=status.HTTP_200_OK)
async def upload_api_key(
    payload: ApiKeyUploadRequest,
    request: Request,
    principal: AuthenticatedPrincipal = Depends(require_authenticated_principal),
    _: str = Depends(require_mfa_context),
    __: None = Depends(_require_https),
) -> ApiKeyUploadResponse:
    from services.common.adapters import KrakenSecretManager  # local import to avoid heavy startup cost

    manager = KrakenSecretManager(account_id=principal.account_id)
    rotation = manager.rotate_credentials(api_key=payload.api_key, api_secret=payload.api_secret)
    metadata = rotation.get("metadata") or {}
    secret_name = str(metadata.get("secret_name") or manager.secret_name)
    rotated_at_raw = metadata.get("rotated_at")
    rotated_at: datetime
    if isinstance(rotated_at_raw, datetime):
        rotated_at = rotated_at_raw.astimezone(timezone.utc)
    elif isinstance(rotated_at_raw, str):
        try:
            parsed = datetime.fromisoformat(rotated_at_raw)
        except ValueError:
            parsed = datetime.now(timezone.utc)
        rotated_at = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    else:
        rotated_at = datetime.now(timezone.utc)
    auditor = _get_auditor(request)
    auditor.record(
        action="builder.api_key.rotate",
        actor_id=principal.account_id,
        before=rotation.get("before"),
        after={"secret_name": secret_name, "rotated_at": rotated_at.isoformat()},
    )
    accounts_service = _get_accounts_service(request)
    accounts_service.set_kraken_credentials_status(principal.account_id, True)
    return ApiKeyUploadResponse(account_id=principal.account_id, secret_name=secret_name, rotated_at=rotated_at)


@secrets_router.get("/status", response_model=SecretsStatusModel)
async def get_secrets_status(
    request: Request,
    principal: AuthenticatedPrincipal = Depends(require_authenticated_principal),
    _: str = Depends(require_mfa_context),
    mfa_header: str = Header(..., alias="X-MFA-Context"),
    __: None = Depends(_require_https),
) -> SecretsStatusModel:
    mfa_context = _require_mfa_header(mfa_header)
    payload = await _call_secrets_service(
        "GET",
        "/secrets/status",
        account_id=principal.account_id,
        mfa_context=mfa_context,
    )
    if not isinstance(payload, Mapping):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Secrets service returned an unexpected payload.",
        )
    last_rotated_at = payload.get("last_rotated_at")
    last_rotated_by = payload.get("last_rotated_by")
    status_message = payload.get("status")
    return SecretsStatusModel(
        last_rotated_at=str(last_rotated_at) if last_rotated_at is not None else None,
        last_rotated_by=str(last_rotated_by) if last_rotated_by is not None else None,
        status=str(status_message) if status_message is not None else None,
    )


@secrets_router.get("/audit", response_model=list[SecretsAuditEntryModel])
async def get_secrets_audit(
    request: Request,
    principal: AuthenticatedPrincipal = Depends(require_authenticated_principal),
    _: str = Depends(require_mfa_context),
    mfa_header: str = Header(..., alias="X-MFA-Context"),
    __: None = Depends(_require_https),
) -> list[SecretsAuditEntryModel]:
    mfa_context = _require_mfa_header(mfa_header)
    payload = await _call_secrets_service(
        "GET",
        "/secrets/audit",
        account_id=principal.account_id,
        mfa_context=mfa_context,
    )
    if not isinstance(payload, list):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Secrets service returned an unexpected payload.",
        )
    entries: list[SecretsAuditEntryModel] = []
    for item in payload:
        if not isinstance(item, Mapping):
            continue
        actor = str(item.get("actor") or principal.account_id)
        rotated_at_raw = item.get("rotated_at")
        rotated_at = (
            _parse_iso_timestamp(str(rotated_at_raw))
            if rotated_at_raw is not None
            else datetime.now(timezone.utc)
        )
        notes_value = item.get("notes")
        notes = str(notes_value) if isinstance(notes_value, str) else None
        entries.append(SecretsAuditEntryModel(actor=actor, rotated_at=rotated_at, notes=notes))
    return entries


@secrets_router.post("/rotate", response_model=SecretsRotateResponseModel, status_code=status.HTTP_200_OK)
async def rotate_secrets(
    payload: ApiKeyUploadRequest,
    request: Request,
    principal: AuthenticatedPrincipal = Depends(require_authenticated_principal),
    _: str = Depends(require_mfa_context),
    mfa_header: str = Header(..., alias="X-MFA-Context"),
    __: None = Depends(_require_https),
) -> SecretsRotateResponseModel:
    mfa_context = _require_mfa_header(mfa_header)
    rotation_payload = {
        "account_id": principal.account_id,
        "api_key": payload.api_key,
        "api_secret": payload.api_secret,
    }
    rotation = await _call_secrets_service(
        "POST",
        "/secrets/kraken",
        account_id=principal.account_id,
        mfa_context=mfa_context,
        payload=rotation_payload,
    )
    if not isinstance(rotation, Mapping):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Secrets service returned an unexpected payload.",
        )
    secret_name = str(rotation.get("secret_name") or "")
    rotated_at_raw = rotation.get("last_rotated")
    rotated_at = (
        _parse_iso_timestamp(str(rotated_at_raw))
        if rotated_at_raw is not None
        else datetime.now(timezone.utc)
    )

    auditor = _get_auditor(request)
    auditor.record(
        action="builder.api_key.rotate",
        actor_id=principal.account_id,
        before=None,
        after={"secret_name": secret_name, "rotated_at": rotated_at.isoformat()},
    )
    accounts_service = _get_accounts_service(request)
    accounts_service.set_kraken_credentials_status(principal.account_id, True)

    return SecretsRotateResponseModel(secret_name=secret_name, last_rotated=rotated_at)
