"""FastAPI endpoint for triggering an immediate trading kill switch."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse, Response

from kill_alerts import NotificationDispatchError, dispatch_notifications
from metrics import CONTENT_TYPE_LATEST, Histogram, _REGISTRY, generate_latest
from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.common.security import require_admin_account
from shared.async_utils import dispatch_async
from shared.audit_hooks import AuditEvent, load_audit_hooks

app = FastAPI(title="Kill Switch Service")


LOGGER = logging.getLogger("kill_switch")


KILL_SWITCH_RESPONSE_SECONDS = Histogram(
    "kill_switch_response_seconds",
    "Duration of kill-switch responses in seconds.",
    ["status"],
    buckets=(5.0, 10.0, 20.0, 30.0, 45.0, 60.0, 90.0),
    registry=_REGISTRY,
)


class KillSwitchReason(str, Enum):
    SPREAD_WIDENING = "spread_widening"
    LATENCY_STALL = "latency_stall"
    LOSS_CAP_BREACH = "loss_cap_breach"


_REASON_DESCRIPTIONS = {
    KillSwitchReason.SPREAD_WIDENING: "Spread widening beyond configured tolerance",
    KillSwitchReason.LATENCY_STALL: "Order gateway latency stalled",
    KillSwitchReason.LOSS_CAP_BREACH: "Daily loss cap breached",
}


def _normalize_account(account_id: str) -> str:
    """Normalize account identifiers to canonical form."""

    normalized = account_id.strip().lower()
    if not normalized:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Account identifier must not be empty.",
        )
    return normalized


@app.post("/risk/kill")
def trigger_kill_switch(
    request: Request,
    account_id: str = Query(..., min_length=1),
    reason_code: KillSwitchReason = Query(KillSwitchReason.LOSS_CAP_BREACH),
    actor_account: str = Depends(require_admin_account),
) -> Dict[str, Any]:
    """Trigger the kill switch for the provided account."""
    start_time = time.perf_counter()
    outcome_label = "error"
    try:
        normalized_account = _normalize_account(account_id)

        activation_ts = datetime.now(timezone.utc)
        reason_description = _REASON_DESCRIPTIONS.get(
            reason_code, "Kill switch engaged"
        )

        timescale = TimescaleAdapter(account_id=normalized_account)
        timescale.set_kill_switch(
            engaged=True,
            reason=reason_description,
            actor=actor_account,
        )

        kafka = KafkaNATSAdapter(account_id=normalized_account)
        dispatch_async(
            kafka.publish(
                topic="risk.events",
                payload={
                    "event": "KILL_SWITCH_TRIGGERED",
                    "account_id": normalized_account,
                    "actor": actor_account,
                    "timestamp": activation_ts.isoformat(),
                    "actions": ["CANCEL_OPEN_ORDERS", "FLATTEN_POSITIONS"],
                    "reason_code": reason_code.value,
                },
            ),
            context="kill_switch.broadcast",
            logger=LOGGER,
        )

        response_status = "ok"
        http_status = status.HTTP_200_OK
        failed_channels: List[str] = []
        try:
            channels_sent: List[str] = dispatch_notifications(
                account_id=normalized_account,
                reason_code=reason_code.value,
                triggered_at=activation_ts,
                extra_metadata={"actor": actor_account},
            )
        except NotificationDispatchError as exc:
            channels_sent = list(exc.delivered)
            failed_channels = sorted(exc.failed.keys())
            response_status = "partial"
            http_status = status.HTTP_207_MULTI_STATUS
            LOGGER.error(
                "Kill switch notifications partially delivered",
                extra={"delivered": channels_sent, "failed": failed_channels},
            )

        timescale.record_kill_event(
            reason_code=reason_code.value,
            triggered_at=activation_ts,
            channels_sent=channels_sent,
        )

        audit_hooks = load_audit_hooks()
        event = AuditEvent(
            actor=actor_account,
            action="kill_switch.triggered",
            entity=normalized_account,
            before={},
            after={
                "reason_code": reason_code.value,
                "reason": reason_description,
                "channels_sent": channels_sent,
                "failed_channels": failed_channels,
                "triggered_at": activation_ts.isoformat(),
            },
            ip_address=request.client.host if request.client else None,
        )
        event.log_with_fallback(
            audit_hooks,
            LOGGER,
            failure_message=(
                "Failed to record audit log for kill switch activation on"
                f" {normalized_account}"
            ),
            disabled_message=(
                "Audit logging disabled; skipping kill switch event for"
                f" {normalized_account}"
            ),
        )

        response_body = {
            "status": response_status,
            "ts": activation_ts.isoformat(),
            "reason_code": reason_code.value,
            "channels_sent": channels_sent,
            "failed_channels": failed_channels,
        }

        outcome_label = response_status

        if http_status != status.HTTP_200_OK:
            return JSONResponse(status_code=http_status, content=response_body)

        return response_body
    finally:
        duration = time.perf_counter() - start_time
        try:
            KILL_SWITCH_RESPONSE_SECONDS.labels(status=outcome_label).observe(duration)
        except Exception:  # pragma: no cover - defensive guard for optional deps
            LOGGER.debug("Prometheus histogram unavailable; skipping metric update")


@app.get("/risk/kill_events")
def list_kill_events(
    account_id: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=20, ge=1, le=100),
    actor_account: str = Depends(require_admin_account),
) -> List[Dict[str, Any]]:
    """Return recent kill switch events for the organisation."""

    _ = actor_account  # ensure dependency is enforced without lint noise

    normalized: str | None = None
    if account_id is not None:
        normalized = _normalize_account(account_id)

    events = TimescaleAdapter.all_kill_events(account_id=normalized, limit=limit)

    response: List[Dict[str, Any]] = []
    for event in events:
        response.append(
            {
                "account_id": event["account_id"],
                "reason": event["reason"],
                "ts": event["ts"].isoformat(),
                "channels_sent": list(event.get("channels_sent", [])),
            }
        )
    return response


@app.get("/metrics")
def metrics() -> Response:
    """Expose Prometheus metrics for the kill-switch service."""

    payload = generate_latest(_REGISTRY)
    return Response(content=payload, media_type=CONTENT_TYPE_LATEST)
