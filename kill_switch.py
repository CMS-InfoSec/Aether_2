"""FastAPI endpoint for triggering an immediate trading kill switch."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import Depends, FastAPI, HTTPException, Query, status

from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.common.security import require_admin_account

app = FastAPI(title="Kill Switch Service")


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
    account_id: str = Query(..., min_length=1),
    actor_account: str = Depends(require_admin_account),
) -> Dict[str, Any]:
    """Trigger the kill switch for the provided account."""

    normalized_account = _normalize_account(account_id)

    activation_ts = datetime.now(timezone.utc)

    timescale = TimescaleAdapter(account_id=normalized_account)
    timescale.set_kill_switch(
        engaged=True,
        reason="Manual kill switch activation",
        actor=actor_account,
    )

    kafka = KafkaNATSAdapter(account_id=normalized_account)
    kafka.publish(
        topic="risk.events",
        payload={
            "event": "KILL_SWITCH_TRIGGERED",
            "account_id": normalized_account,
            "actor": actor_account,
            "timestamp": activation_ts.isoformat(),
            "actions": ["CANCEL_OPEN_ORDERS", "FLATTEN_POSITIONS"],
        },
    )

    return {"status": "ok", "ts": activation_ts.isoformat()}
