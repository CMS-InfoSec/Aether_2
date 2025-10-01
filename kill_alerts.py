"""Notification helpers for kill switch activations."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Sequence

logger = logging.getLogger(__name__)

_SENT_EMAILS: List[Dict[str, object]] = []
_SENT_SMS: List[Dict[str, object]] = []
_SENT_WEBHOOKS: List[Dict[str, object]] = []


def reset_notifications() -> None:
    """Clear previously recorded notifications.

    The notifier functions append payloads to in-memory collections so tests can
    assert behaviour without requiring real network calls. Resetting ensures a
    clean slate between invocations.
    """

    _SENT_EMAILS.clear()
    _SENT_SMS.clear()
    _SENT_WEBHOOKS.clear()


def email_notifications() -> List[Dict[str, object]]:
    """Return a copy of email notifications that have been dispatched."""

    return list(_SENT_EMAILS)


def sms_notifications() -> List[Dict[str, object]]:
    """Return a copy of SMS notifications that have been dispatched."""

    return list(_SENT_SMS)


def webhook_notifications() -> List[Dict[str, object]]:
    """Return a copy of webhook notifications that have been dispatched."""

    return list(_SENT_WEBHOOKS)


def _build_payload(account_id: str, reason_code: str, triggered_at: datetime) -> Dict[str, object]:
    return {
        "account_id": account_id,
        "reason_code": reason_code,
        "triggered_at": triggered_at.isoformat(),
        "summary": f"Kill switch triggered for account '{account_id}'",
    }


def _send_email(payload: Dict[str, object]) -> None:
    message = {
        "to": "risk-ops@example.com",
        "subject": "Kill switch engaged",
        "body": (
            "Kill switch engaged for account {account_id} due to {reason_code}."
        ).format(
            account_id=payload["account_id"], reason_code=payload["reason_code"]
        ),
        "payload": payload,
    }
    _SENT_EMAILS.append(message)


def _send_sms(payload: Dict[str, object]) -> None:
    message = {
        "to": "+15555550100",
        "body": (
            "Kill switch engaged: {account_id} ({reason_code})"
        ).format(
            account_id=payload["account_id"], reason_code=payload["reason_code"]
        ),
        "payload": payload,
    }
    _SENT_SMS.append(message)


def _send_webhook(payload: Dict[str, object]) -> None:
    message = {
        "url": "https://hooks.internal/kill-switch",
        "payload": payload,
    }
    _SENT_WEBHOOKS.append(message)


def dispatch_notifications(
    *,
    account_id: str,
    reason_code: str,
    triggered_at: datetime,
    extra_metadata: Dict[str, object] | None = None,
) -> List[str]:
    """Dispatch kill switch notifications across email, SMS, and webhook.

    Returns the list of channels for which dispatch succeeded.
    """

    payload = _build_payload(account_id, reason_code, triggered_at)
    if extra_metadata:
        payload.update(extra_metadata)

    channels: Sequence[tuple[str, Callable[[Dict[str, object]], None]]] = (
        ("email", _send_email),
        ("sms", _send_sms),
        ("webhook", _send_webhook),
    )

    delivered: List[str] = []
    for name, handler in channels:
        try:
            handler(payload)
        except Exception:  # pragma: no cover - defensive logging only
            logger.exception("Failed to dispatch kill switch notification", extra={"channel": name})
            continue
        delivered.append(name)

    return delivered


def notification_summary() -> Dict[str, Iterable[Dict[str, object]]]:
    """Return a snapshot of delivered notifications by channel."""

    return {
        "email": email_notifications(),
        "sms": sms_notifications(),
        "webhook": webhook_notifications(),
    }

