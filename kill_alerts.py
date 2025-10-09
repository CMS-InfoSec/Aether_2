"""Notification helpers for kill switch activations."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Sequence

try:  # pragma: no cover - optional dependency import
    import requests as _REQUESTS_MODULE  # type: ignore[import-not-found]
except Exception as exc:  # pragma: no cover - exercised via optional-dependency tests
    _REQUESTS_MODULE = None
    _REQUESTS_IMPORT_ERROR = exc
else:  # pragma: no cover - trivial
    _REQUESTS_IMPORT_ERROR = None

logger = logging.getLogger(__name__)


class NotificationDispatchError(RuntimeError):
    """Raised when one or more notification channels fail to deliver."""

    def __init__(self, delivered: Sequence[str], failures: Mapping[str, Exception]):
        self.delivered = list(delivered)
        self.failed = dict(failures)
        failure_list = ", ".join(f"{name}: {exc}" for name, exc in self.failed.items()) or "unknown"
        super().__init__(f"Failed to dispatch notifications for: {failure_list}")


class MissingDependencyError(RuntimeError):
    """Raised when required optional dependencies are unavailable."""


def _require_requests() -> Any:
    """Return the :mod:`requests` module or raise a helpful error."""

    if _REQUESTS_MODULE is None:
        message = "requests is required for kill switch notifications"
        raise MissingDependencyError(message) from _REQUESTS_IMPORT_ERROR
    return _REQUESTS_MODULE


def _require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise RuntimeError(
            f"Environment variable '{key}' is required for kill switch notifications"
        )
    return value


def _build_payload(account_id: str, reason_code: str, triggered_at: datetime) -> Dict[str, object]:
    return {
        "account_id": account_id,
        "reason_code": reason_code,
        "triggered_at": triggered_at.isoformat(),
        "summary": f"Kill switch triggered for account '{account_id}'",
    }


def _send_email(payload: Dict[str, object]) -> None:
    """Send kill switch notification email via SendGrid-compatible API."""

    api_key = _require_env("KILL_ALERT_EMAIL_API_KEY")
    sender = _require_env("KILL_ALERT_EMAIL_FROM")
    recipients = _require_env("KILL_ALERT_EMAIL_TO")
    endpoint = os.getenv("KILL_ALERT_EMAIL_ENDPOINT", "https://api.sendgrid.com/v3/mail/send")

    body = {
        "personalizations": [
            {
                "to": [
                    {"email": recipient.strip()}
                    for recipient in recipients.split(",")
                    if recipient.strip()
                ],
            }
        ],
        "from": {"email": sender},
        "subject": "Kill switch engaged",
        "content": [
            {
                "type": "text/plain",
                "value": (
                    "Kill switch engaged for account {account_id} due to {reason_code}."
                ).format(
                    account_id=payload["account_id"],
                    reason_code=payload["reason_code"],
                ),
            }
        ],
        "custom_args": payload,
    }

    requests_module = _require_requests()
    response = requests_module.post(
        endpoint,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=body,
        timeout=10,
    )
    if response.status_code >= 400:
        raise RuntimeError(
            f"Email notification failed with status {response.status_code}: {response.text}"
        )


def _send_sms(payload: Dict[str, object]) -> None:
    """Send kill switch notification SMS via Twilio-compatible API."""

    account_sid = _require_env("KILL_ALERT_SMS_ACCOUNT_SID")
    auth_token = _require_env("KILL_ALERT_SMS_AUTH_TOKEN")
    from_number = _require_env("KILL_ALERT_SMS_FROM")
    to_number = _require_env("KILL_ALERT_SMS_TO")
    endpoint = os.getenv(
        "KILL_ALERT_SMS_ENDPOINT",
        f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
    )

    message_body = "Kill switch engaged: {account_id} ({reason_code})".format(
        account_id=payload["account_id"],
        reason_code=payload["reason_code"],
    )

    requests_module = _require_requests()
    response = requests_module.post(
        endpoint,
        data={
            "To": to_number,
            "From": from_number,
            "Body": message_body,
        },
        auth=(account_sid, auth_token),
        timeout=10,
    )
    if response.status_code >= 400:
        raise RuntimeError(
            f"SMS notification failed with status {response.status_code}: {response.text}"
        )


def _send_webhook(payload: Dict[str, object]) -> None:
    """Send kill switch notification to an internal signed webhook."""

    url = _require_env("KILL_ALERT_WEBHOOK_URL")
    secret = _require_env("KILL_ALERT_WEBHOOK_SECRET")

    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()

    requests_module = _require_requests()
    response = requests_module.post(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-Signature": signature,
        },
        timeout=5,
    )
    if response.status_code >= 400:
        raise RuntimeError(
            f"Webhook notification failed with status {response.status_code}: {response.text}"
        )


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
    failures: Dict[str, Exception] = {}
    for name, handler in channels:
        try:
            handler(payload)
        except Exception as exc:  # pragma: no cover - logged and aggregated
            failures[name] = exc
            logger.exception(
                "Failed to dispatch kill switch notification", extra={"channel": name}
            )
        else:
            delivered.append(name)

    if failures:
        raise NotificationDispatchError(delivered=delivered, failures=failures)

    return delivered

