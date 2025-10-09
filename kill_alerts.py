"""Notification helpers for kill switch activations."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, MutableMapping, Sequence
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

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


@dataclass
class _FallbackResponse:
    status_code: int
    text: str


def _perform_http_post(
    url: str,
    *,
    headers: MutableMapping[str, str] | None = None,
    json_body: Dict[str, Any] | None = None,
    data: Mapping[str, Any] | bytes | None = None,
    auth: tuple[str, str] | None = None,
    timeout: float = 10.0,
) -> Any:
    """Send a POST request using :mod:`requests` or a urllib fallback."""

    if _REQUESTS_MODULE is not None:
        kwargs: Dict[str, Any] = {
            "headers": headers,
            "timeout": timeout,
        }
        if json_body is not None:
            kwargs["json"] = json_body
        if isinstance(data, bytes):
            kwargs["data"] = data
        elif data is not None:
            kwargs["data"] = data
        if auth is not None:
            kwargs["auth"] = auth
        return _REQUESTS_MODULE.post(url, **kwargs)

    request_headers: Dict[str, str] = dict(headers or {})
    body: bytes
    if json_body is not None:
        body = json.dumps(json_body).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    elif isinstance(data, bytes):
        body = data
    elif data is not None:
        body = urllib_parse.urlencode(dict(data)).encode("utf-8")
        request_headers.setdefault(
            "Content-Type", "application/x-www-form-urlencoded"
        )
    else:
        body = b""

    if auth is not None:
        user, password = auth
        token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        request_headers.setdefault("Authorization", f"Basic {token}")

    request_obj = urllib_request.Request(url, data=body, headers=request_headers, method="POST")

    try:
        with urllib_request.urlopen(request_obj, timeout=timeout) as response:
            payload = response.read().decode("utf-8", errors="replace")
            status = getattr(response, "status", response.getcode())
    except urllib_error.HTTPError as exc:  # pragma: no cover - exercised in failure tests
        payload = exc.read().decode("utf-8", errors="replace")
        status = exc.code
    except Exception as exc:  # pragma: no cover - network errors handled at runtime
        raise RuntimeError(f"HTTP POST to {url} failed: {exc}") from exc

    return _FallbackResponse(status_code=status, text=payload)


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

    response = _perform_http_post(
        endpoint,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json_body=body,
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

    response = _perform_http_post(
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

    response = _perform_http_post(
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

