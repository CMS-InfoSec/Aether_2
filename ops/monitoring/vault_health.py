"""Vault health monitoring job that posts alerts to the Ops channel."""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from typing import Any
from urllib import error, request

LOGGER = logging.getLogger(__name__)


class VaultHealthError(RuntimeError):
    """Raised when the Vault health endpoint indicates a problem."""


@dataclass
class VaultHealthConfig:
    """Configuration for the Vault health check."""

    address: str
    token: str | None = None
    slack_webhook: str | None = None
    timeout_seconds: float = 5.0


@dataclass
class HttpResponse:
    """Simple HTTP response container."""

    status_code: int
    body: bytes
    headers: dict[str, str]


@dataclass
class VaultHealthStatus:
    """Represents the parsed health response."""

    is_healthy: bool
    sealed: bool | None
    status_code: int
    message: str


def _build_headers(config: VaultHealthConfig) -> dict[str, str]:
    headers: dict[str, str] = {"Accept": "application/json"}
    if config.token:
        headers["X-Vault-Token"] = config.token
    return headers


def fetch_health(config: VaultHealthConfig) -> HttpResponse:
    """Request the Vault health endpoint."""

    url = config.address.rstrip("/") + "/v1/sys/health"
    LOGGER.debug("Querying Vault health endpoint", extra={"url": url})
    req = request.Request(url, headers=_build_headers(config))
    try:
        with request.urlopen(req, timeout=config.timeout_seconds) as resp:
            body = resp.read()
            status = resp.getcode() or 0
            headers = dict(resp.headers.items())
    except error.URLError as exc:  # pragma: no cover - network failure path
        raise VaultHealthError(f"Vault health endpoint unreachable: {exc}") from exc

    return HttpResponse(status_code=status, body=body, headers=headers)


def parse_health(response: HttpResponse) -> VaultHealthStatus:
    """Interpret the Vault health response."""

    sealed: bool | None = None
    message: str = ""
    try:
        text = response.body.decode("utf-8") or "{}"
        payload: Any = json.loads(text)
        if isinstance(payload, dict):
            sealed = payload.get("sealed")
            if payload.get("initialized") is False:
                message = "Vault is not initialised"
            elif sealed is True:
                message = "Vault is sealed"
            elif payload.get("standby"):
                message = "Vault is in standby mode"
            elif payload.get("performance_standby"):
                message = "Vault is in performance standby"
            else:
                message = payload.get("message") or "Vault is active"
        else:
            message = "Unexpected payload type from health endpoint"
    except json.JSONDecodeError:
        message = "Non-JSON response from Vault health endpoint"

    status_code = response.status_code

    healthy = status_code == 200 and sealed is False
    if sealed is True:
        healthy = False
    elif status_code in {429, 472, 473} and sealed is False:
        healthy = True
    elif status_code >= 400:
        healthy = False

    return VaultHealthStatus(is_healthy=healthy, sealed=sealed, status_code=status_code, message=message)


def _send_slack_message(webhook: str | None, text: str, emoji: str) -> None:
    if not webhook:
        LOGGER.warning("Slack webhook not configured; skipping notification")
        return

    payload = json.dumps({"text": f"{emoji} {text}"}).encode("utf-8")
    req = request.Request(webhook, data=payload, headers={"Content-Type": "application/json"})
    try:
        with request.urlopen(req, timeout=5):
            pass
    except error.URLError:
        LOGGER.exception("Failed to post Vault health alert to Slack")


def check_vault_health(config: VaultHealthConfig) -> VaultHealthStatus:
    """Fetch and evaluate Vault health, emitting alerts when unhealthy."""

    try:
        response = fetch_health(config)
    except VaultHealthError as exc:
        message = str(exc)
        LOGGER.error(message)
        _send_slack_message(config.slack_webhook, message, ":rotating_light:")
        raise

    status = parse_health(response)
    if status.is_healthy:
        LOGGER.info("Vault health check passed", extra={"status_code": status.status_code})
        return status

    error_message = f"Vault health check failed (status={status.status_code}): {status.message}"
    LOGGER.error(error_message)
    _send_slack_message(config.slack_webhook, error_message, ":rotating_light:")
    raise VaultHealthError(error_message)


def _load_config_from_env() -> VaultHealthConfig:
    address = os.environ.get("VAULT_ADDR")
    if not address:
        raise SystemExit("VAULT_ADDR environment variable must be set")

    token = os.environ.get("VAULT_TOKEN")
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    timeout = float(os.environ.get("VAULT_STATUS_TIMEOUT_SECONDS", "5"))
    return VaultHealthConfig(address=address, token=token, slack_webhook=webhook, timeout_seconds=timeout)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    config = _load_config_from_env()
    try:
        status = check_vault_health(config)
    except VaultHealthError:
        sys.exit(1)

    LOGGER.info("Vault status: %s", status.message)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
