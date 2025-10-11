"""Helper utilities for invoking the Kraken Secrets microservice."""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, Mapping

import httpx

__all__ = [
    "SecretsServiceError",
    "SecretsServiceConfigurationError",
    "SecretsServiceTransportError",
    "SecretsServiceConfig",
    "resolve_account_token",
    "request_json",
]


class SecretsServiceError(RuntimeError):
    """Base error for secrets service integration issues."""


class SecretsServiceConfigurationError(SecretsServiceError):
    """Raised when required configuration for the secrets service is missing."""


class SecretsServiceTransportError(SecretsServiceError):
    """Raised when the HTTP call to the secrets service fails."""


@dataclass(frozen=True)
class SecretsServiceConfig:
    """Resolved configuration used to connect to the secrets microservice."""

    base_url: str
    timeout: float
    account_tokens: Mapping[str, str]


def _parse_account_token_mapping(raw: str) -> Dict[str, str]:
    """Parse an account-token mapping string into a dictionary."""

    mapping: Dict[str, str] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        account, _, token = entry.partition("=")
        if not _:
            # Skip malformed entries that do not use the key=value format.
            continue
        account_key = account.strip().lower()
        token_value = token.strip()
        if account_key and token_value:
            mapping[account_key] = token_value
    return mapping


def _invert_token_labels(raw: str) -> Dict[str, str]:
    """Invert a token:label specification into a label->token mapping."""

    mapping: Dict[str, str] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        token, _, label = entry.partition(":")
        if not _:
            continue
        token_value = token.strip()
        label_key = label.strip().lower()
        if token_value and label_key:
            mapping[label_key] = token_value
    return mapping


@lru_cache(maxsize=1)
def load_config() -> SecretsServiceConfig:
    """Resolve and cache configuration for the secrets microservice."""

    base_url = os.getenv("SECRETS_SERVICE_URL", "http://secrets-service").strip()
    if not base_url:
        raise SecretsServiceConfigurationError(
            "SECRETS_SERVICE_URL environment variable must be configured"
        )

    timeout_raw = os.getenv("SECRETS_SERVICE_TIMEOUT", "10.0").strip()
    try:
        timeout = float(timeout_raw)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise SecretsServiceConfigurationError(
            "SECRETS_SERVICE_TIMEOUT must be a numeric value"
        ) from exc

    tokens = _parse_account_token_mapping(
        os.getenv("SECRETS_SERVICE_ACCOUNT_TOKENS", "")
    )
    if not tokens:
        tokens = _invert_token_labels(os.getenv("KRAKEN_SECRETS_AUTH_TOKENS", ""))

    if not tokens:
        raise SecretsServiceConfigurationError(
            "No secrets service tokens configured. Set SECRETS_SERVICE_ACCOUNT_TOKENS "
            "or provide labelled tokens via KRAKEN_SECRETS_AUTH_TOKENS."
        )

    return SecretsServiceConfig(base_url=base_url.rstrip("/"), timeout=timeout, account_tokens=tokens)


def resolve_account_token(account_id: str, *, config: SecretsServiceConfig | None = None) -> str:
    """Resolve the secrets service token for an account identifier."""

    if not account_id:
        raise SecretsServiceConfigurationError("Account identifier must be provided")

    cfg = config or load_config()
    token = cfg.account_tokens.get(account_id.strip().lower())
    if not token:
        raise SecretsServiceConfigurationError(
            f"No secrets service token configured for account '{account_id}'."
        )
    return token


async def request_json(
    method: str,
    path: str,
    *,
    account_id: str,
    mfa_context: str,
    json: Mapping[str, Any] | None = None,
    config: SecretsServiceConfig | None = None,
) -> httpx.Response:
    """Execute a request against the secrets microservice."""

    cfg = config or load_config()
    token = resolve_account_token(account_id, config=cfg)

    if not mfa_context.strip():
        raise SecretsServiceConfigurationError("MFA context token is required for secrets requests")

    headers = {
        "Authorization": f"Bearer {token}",
        "X-MFA-Token": mfa_context.strip(),
        "Accept": "application/json",
    }

    url_path = path if path.startswith("/") else f"/{path}"

    try:
        async with httpx.AsyncClient(base_url=cfg.base_url, timeout=cfg.timeout) as client:
            response = await client.request(method.upper(), url_path, headers=headers, json=json)
    except httpx.RequestError as exc:
        raise SecretsServiceTransportError("Failed to contact the secrets service") from exc

    return response
