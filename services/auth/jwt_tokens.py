"""Helpers for issuing JWTs used by the authentication service."""

from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Tuple


def _b64url(data: bytes) -> str:
    """Return base64-url encoded data without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _sign(data: bytes, secret: str) -> str:
    import hashlib
    import hmac

    digest = hmac.new(secret.encode("utf-8"), data, hashlib.sha256).digest()
    return _b64url(digest)


def create_jwt(
    *,
    subject: str,
    role: Optional[str] = None,
    claims: Optional[Mapping[str, Any]] = None,
    ttl_seconds: Optional[int] = None,
    secret: Optional[str] = None,
) -> Tuple[str, datetime]:
    """Create a signed JWT for the given subject.

    ``role`` can be provided explicitly or embedded within the optional ``claims``
    mapping.  At least one of those two mechanisms must supply the role claim so
    downstream services can enforce authorization policies.  The token TTL can be
    overridden either via the ``ttl_seconds`` argument or the
    ``AUTH_JWT_TTL_SECONDS`` environment variable.
    """

    signing_secret = secret or os.getenv("AUTH_JWT_SECRET")
    if not signing_secret:
        raise ValueError("AUTH_JWT_SECRET environment variable must be set")

    ttl = ttl_seconds or int(os.getenv("AUTH_JWT_TTL_SECONDS", "3600"))
    now = datetime.now(timezone.utc)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=ttl)).timestamp()),
    }

    if claims:
        for key, value in claims.items():
            if key in {"sub", "iat", "exp"}:
                continue
            payload[key] = value

    if role is not None:
        payload["role"] = role

    if "role" not in payload:
        raise ValueError("JWT role claim must be provided via the role argument or claims mapping")
    header = {"alg": "HS256", "typ": "JWT"}

    header_b64 = _b64url(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    signature = _sign(signing_input, signing_secret)
    token = f"{header_b64}.{payload_b64}.{signature}"
    return token, now + timedelta(seconds=ttl)
