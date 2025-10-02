"""Helpers for issuing JWTs used by the authentication service."""

from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple


def _b64url(data: bytes) -> str:
    """Return base64-url encoded data without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _sign(data: bytes, secret: str) -> str:
    import hashlib
    import hmac

    digest = hmac.new(secret.encode("utf-8"), data, hashlib.sha256).digest()
    return _b64url(digest)


def create_jwt(*, subject: str, ttl_seconds: Optional[int] = None) -> Tuple[str, datetime]:
    """Create a signed JWT for the given subject.

    The JWT role claim is hard-coded to ``admin`` to align with existing downstream
    expectations.  The token TTL can be overridden either via the ``ttl_seconds``
    argument or the ``AUTH_JWT_TTL_SECONDS`` environment variable.
    """

    secret = os.getenv("AUTH_JWT_SECRET")
    if not secret:
        raise ValueError("AUTH_JWT_SECRET environment variable must be set")

    ttl = ttl_seconds or int(os.getenv("AUTH_JWT_TTL_SECONDS", "3600"))
    now = datetime.now(timezone.utc)
    payload = {
        "sub": subject,
        "role": "admin",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=ttl)).timestamp()),
    }
    header = {"alg": "HS256", "typ": "JWT"}

    header_b64 = _b64url(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    signature = _sign(signing_input, secret)
    token = f"{header_b64}.{payload_b64}.{signature}"
    return token, now + timedelta(seconds=ttl)
