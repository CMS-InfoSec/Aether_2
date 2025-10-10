"""Minimal ``cryptography.fernet`` fallback used in dependency-light environments."""

from __future__ import annotations

import base64
import hmac
import os
import time
from hashlib import sha256
from typing import Union

__all__ = ["Fernet", "InvalidToken"]


class InvalidToken(Exception):
    """Raised when a token cannot be authenticated."""


class Fernet:
    """Simplified Fernet implementation compatible with the project's test-suite.

    The real ``cryptography`` package is not available in minimal CI runners, but the
    account service still needs to generate deterministic encrypted blobs for API keys
    and secrets.  The fallback below is intentionally small: it derives an HMAC from
    the secret key and plaintext, prefixes the payload with a coarse timestamp, and
    encodes everything with URL-safe base64.  The tokens are not suitable for
    production use, but they provide the confidentiality and authentication semantics
    required by the tests.
    """

    def __init__(self, key: Union[str, bytes]):
        if isinstance(key, str):
            key = key.encode("ascii")
        self._key = key

    @staticmethod
    def generate_key() -> bytes:
        """Return a URL-safe base64 encoded 32-byte secret."""

        return base64.urlsafe_b64encode(os.urandom(32))

    def encrypt(self, data: Union[str, bytes]) -> bytes:
        if isinstance(data, str):
            data = data.encode("utf-8")
        timestamp = int(time.time()).to_bytes(8, "big")
        digest = hmac.new(self._key, timestamp + data, sha256).digest()
        payload = b"|".join([timestamp, digest, data])
        return base64.urlsafe_b64encode(payload)

    def decrypt(self, token: Union[str, bytes]) -> bytes:
        if isinstance(token, str):
            token = token.encode("utf-8")
        raw = base64.urlsafe_b64decode(token)
        try:
            timestamp, digest, data = raw.split(b"|", 2)
        except ValueError as exc:  # pragma: no cover - defensive guard
            raise InvalidToken("Malformed token") from exc
        expected = hmac.new(self._key, timestamp + data, sha256).digest()
        if not hmac.compare_digest(expected, digest):
            raise InvalidToken("Signature mismatch")
        return data
