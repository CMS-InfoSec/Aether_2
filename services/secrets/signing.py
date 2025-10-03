"""Kraken request signing utilities shared by services."""

from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Any, Dict, Tuple
from urllib.parse import urlencode


def sign_kraken_request(path: str, data: Dict[str, Any], api_secret: str) -> Tuple[str, str]:
    """Return the encoded payload and HMAC signature for a Kraken request."""

    post_data = urlencode(data)
    nonce = str(data.get("nonce", ""))
    encoded = (nonce + post_data).encode()
    message = hashlib.sha256(encoded).digest()
    secret_key = base64.b64decode(api_secret)
    mac = hmac.new(secret_key, path.encode() + message, hashlib.sha512)
    signature = base64.b64encode(mac.digest()).decode()
    return post_data, signature


__all__ = ["sign_kraken_request"]
