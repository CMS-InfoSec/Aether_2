import base64
import json
from datetime import datetime, timezone

import pytest

from services.auth.jwt_tokens import create_jwt


def _decode_segment(segment: str) -> dict:
    padding = "=" * (-len(segment) % 4)
    decoded = base64.urlsafe_b64decode(segment + padding)
    return json.loads(decoded)


def test_create_jwt_structure_and_claims(monkeypatch):
    monkeypatch.setenv("AUTH_JWT_SECRET", "super-secret-key")
    monkeypatch.setenv("AUTH_JWT_TTL_SECONDS", "120")

    token, expires_at = create_jwt(subject="user-123", role="admin")

    parts = token.split(".")
    assert len(parts) == 3, "JWT should contain header, payload, and signature"

    header = _decode_segment(parts[0])
    payload = _decode_segment(parts[1])

    assert header["alg"] == "HS256"
    assert header["typ"] == "JWT"

    assert payload["sub"] == "user-123"
    assert payload["role"] == "admin"

    ttl = payload["exp"] - payload["iat"]
    assert ttl == 120

    expected_exp = datetime.fromtimestamp(payload["exp"], tz=timezone.utc)
    assert abs((expires_at - expected_exp).total_seconds()) <= 1


def test_create_jwt_missing_secret(monkeypatch):
    monkeypatch.delenv("AUTH_JWT_SECRET", raising=False)
    monkeypatch.setenv("AUTH_JWT_TTL_SECONDS", "60")

    with pytest.raises(ValueError):
        create_jwt(subject="user-456", role="auditor")


def test_create_jwt_accepts_claim_mapping(monkeypatch):
    monkeypatch.setenv("AUTH_JWT_SECRET", "super-secret-key")
    extra_claims = {"role": "auditor", "permissions": ["read"]}

    token, _ = create_jwt(subject="user-789", claims=extra_claims)

    parts = token.split(".")
    payload = _decode_segment(parts[1])

    assert payload["role"] == "auditor"
    assert payload["permissions"] == ["read"]
