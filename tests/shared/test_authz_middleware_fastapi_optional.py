import asyncio
import base64
import builtins
import hashlib
import hmac
import importlib
import json
import sys
from types import SimpleNamespace
from typing import Iterable

import pytest


def _purge_modules(prefixes: Iterable[str]) -> None:
    for prefix in prefixes:
        for name in list(sys.modules):
            if name == prefix or name.startswith(prefix + "."):
                sys.modules.pop(name, None)


def _make_jwt(secret: str, payload: dict[str, object]) -> str:
    header_bytes = json.dumps({"alg": "HS256", "typ": "JWT"}).encode("utf-8")
    payload_bytes = json.dumps(payload).encode("utf-8")
    header_segment = base64.urlsafe_b64encode(header_bytes).rstrip(b"=").decode("ascii")
    payload_segment = base64.urlsafe_b64encode(payload_bytes).rstrip(b"=").decode("ascii")
    signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    signature_segment = base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii")
    return f"{header_segment}.{payload_segment}.{signature_segment}"


def test_authz_dependency_without_fastapi(monkeypatch: pytest.MonkeyPatch) -> None:
    _purge_modules(["fastapi", "shared.authz_middleware"])

    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("fastapi unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    monkeypatch.setenv("AUTH_JWT_SECRET", "dev-secret")

    middleware = importlib.import_module("shared.authz_middleware")

    token = _make_jwt(
        "dev-secret",
        {"sub": "user-123", "account_scopes": ["acct-1"], "exp": 9999999999},
    )

    request = middleware.Request(
        {
            "headers": [("authorization", f"Bearer {token}")],
            "path_params": {"account_id": "acct-1"},
            "query_params": {},
            "app": SimpleNamespace(state=SimpleNamespace()),
            "path": "/accounts/acct-1",
        }
    )
    request.url = SimpleNamespace(path="/accounts/acct-1")  # type: ignore[attr-defined]

    user = asyncio.run(middleware.authz_dependency(request))

    assert user.user_id == "user-123"
    assert getattr(request.state, "user") is user
    assert getattr(request.state, "account_scopes") == ("acct-1",)
    assert getattr(request, "account_scopes") == ("acct-1",)

