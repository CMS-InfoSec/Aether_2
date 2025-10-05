"""Regression tests for the default auth service configuration loader."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "services" / "auth" / "auth_service.py"

spec = importlib.util.spec_from_file_location("services.auth.auth_service", MODULE_PATH)
if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
    raise RuntimeError("Unable to load services.auth.auth_service module spec")
auth_module = importlib.util.module_from_spec(spec)
sys.modules.setdefault("services.auth.auth_service", auth_module)
spec.loader.exec_module(auth_module)


def test_default_loader_requires_configured_jwt_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AUTH_JWT_SECRET", raising=False)

    with pytest.raises(RuntimeError):
        auth_module._load_default_auth_service()


def test_default_loader_rejects_blank_jwt_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH_JWT_SECRET", "   ")

    with pytest.raises(RuntimeError):
        auth_module._load_default_auth_service()


def test_default_loader_uses_explicit_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH_JWT_SECRET", "prod-secret")

    service = auth_module._load_default_auth_service()

    assert service.settings.jwt_secret.get_secret_value() == "prod-secret"
