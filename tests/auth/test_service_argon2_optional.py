"""Tests ensuring the auth service works with and without argon2 installed."""

from __future__ import annotations

import importlib
import importlib.abc
import importlib.util
import sys
import types
from typing import Iterable

import pytest


ARGON2_AVAILABLE = importlib.util.find_spec("argon2") is not None


def _reload_auth_service(monkeypatch: pytest.MonkeyPatch, *, block_argon2: bool):
    """Reload ``auth.service`` optionally simulating an unavailable argon2."""

    module_names: Iterable[str] = [
        name for name in list(sys.modules) if name == "auth.service" or name.startswith("auth.service.")
    ]
    for name in module_names:
        sys.modules.pop(name, None)

    if block_argon2:
        for name in list(sys.modules):
            if name == "argon2" or name.startswith("argon2."):
                sys.modules.pop(name, None)

        class _BlockArgon2Finder(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname: str, path=None, target=None):  # type: ignore[override]
                if fullname.startswith("argon2"):
                    raise ModuleNotFoundError("argon2 blocked for test")
                return None

        finder = _BlockArgon2Finder()
        monkeypatch.setattr(sys, "meta_path", [finder, *sys.meta_path])

    return importlib.import_module("auth.service")


class _StubTOTP:
    def __init__(self, secret: str) -> None:
        self.secret = secret

    def verify(self, code: str, valid_window: int = 1) -> bool:
        return code == "123456"

    def now(self) -> str:
        return "123456"


def _install_totp_stub(monkeypatch: pytest.MonkeyPatch, service_module) -> None:
    stub = types.SimpleNamespace(TOTP=_StubTOTP, random_base32=lambda: "STUBSECRET")
    monkeypatch.setattr(service_module, "pyotp", stub, raising=False)


def _build_service(service_module):
    repository = service_module.InMemoryAdminRepository()
    sessions = service_module.InMemorySessionStore()
    return service_module.AuthService(repository, sessions), repository


def test_login_flow_without_argon2(monkeypatch: pytest.MonkeyPatch) -> None:
    service_module = _reload_auth_service(monkeypatch, block_argon2=True)
    _install_totp_stub(monkeypatch, service_module)
    service, repository = _build_service(service_module)

    admin = service_module.AdminAccount(
        admin_id="fallback-1",
        email="fallback@example.com",
        password_hash=service_module.hash_password("hunter2"),
        mfa_secret="STUBSECRET",
    )
    repository.add(admin)

    with pytest.raises(PermissionError):
        service.login(
            email=admin.email,
            password="hunter2",
            mfa_code="000000",
            ip_address=None,
        )

    session = service.login(
        email=admin.email,
        password="hunter2",
        mfa_code="123456",
        ip_address=None,
    )

    assert session.admin_id == admin.admin_id
    assert getattr(service_module._ARGON2_HASHER, "hash_prefix", "").startswith("$pbkdf2-sha256$")
    assert repository.get_by_email(admin.email).password_hash == admin.password_hash


def test_login_upgrades_legacy_pbkdf2_hash_without_argon2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_module = _reload_auth_service(monkeypatch, block_argon2=True)
    _install_totp_stub(monkeypatch, service_module)
    service, repository = _build_service(service_module)

    legacy_hasher = service_module._FallbackPasswordHasher(iterations=1000)
    legacy_hash = legacy_hasher.hash("hunter2")

    admin = service_module.AdminAccount(
        admin_id="fallback-legacy",
        email="legacy@example.com",
        password_hash=legacy_hash,
        mfa_secret="STUBSECRET",
    )
    repository.add(admin)

    session = service.login(
        email=admin.email,
        password="hunter2",
        mfa_code="123456",
        ip_address=None,
    )

    assert session.admin_id == admin.admin_id

    updated_admin = repository.get_by_email(admin.email)
    assert updated_admin is not None
    assert updated_admin.password_hash != legacy_hash
    assert updated_admin.password_hash.startswith(service_module._ARGON2_HASHER.hash_prefix)
    assert updated_admin.password_hash.split("$")[2] == str(service_module._ARGON2_HASHER._iterations)


@pytest.mark.skipif(not ARGON2_AVAILABLE, reason="argon2 is required for this test")
def test_login_flow_with_argon2(monkeypatch: pytest.MonkeyPatch) -> None:
    service_module = _reload_auth_service(monkeypatch, block_argon2=False)
    _install_totp_stub(monkeypatch, service_module)
    service, repository = _build_service(service_module)

    admin = service_module.AdminAccount(
        admin_id="argon2-1",
        email="argon2@example.com",
        password_hash=service_module.hash_password("CorrectHorse"),
        mfa_secret="STUBSECRET",
    )
    repository.add(admin)

    with pytest.raises(PermissionError):
        service.login(
            email=admin.email,
            password="CorrectHorse",
            mfa_code="000000",
            ip_address=None,
        )

    session = service.login(
        email=admin.email,
        password="CorrectHorse",
        mfa_code="123456",
        ip_address=None,
    )

    assert session.admin_id == admin.admin_id
    assert service_module._ARGON2_HASHER.hash("CorrectHorse").startswith("$argon2")
