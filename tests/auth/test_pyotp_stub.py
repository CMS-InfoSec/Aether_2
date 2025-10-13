"""Behavioural tests for the in-module pyotp fallback used by auth.service."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType


def test_pyotp_stub_provisioning_uri(monkeypatch):
    """The deterministic pyotp stub should expose a provisioning URI helper."""

    # Ensure the auth.service module is re-imported without a real pyotp module
    # available so that the in-module fallback is exercised.
    monkeypatch.delitem(sys.modules, "pyotp", raising=False)
    for name in [
        mod_name
        for mod_name in list(sys.modules)
        if mod_name == "auth.service" or mod_name.startswith("auth.service.")
    ]:
        monkeypatch.delitem(sys.modules, name, raising=False)

    service_module = importlib.import_module("auth.service")

    totp = service_module.pyotp.TOTP("SECRET123")
    uri = totp.provisioning_uri(
        "admin@example.com",
        issuer_name="Aether Corp",
        period=45,
    )

    assert (
        uri
        == "otpauth://totp/Aether%20Corp%3Aadmin%40example.com"
        "?secret=SECRET123&issuer=Aether+Corp&period=45"
    )


def test_pyotp_adapter_provisioning_uri(monkeypatch):
    """Adapters installed when tests stub pyotp should expose provisioning URIs."""

    class _ConftestTOTP:
        __module__ = "tests.conftest_pyotp_stub"

        def __init__(self, secret: str) -> None:
            self._secret = secret

        def now(self) -> str:
            return "999999"

        def verify(self, code: str, valid_window: int = 1) -> bool:
            del valid_window
            return code == self.now()

    pyotp_stub = ModuleType("pyotp")
    pyotp_stub.TOTP = _ConftestTOTP  # type: ignore[attr-defined]
    pyotp_stub.random_base32 = lambda: "SECRET123"  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyotp", pyotp_stub)

    for name in [
        mod_name
        for mod_name in list(sys.modules)
        if mod_name == "auth.service" or mod_name.startswith("auth.service.")
    ]:
        monkeypatch.delitem(sys.modules, name, raising=False)

    service_module = importlib.import_module("auth.service")

    totp = service_module.pyotp.TOTP("SECRET123")
    uri = totp.provisioning_uri(
        "admin@example.com",
        issuer_name="Aether Corp",
        period=30,
    )

    assert (
        uri
        == "otpauth://totp/Aether%20Corp%3Aadmin%40example.com"
        "?secret=SECRET123&issuer=Aether+Corp&period=30"
    )
