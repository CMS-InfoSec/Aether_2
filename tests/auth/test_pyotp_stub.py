"""Behavioural tests for the in-module pyotp fallback used by auth.service."""

from __future__ import annotations

import importlib
import sys


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
