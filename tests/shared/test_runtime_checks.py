import os

import pytest

from shared import runtime_checks


def test_runtime_checks_enforce_allowlists(monkeypatch):
    original_admin = os.environ.get("ADMIN_ALLOWLIST")
    original_director = os.environ.get("DIRECTOR_ALLOWLIST")

    monkeypatch.setattr(runtime_checks, "_is_test_environment", lambda: False)
    monkeypatch.delenv("ADMIN_ALLOWLIST", raising=False)
    monkeypatch.delenv("DIRECTOR_ALLOWLIST", raising=False)

    with pytest.raises(RuntimeError) as excinfo:
        runtime_checks.assert_account_allowlists_configured()

    message = str(excinfo.value)
    assert "ADMIN_ALLOWLIST" in message
    assert "DIRECTOR_ALLOWLIST" in message

    if original_admin is not None:
        monkeypatch.setenv("ADMIN_ALLOWLIST", original_admin)
    if original_director is not None:
        monkeypatch.setenv("DIRECTOR_ALLOWLIST", original_director)
