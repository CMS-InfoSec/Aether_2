"""Coverage for per-account Kraken credential mount enforcement."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from services.common import config


@pytest.fixture(autouse=True)
def reset_secret_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    config._SECRET_PATH_REGISTRY.clear()
    config._SECRET_PATH_USAGE.clear()
    for key in list(os.environ):
        if key.startswith("AETHER_") and key.endswith("_KRAKEN_SECRET_PATH"):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("AETHER_STATE_DIR", raising=False)
    yield
    config._SECRET_PATH_REGISTRY.clear()
    config._SECRET_PATH_USAGE.clear()


def _write_credentials(path: Path, *, key: str = "key", secret: str = "secret") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"key": key, "secret": secret}))


def test_unique_secret_mounts_required(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "_allow_test_fallbacks", lambda: False)

    shared_path = tmp_path / "kraken.json"
    _write_credentials(shared_path, key="company", secret="company-secret")

    monkeypatch.setenv("AETHER_COMPANY_KRAKEN_SECRET_PATH", str(shared_path))
    monkeypatch.setenv("AETHER_DIRECTOR-1_KRAKEN_SECRET_PATH", str(shared_path))

    creds = config.get_kraken_credentials("company")
    assert creds.key == "company"

    with pytest.raises(RuntimeError) as exc:
        config.get_kraken_credentials("director-1")
    assert "secret path conflict" in str(exc.value)


def test_insecure_defaults_generate_stub(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "_allow_test_fallbacks", lambda: True)
    monkeypatch.setenv("AETHER_STATE_DIR", str(tmp_path))

    creds = config.get_kraken_credentials("company")
    assert creds.key == "stub-company-key"
    assert creds.secret == "stub-company-secret"

    fallback_path = tmp_path / "kraken_credentials" / "company.json"
    assert fallback_path.exists()
    stored = json.loads(fallback_path.read_text())
    assert stored["key"] == "stub-company-key"
    assert stored["secret"] == "stub-company-secret"

