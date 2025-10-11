from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


def _reload_crypto_module() -> object:
    module = importlib.import_module("services.account_crypto")
    return importlib.reload(module)


def _state_dir(tmp_path: Path) -> Path:
    path = tmp_path / "state"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_local_key_persisted_when_insecure_defaults_enabled(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("ACCOUNT_ENCRYPTION_KEY", raising=False)
    monkeypatch.setenv("ACCOUNTS_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("AETHER_STATE_DIR", str(_state_dir(tmp_path)))

    module = _reload_crypto_module()
    module.fernet_key.cache_clear()  # type: ignore[attr-defined]
    key_bytes = module.fernet_key()  # type: ignore[attr-defined]

    assert isinstance(key_bytes, bytes)
    assert key_bytes

    key_path = tmp_path / "state" / "accounts" / "encryption.key"
    assert key_path.exists()
    persisted = key_path.read_text(encoding="ascii").strip().encode("ascii")
    assert persisted == key_bytes

    module.fernet_key.cache_clear()  # type: ignore[attr-defined]
    assert module.fernet_key() == key_bytes  # type: ignore[attr-defined]


def test_missing_key_raises_when_insecure_defaults_disabled(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("ACCOUNT_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("ACCOUNTS_ALLOW_INSECURE_DEFAULTS", raising=False)
    monkeypatch.setenv("AETHER_STATE_DIR", str(_state_dir(tmp_path)))
    monkeypatch.delitem(sys.modules, "pytest", raising=False)

    module = _reload_crypto_module()
    module.fernet_key.cache_clear()  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError):
        module.fernet_key()  # type: ignore[attr-defined]
