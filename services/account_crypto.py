"""Account credential encryption helpers with insecure-default fallbacks."""

from __future__ import annotations

import logging
import os
import sys
import time
from functools import lru_cache
from pathlib import Path

LOGGER = logging.getLogger(__name__)

_INSECURE_DEFAULTS_FLAG = "ACCOUNTS_ALLOW_INSECURE_DEFAULTS"
_STATE_DIR_ENV = "AETHER_STATE_DIR"
_DEFAULT_STATE_DIR = Path(".aether_state")
_LOCAL_KEY_FILENAME = "encryption.key"
_LOCAL_KEY_SUBDIR = "accounts"


def _state_root() -> Path:
    root = Path(os.getenv(_STATE_DIR_ENV, _DEFAULT_STATE_DIR))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _local_key_path() -> Path:
    directory = _state_root() / _LOCAL_KEY_SUBDIR
    directory.mkdir(parents=True, exist_ok=True)
    return directory / _LOCAL_KEY_FILENAME


def _insecure_defaults_enabled() -> bool:
    flag = os.getenv(_INSECURE_DEFAULTS_FLAG)
    if flag == "1":
        return True
    if flag == "0":
        return False
    return "pytest" in sys.modules


def _read_local_key(path: Path) -> bytes | None:
    try:
        existing = path.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError) as exc:  # pragma: no cover - defensive warning
        LOGGER.warning("Failed to read local encryption key from %s: %s", path, exc)
        return None

    if not existing:
        return None

    return existing.encode("ascii")


def _load_or_generate_local_key() -> bytes:
    from cryptography.fernet import Fernet

    path = _local_key_path()

    while True:
        existing = _read_local_key(path)
        if existing is not None:
            return existing

        key_bytes = Fernet.generate_key()
        key_text = key_bytes.decode("ascii")

        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            time.sleep(0.05)
            continue
        except OSError as exc:  # pragma: no cover - persistence best-effort
            LOGGER.warning("Failed to open %s for writing local encryption key: %s", path, exc)
            break

        try:
            with os.fdopen(fd, "w", encoding="ascii") as handle:
                handle.write(key_text)
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:  # pragma: no cover - persistence best-effort
            LOGGER.warning("Failed to persist generated encryption key to %s: %s", path, exc)
        return key_bytes

    # Fall back to a best-effort read if exclusive creation fails repeatedly.
    existing = _read_local_key(path)
    if existing is not None:
        return existing

    return key_bytes


@lru_cache(maxsize=1)
def fernet_key() -> bytes:
    key = os.getenv("ACCOUNT_ENCRYPTION_KEY")
    if key:
        try:
            return key.encode("ascii")
        except Exception as exc:  # pragma: no cover - defensive
            raise RuntimeError("ACCOUNT_ENCRYPTION_KEY must be ASCII encodable") from exc

    if not _insecure_defaults_enabled():
        raise RuntimeError("ACCOUNT_ENCRYPTION_KEY environment variable is required")

    return _load_or_generate_local_key()


@lru_cache(maxsize=1)
def _fernet() -> "Fernet":  # type: ignore[name-defined]
    from cryptography.fernet import Fernet

    return Fernet(fernet_key())


def encrypt_value(value: str) -> str:
    if value == "":
        raise ValueError("Cannot encrypt empty value")
    token = _fernet().encrypt(value.encode("utf-8"))
    return token.decode("ascii")


def decrypt_value(value: str) -> str:
    token = _fernet().decrypt(value.encode("ascii"))
    return token.decode("utf-8")


__all__ = [
    "decrypt_value",
    "encrypt_value",
    "fernet_key",
]
