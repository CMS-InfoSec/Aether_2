"""Tamper-evident audit logging utilities."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union

import errno
import logging

try:  # pragma: no cover - import is validated in unit tests indirectly
    import psycopg  # type: ignore
except Exception as exc:  # pragma: no cover - handled at runtime when psycopg is unavailable
    psycopg = None  # type: ignore
    _PSYCOPG_IMPORT_ERROR = exc
else:  # pragma: no cover
    _PSYCOPG_IMPORT_ERROR = None


LOGGER = logging.getLogger(__name__)

_GENESIS_HASH = "0" * 64
_CORE_FIELDS: Iterable[str] = (
    "actor",
    "action",
    "entity",
    "before",
    "after",
    "ip_hash",
    "ts",
    "sensitive",
)

_SENSITIVE_ACTION_PREFIXES: Iterable[str] = (
    "config.",
    "secret.",
    "override.",
    "safe_mode.",
)

_DEFAULT_CHAIN_DIR = Path.home() / ".cache"


def _sanitize_chain_path(env_var: str, default_filename: str) -> Path:
    """Return a secure filesystem path for audit chain artefacts."""

    raw = os.getenv(env_var)
    if raw:
        candidate = Path(raw).expanduser()
    else:
        candidate = _DEFAULT_CHAIN_DIR / default_filename

    if not candidate.is_absolute():
        raise ValueError(f"{env_var} must be an absolute path")

    if any(part in {".", ".."} for part in candidate.parts[1:]):
        raise ValueError(f"{env_var} must not contain path traversal sequences")

    ancestors = [candidate] + list(candidate.parents)
    anchor = Path(candidate.anchor) if candidate.is_absolute() else None

    for ancestor in ancestors:
        if anchor is not None and ancestor == anchor:
            break
        if ancestor.is_symlink():
            raise ValueError(f"{env_var} must not reference symlinks")

    return candidate


def is_sensitive_action(action: str) -> bool:
    """Return ``True`` if the supplied action is considered sensitive."""

    return any(action.startswith(prefix) for prefix in _SENSITIVE_ACTION_PREFIXES)


def hash_ip(value: Optional[str]) -> Optional[str]:
    """Return a stable SHA-256 hash of the provided IP address."""

    if value is None:
        return None

    stripped = value.strip()
    if not stripped:
        return None

    return hashlib.sha256(stripped.encode("utf-8")).hexdigest()


def _database_dsn() -> str:
    """Return the database connection string for audit logging."""

    dsn = os.getenv("AUDIT_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "AUDIT_DATABASE_URL or DATABASE_URL environment variable must be set for audit logging"
        )
    return dsn


def _chain_state_path() -> Path:
    return _sanitize_chain_path("AUDIT_CHAIN_STATE", "audit_chain_state.json")


def _chain_log_path() -> Path:
    return _sanitize_chain_path("AUDIT_CHAIN_LOG", "audit_chain.log")


def _ensure_parent(path: Path) -> None:
    parent = path.parent
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except FileExistsError as exc:
        raise ValueError("Audit chain parent path must be a directory") from exc

    if parent.exists() and parent.is_symlink():
        raise ValueError("Audit chain parent path must not be a symlink")


def _ensure_not_symlink(path: Path) -> None:
    if path.is_symlink():
        raise ValueError("Audit chain path must not be a symlink")


def _write_text_secure(path: Path, payload: str, *, append: bool = False) -> None:
    """Write ``payload`` to ``path`` while guarding against symlink traversal."""

    _ensure_not_symlink(path)

    flags = os.O_WRONLY | os.O_CREAT
    flags |= os.O_APPEND if append else os.O_TRUNC

    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW  # pragma: no cover - platform specific branch

    mode = 0o600

    try:
        fd = os.open(path, flags, mode)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EPERM}:
            raise ValueError("Audit chain path must not be a symlink") from exc
        raise

    with os.fdopen(fd, "a" if append else "w", encoding="utf-8") as handle:
        handle.write(payload)


def _read_text_secure(path: Path) -> str:
    """Return the contents of ``path`` while preventing symlink traversal."""

    _ensure_not_symlink(path)

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW  # pragma: no cover - platform specific branch

    try:
        fd = os.open(path, flags)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EPERM}:
            raise ValueError("Audit chain path must not be a symlink") from exc
        raise

    with os.fdopen(fd, "r", encoding="utf-8") as handle:
        return handle.read()


def _read_last_hash(state_path: Path) -> str:
    try:
        raw = _read_text_secure(state_path)
    except FileNotFoundError:
        return _GENESIS_HASH
    except ValueError as exc:
        LOGGER.error("Audit chain state path %s is invalid: %s", state_path, exc)
        raise

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:  # pragma: no cover - indicates manual tampering
        return _GENESIS_HASH
    return payload.get("last_hash", _GENESIS_HASH)


def _write_last_hash(state_path: Path, entry_hash: str) -> None:
    _ensure_parent(state_path)
    payload = {"last_hash": entry_hash}
    serialized = json.dumps(payload, separators=(",", ":"))
    _write_text_secure(state_path, serialized)


def _append_chain_log(log_path: Path, entry: Dict[str, Any]) -> None:
    _ensure_parent(log_path)
    payload = json.dumps(entry, separators=(",", ":"), sort_keys=True) + "\n"
    _write_text_secure(log_path, payload, append=True)


def _canonical_payload(entry: Dict[str, Any]) -> Dict[str, Any]:
    canonical: Dict[str, Any] = {}
    for field in _CORE_FIELDS:
        if field == "sensitive":
            canonical[field] = bool(entry.get(field, False))
        else:
            canonical[field] = entry[field]
    return canonical


def _canonical_serialized(entry: Dict[str, Any]) -> str:
    return json.dumps(entry, separators=(",", ":"), sort_keys=True)


def log_audit(
    actor: str,
    action: str,
    entity: str,
    before: Dict[str, Any],
    after: Dict[str, Any],
    ip_hash: Optional[str],
) -> None:
    """Record an audit entry.

    The entry is:
    * Emitted to stdout as a structured JSON payload.
    * Inserted into the ``audit_log`` PostgreSQL table.
    * Chained via SHA-256 digests for tamper-evident verification.
    """

    if psycopg is None:  # pragma: no cover - import verified via runtime guard in tests
        raise RuntimeError("psycopg is required for audit logging") from _PSYCOPG_IMPORT_ERROR

    timestamp = dt.datetime.now(dt.timezone.utc)
    timestamp_iso = timestamp.isoformat()

    core_payload: Dict[str, Any] = {
        "actor": actor,
        "action": action,
        "entity": entity,
        "before": before,
        "after": after,
        "ip_hash": ip_hash,
        "ts": timestamp_iso,
        "sensitive": is_sensitive_action(action),
    }

    state_path = _chain_state_path()
    prev_entry_hash = _read_last_hash(state_path)
    canonical_serialized = _canonical_serialized(core_payload)
    entry_hash = hashlib.sha256((prev_entry_hash + canonical_serialized).encode("utf-8")).hexdigest()

    chained_payload = dict(core_payload)
    chained_payload["prev_hash"] = prev_entry_hash
    chained_payload["hash"] = entry_hash

    sys.stdout.write(_canonical_serialized(chained_payload) + "\n")

    _append_chain_log(_chain_log_path(), chained_payload)
    _write_last_hash(state_path, entry_hash)

    before_json = json.dumps(before, separators=(",", ":"), sort_keys=True)
    after_json = json.dumps(after, separators=(",", ":"), sort_keys=True)

    dsn = _database_dsn()

    with psycopg.connect(dsn) as conn:  # pragma: no cover - integration behaviour
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit_log (
                    actor,
                    action,
                    entity,
                    before,
                    after,
                    ts,
                    hash,
                    prev_hash
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """.strip(),
                (
                    actor,
                    action,
                    entity,
                    before_json,
                    after_json,
                    timestamp,
                    entry_hash,
                    prev_entry_hash,
                ),
            )


def verify_audit_chain(
    *, log_path: Optional[Union[str, Path]] = None, state_path: Optional[Union[str, Path]] = None
) -> bool:
    """Validate the audit chain log for tamper evidence.

    Returns ``True`` when the chain is valid, otherwise ``False``.
    """

    log_path = Path(log_path) if log_path is not None else _chain_log_path()
    state_path = Path(state_path) if state_path is not None else _chain_state_path()

    try:
        raw_log = _read_text_secure(log_path)
    except FileNotFoundError:
        print("No audit chain entries found.")
        return True
    except ValueError as exc:
        LOGGER.error("Audit chain log path %s is invalid: %s", log_path, exc)
        raise

    prev_entry_hash = _GENESIS_HASH
    valid = True
    final_entry_hash = prev_entry_hash

    for line_number, line in enumerate(raw_log.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            print(f"Invalid JSON at line {line_number}")
            valid = False
            break

        try:
            entry_prev = entry["prev_hash"]
            entry_hash = entry["hash"]
        except KeyError as exc:
            print(f"Missing key {exc.args[0]!r} at line {line_number}")
            valid = False
            break

        expected_prev_hash = prev_entry_hash

        if entry_prev != expected_prev_hash:
            print(
                "Chain break at line "
                f"{line_number}: expected prev_hash {expected_prev_hash}, got {entry_prev}"
            )
            valid = False
            break

        canonical_payload = _canonical_payload(entry)
        recalculated = hashlib.sha256(
            (expected_prev_hash + _canonical_serialized(canonical_payload)).encode("utf-8")
        ).hexdigest()

        if recalculated != entry_hash:
            print(
                f"Hash mismatch at line {line_number}: expected {entry_hash}, calculated {recalculated}"
            )
            valid = False
            break

        prev_entry_hash = entry_hash
        final_entry_hash = entry_hash

    if valid:
        recorded_state_hash = _read_last_hash(state_path)
        if recorded_state_hash != final_entry_hash:
            print(
                "State hash mismatch: "
                f"expected {final_entry_hash}, found {recorded_state_hash} in state file"
            )
            valid = False

    if valid:
        print("Audit chain verified successfully.")
    return valid


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit logger utilities")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True
    verify_parser = subparsers.add_parser("verify", help="Verify the audit chain integrity")
    verify_parser.add_argument(
        "--log",
        dest="log_path",
        type=Path,
        default=None,
        help="Path to the audit chain log file (defaults to AUDIT_CHAIN_LOG)",
    )
    verify_parser.add_argument(
        "--state",
        dest="state_path",
        type=Path,
        default=None,
        help="Path to the audit chain state file (defaults to AUDIT_CHAIN_STATE)",
    )
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = _build_cli()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "verify":
        return 0 if verify_audit_chain(log_path=args.log_path, state_path=args.state_path) else 1

    parser.print_help()
    return 1


if __name__ == "__main__":  # pragma: no cover - manual execution entry point
    sys.exit(main())


__all__ = ["log_audit", "verify_audit_chain", "main", "hash_ip", "is_sensitive_action"]

