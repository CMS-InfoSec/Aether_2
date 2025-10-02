"""Tamper-evident audit logging utilities."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

try:  # pragma: no cover - import is validated in unit tests indirectly
    import psycopg  # type: ignore
except Exception as exc:  # pragma: no cover - handled at runtime when psycopg is unavailable
    psycopg = None  # type: ignore
    _PSYCOPG_IMPORT_ERROR = exc
else:  # pragma: no cover
    _PSYCOPG_IMPORT_ERROR = None


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
    path = os.getenv("AUDIT_CHAIN_STATE")
    if path:
        return Path(path).expanduser()
    return Path.home() / ".cache" / "audit_chain_state.json"


def _chain_log_path() -> Path:
    path = os.getenv("AUDIT_CHAIN_LOG")
    if path:
        return Path(path).expanduser()
    return Path.home() / ".cache" / "audit_chain.log"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _read_last_hash(state_path: Path) -> str:
    try:
        with state_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except FileNotFoundError:
        return _GENESIS_HASH
    except json.JSONDecodeError:  # pragma: no cover - indicates manual tampering
        return _GENESIS_HASH
    return payload.get("last_hash", _GENESIS_HASH)


def _write_last_hash(state_path: Path, entry_hash: str) -> None:
    _ensure_parent(state_path)
    payload = {"last_hash": entry_hash}
    with state_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))


def _append_chain_log(log_path: Path, entry: Dict[str, Any]) -> None:
    _ensure_parent(log_path)
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, separators=(",", ":"), sort_keys=True) + "\n")


def _chain_previous_hash(prev_hash: str) -> str:
    """Return the SHA-256 digest of the previous hash value."""

    return hashlib.sha256(prev_hash.encode("utf-8")).hexdigest()


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
    chained_prev_hash = _chain_previous_hash(prev_entry_hash)
    canonical_serialized = _canonical_serialized(core_payload)
    entry_hash = hashlib.sha256((chained_prev_hash + canonical_serialized).encode("utf-8")).hexdigest()

    chained_payload = dict(core_payload)
    chained_payload["prev_hash"] = chained_prev_hash
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
                    chained_prev_hash,
                ),
            )


def verify_audit_chain() -> bool:
    """Validate the audit chain log for tamper evidence.

    Returns ``True`` when the chain is valid, otherwise ``False``.
    """

    log_path = _chain_log_path()
    if not log_path.exists():
        print("No audit chain entries found.")
        return True

    prev_entry_hash = _GENESIS_HASH
    valid = True

    with log_path.open("r", encoding="utf-8") as fh:
        for line_number, line in enumerate(fh, start=1):
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

            expected_prev_hash = _chain_previous_hash(prev_entry_hash)

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

    if valid:
        print("Audit chain verified successfully.")
    return valid


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit logger utilities")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True
    subparsers.add_parser("verify", help="Verify the audit chain integrity")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = _build_cli()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "verify":
        return 0 if verify_audit_chain() else 1

    parser.print_help()
    return 1


if __name__ == "__main__":  # pragma: no cover - manual execution entry point
    sys.exit(main())


__all__ = ["log_audit", "verify_audit_chain", "main", "hash_ip", "is_sensitive_action"]

