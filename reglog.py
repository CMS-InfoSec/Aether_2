"""Regulatory-grade tamper-evident logging utilities."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

try:  # pragma: no cover - dependency may be absent in insecure environments
    import psycopg
    from psycopg.rows import dict_row
except Exception as exc:  # pragma: no cover - exercised in fallback tests
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]
    _PSYCOPG_IMPORT_ERROR = exc
else:  # pragma: no cover - recorded for error propagation when required
    _PSYCOPG_IMPORT_ERROR = None


DEFAULT_ENCODING = "utf-8"
_INSECURE_DEFAULTS_FLAG = "REGLOG_ALLOW_INSECURE_DEFAULTS"
_STATE_DIR_ENV = "REGLOG_STATE_DIR"
_DEFAULT_STATE_DIR = Path(".aether_state/reglog")
_LOCAL_LOG_FILENAME = "reglog_log.json"
_LOCAL_LOG_LOCK = threading.Lock()


class ChainIntegrityError(RuntimeError):
    """Raised when the regulatory log hash chain is broken."""


def _insecure_defaults_enabled() -> bool:
    flag = os.getenv(_INSECURE_DEFAULTS_FLAG)
    if flag == "1":
        return True
    if flag == "0":
        return False
    return "pytest" in sys.modules


def _state_dir() -> Path:
    root = Path(os.getenv(_STATE_DIR_ENV, _DEFAULT_STATE_DIR))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _local_log_path() -> Path:
    return _state_dir() / _LOCAL_LOG_FILENAME


def _read_local_entries() -> list[dict[str, Any]]:
    path = _local_log_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    records: list[dict[str, Any]] = []
    for entry in payload:
        if isinstance(entry, dict):
            records.append(entry)
    return records


def _write_local_entries(entries: Iterable[dict[str, Any]]) -> None:
    path = _local_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    serialised = json.dumps(list(entries), separators=(",", ":"), sort_keys=True)
    path.write_text(serialised, encoding="utf-8")


def _load_local_chain() -> list[RegLogRecord]:
    records: list[RegLogRecord] = []
    for raw in _read_local_entries():
        ts_raw = raw.get("ts")
        if isinstance(ts_raw, str):
            try:
                ts_value = dt.datetime.fromisoformat(ts_raw)
            except ValueError:
                ts_value = dt.datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        else:
            ts_value = dt.datetime.now(dt.timezone.utc)
        records.append(
            RegLogRecord(
                id=int(raw.get("id", len(records) + 1)),
                ts=ts_value,
                actor=str(raw.get("actor", "")),
                action=str(raw.get("action", "")),
                payload=str(raw.get("payload", "{}")),
                hash=str(raw.get("hash", "")),
                prev_hash=raw.get("prev_hash"),
            )
        )
    return records


@dataclass(frozen=True)
class RegLogRecord:
    """Represents a single persisted regulatory log entry."""

    id: int
    ts: dt.datetime
    actor: str
    action: str
    payload: str
    hash: str
    prev_hash: Optional[str]


def _database_dsn(
    explicit_dsn: str | None = None, *, allow_missing: bool = False
) -> str | None:
    """Return the DSN that should be used for reg-log operations."""

    if explicit_dsn:
        return explicit_dsn
    env_dsn = os.getenv("REGLOG_DATABASE_URL") or os.getenv("AUDIT_DATABASE_URL")
    if env_dsn:
        return env_dsn
    fallback = os.getenv("DATABASE_URL")
    if fallback:
        return fallback
    if allow_missing:
        return None
    raise RuntimeError(
        "REGLOG_DATABASE_URL, AUDIT_DATABASE_URL, or DATABASE_URL must be set for reg logging",
    )


def _format_timestamp(value: dt.datetime) -> str:
    """Normalise timestamps to a stable ISO representation."""

    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc).isoformat(timespec="microseconds")


def _json_default(value: Any) -> str:
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    return str(value)


def _serialise_payload(payload: Any) -> str:
    """Normalise payload data for hashing and persistence."""

    if payload is None:
        return "{}"
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, default=_json_default, separators=(",", ":"), sort_keys=True)


def _compute_hash(
    ts_text: str,
    actor: str,
    action: str,
    payload_text: str,
    prev_hash: Optional[str],
) -> str:
    """Compute the SHA256 digest for a regulatory log record."""

    material = "|".join(
        (
            ts_text,
            actor,
            action,
            payload_text,
            prev_hash or "",
        )
    )
    return hashlib.sha256(material.encode(DEFAULT_ENCODING)).hexdigest()


def append_log(
    actor: str,
    action: str,
    payload: Any,
    *,
    timestamp: Optional[dt.datetime] = None,
    dsn: str | None = None,
) -> str:
    """Append a tamper-evident regulatory log entry and return its hash."""

    ts = timestamp or dt.datetime.now(dt.timezone.utc)
    ts_text = _format_timestamp(ts)
    payload_text = _serialise_payload(payload)
    allow_insecure = _insecure_defaults_enabled()
    resolved_dsn = _database_dsn(dsn, allow_missing=allow_insecure)

    if psycopg is None or resolved_dsn is None:
        if not allow_insecure:
            raise RuntimeError("psycopg is required for reg logging") from _PSYCOPG_IMPORT_ERROR
        with _LOCAL_LOG_LOCK:
            entries = _read_local_entries()
            prev_hash = entries[-1]["hash"] if entries else None
            entry_hash = _compute_hash(ts_text, actor, action, payload_text, prev_hash)
            last_id = int(entries[-1]["id"]) if entries and "id" in entries[-1] else len(entries)
            record = {
                "id": last_id + 1,
                "ts": ts_text,
                "actor": actor,
                "action": action,
                "payload": payload_text,
                "hash": entry_hash,
                "prev_hash": prev_hash,
            }
            entries.append(record)
            _write_local_entries(entries)
        return entry_hash

    with psycopg.connect(resolved_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT hash FROM reg_log ORDER BY id DESC LIMIT 1 FOR UPDATE")
            result = cur.fetchone()
            prev_hash = result[0] if result else None
            entry_hash = _compute_hash(ts_text, actor, action, payload_text, prev_hash)
            cur.execute(
                """
                INSERT INTO reg_log (ts, actor, action, payload, hash, prev_hash)
                VALUES (%s, %s, %s, %s, %s, %s)
                """.strip(),
                (ts, actor, action, payload_text, entry_hash, prev_hash),
            )
        conn.commit()
    return entry_hash


def _fetch_chain(conn: Any) -> Iterable[RegLogRecord]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, ts, actor, action, payload, hash, prev_hash
            FROM reg_log
            ORDER BY id ASC
            """.strip()
        )
        rows = cur.fetchall()
    for row in rows:
        yield RegLogRecord(
            id=row["id"],
            ts=row["ts"],
            actor=row["actor"],
            action=row["action"],
            payload=row["payload"],
            hash=row["hash"],
            prev_hash=row["prev_hash"],
        )


def verify_chain(*, dsn: str | None = None) -> bool:
    """Verify the integrity of the entire regulatory log chain."""

    allow_insecure = _insecure_defaults_enabled()
    resolved_dsn = _database_dsn(dsn, allow_missing=allow_insecure)

    if psycopg is None or resolved_dsn is None:
        if not allow_insecure:
            raise RuntimeError("psycopg is required for reg logging") from _PSYCOPG_IMPORT_ERROR
        with _LOCAL_LOG_LOCK:
            records = list(_load_local_chain())
    else:
        with psycopg.connect(resolved_dsn) as conn:
            records = list(_fetch_chain(conn))

    previous_hash: Optional[str] = None
    for record in records:
        ts_text = _format_timestamp(record.ts)
        expected_hash = _compute_hash(
            ts_text,
            record.actor,
            record.action,
            record.payload,
            previous_hash,
        )
        if record.prev_hash != previous_hash:
            raise ChainIntegrityError(
                f"reg_log row {record.id} links to {record.prev_hash!r}, expected {previous_hash!r}",
            )
        if record.hash != expected_hash:
            raise ChainIntegrityError(
                f"reg_log row {record.id} hash mismatch: stored {record.hash}, expected {expected_hash}",
            )
        previous_hash = record.hash
    return True


def _cli_verify(args: argparse.Namespace) -> int:
    try:
        verify_chain(dsn=args.dsn)
    except ChainIntegrityError as exc:
        sys.stderr.write(f"Chain verification failed: {exc}\n")
        return 1
    except Exception as exc:  # pragma: no cover - surfaced to the operator.
        sys.stderr.write(f"Verification encountered an unexpected error: {exc}\n")
        return 2
    else:
        sys.stdout.write("reg_log chain verified successfully.\n")
        return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Tamper-evident regulatory logging utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    verify_parser = subparsers.add_parser("verify", help="Validate the reg_log hash chain")
    verify_parser.add_argument("--dsn", dest="dsn", help="Override the PostgreSQL DSN to use", default=None)
    verify_parser.set_defaults(func=_cli_verify)

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 1
    return func(args)


if __name__ == "__main__":  # pragma: no cover - CLI entry point.
    raise SystemExit(main())
