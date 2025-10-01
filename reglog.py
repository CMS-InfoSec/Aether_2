"""Regulatory-grade tamper-evident logging utilities."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Iterable, Optional

import psycopg
from psycopg.rows import dict_row


DEFAULT_ENCODING = "utf-8"


class ChainIntegrityError(RuntimeError):
    """Raised when the regulatory log hash chain is broken."""


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


def _database_dsn(explicit_dsn: str | None = None) -> str:
    """Return the DSN that should be used for reg-log operations."""

    if explicit_dsn:
        return explicit_dsn
    env_dsn = os.getenv("REGLOG_DATABASE_URL") or os.getenv("AUDIT_DATABASE_URL")
    if env_dsn:
        return env_dsn
    fallback = os.getenv("DATABASE_URL")
    if fallback:
        return fallback
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
    resolved_dsn = _database_dsn(dsn)

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


def _fetch_chain(conn: psycopg.Connection[Any]) -> Iterable[RegLogRecord]:
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

    resolved_dsn = _database_dsn(dsn)
    with psycopg.connect(resolved_dsn) as conn:
        previous_hash: Optional[str] = None
        for record in _fetch_chain(conn):
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
