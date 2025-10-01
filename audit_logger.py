"""Audit logging utilities for structured logging and database persistence."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sys
from typing import Dict, Optional

try:  # pragma: no cover - exercised implicitly in unit tests.
    import psycopg
except Exception as exc:  # pragma: no cover - handled at runtime if psycopg is missing.
    psycopg = None  # type: ignore
    _PSYCOPG_IMPORT_ERROR = exc
else:  # pragma: no cover
    _PSYCOPG_IMPORT_ERROR = None


def _database_dsn() -> str:
    """Return the database connection string for audit logging."""

    dsn = os.getenv("AUDIT_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "AUDIT_DATABASE_URL or DATABASE_URL environment variable must be set for audit logging"
        )
    return dsn


def _hash_ip(ip: Optional[str]) -> Optional[str]:
    if not ip:
        return None
    digest = hashlib.sha256(ip.encode("utf-8")).hexdigest()
    return digest


def log_audit(
    actor: str,
    action: str,
    entity: str,
    before: Dict,
    after: Dict,
    ip: Optional[str],
) -> None:
    """Record an audit trail entry.

    The function both emits a structured JSON log line and persists the same entry
    into the ``audit_log`` PostgreSQL table.
    """

    if psycopg is None:  # pragma: no cover - tested indirectly by raising error.
        raise RuntimeError("psycopg is required for audit logging") from _PSYCOPG_IMPORT_ERROR

    timestamp = dt.datetime.now(dt.timezone.utc)
    ip_hash = _hash_ip(ip)

    log_record = {
        "actor": actor,
        "action": action,
        "entity": entity,
        "before": before,
        "after": after,
        "ip_hash": ip_hash,
        "ts": timestamp.isoformat(),
    }

    sys.stdout.write(json.dumps(log_record, separators=(",", ":")) + "\n")

    before_json = json.dumps(before, separators=(",", ":"))
    after_json = json.dumps(after, separators=(",", ":"))

    dsn = _database_dsn()

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit_log (actor, action, entity, before_json, after_json, ts, ip_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """.strip(),
                (
                    actor,
                    action,
                    entity,
                    before_json,
                    after_json,
                    timestamp,
                    ip_hash,
                ),
            )


__all__ = ["log_audit"]

