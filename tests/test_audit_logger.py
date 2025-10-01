from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any
from unittest.mock import MagicMock

import pytest

import audit_logger


class _FakeDatetime(dt.datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        return cls(2023, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)


def _make_connection_mock():
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value = cursor
    return conn, cursor


@pytest.fixture(autouse=True)
def _set_fixed_datetime(monkeypatch):
    monkeypatch.setattr(audit_logger.dt, "datetime", _FakeDatetime)
    yield
    monkeypatch.setattr(audit_logger.dt, "datetime", dt.datetime)


def _call_log_audit(monkeypatch: pytest.MonkeyPatch, **kwargs: Any):
    conn, cursor = _make_connection_mock()
    monkeypatch.setenv("AUDIT_DATABASE_URL", "postgresql://audit:audit@localhost:5432/audit")
    monkeypatch.setattr(audit_logger, "psycopg", MagicMock(connect=MagicMock(return_value=conn)))
    audit_logger.log_audit(**kwargs)
    return conn, cursor


def test_log_audit_inserts_and_logs(monkeypatch, capsys):
    before = {"status": "pending"}
    after = {"status": "approved"}
    conn, cursor = _call_log_audit(
        monkeypatch,
        actor="alice",
        action="approve",
        entity="order-9",
        before=before,
        after=after,
        ip="192.0.2.1",
    )

    captured = capsys.readouterr().out.strip()
    assert captured, "Structured log line should be emitted"

    payload = json.loads(captured)
    assert payload["actor"] == "alice"
    assert payload["action"] == "approve"
    assert payload["entity"] == "order-9"
    assert payload["before"] == before
    assert payload["after"] == after

    expected_hash = hashlib.sha256("192.0.2.1".encode()).hexdigest()
    assert payload["ip_hash"] == expected_hash
    assert payload["ts"] == "2023-01-01T12:00:00+00:00"

    assert audit_logger.psycopg.connect.called
    cursor.execute.assert_called_once()
    sql, params = cursor.execute.call_args[0]
    assert "INSERT INTO audit_log" in sql
    assert params == (
        "alice",
        "approve",
        "order-9",
        json.dumps(before, separators=(",", ":")),
        json.dumps(after, separators=(",", ":")),
        dt.datetime(2023, 1, 1, 12, 0, tzinfo=dt.timezone.utc),
        expected_hash,
    )


def test_log_audit_allows_missing_ip(monkeypatch, capsys):
    _, cursor = _call_log_audit(
        monkeypatch,
        actor="bob",
        action="delete",
        entity="session-1",
        before={},
        after={},
        ip=None,
    )

    captured = capsys.readouterr().out.strip()
    payload = json.loads(captured)
    assert payload["ip_hash"] is None

    params = cursor.execute.call_args[0][1]
    assert params[-1] is None


def test_log_audit_requires_database_url(monkeypatch):
    monkeypatch.delenv("AUDIT_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError):
        audit_logger.log_audit(
            actor="carol",
            action="update",
            entity="profile",
            before={},
            after={},
            ip=None,
        )
