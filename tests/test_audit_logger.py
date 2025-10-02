from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from common.utils import audit_logger


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


def _call_log_audit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, **kwargs: Any):
    conn, cursor = _make_connection_mock()
    monkeypatch.setenv("AUDIT_DATABASE_URL", "postgresql://audit:audit@localhost:5432/audit")
    monkeypatch.setenv("AUDIT_CHAIN_LOG", str(tmp_path / "chain.log"))
    monkeypatch.setenv("AUDIT_CHAIN_STATE", str(tmp_path / "chain_state.json"))
    monkeypatch.setattr(audit_logger, "psycopg", MagicMock(connect=MagicMock(return_value=conn)))
    audit_logger.log_audit(**kwargs)
    return conn, cursor


def test_log_audit_inserts_and_logs(monkeypatch, tmp_path, capsys):
    before = {"status": "pending"}
    after = {"status": "approved"}
    hashed_ip = audit_logger.hash_ip("192.0.2.1")
    conn, cursor = _call_log_audit(
        monkeypatch,
        tmp_path,
        actor="alice",
        action="approve",
        entity="order-9",
        before=before,
        after=after,
        ip_hash=hashed_ip,
    )

    captured = capsys.readouterr().out.strip()
    assert captured, "Structured log line should be emitted"

    payload = json.loads(captured)
    assert payload["actor"] == "alice"
    assert payload["action"] == "approve"
    assert payload["entity"] == "order-9"
    assert payload["before"] == before
    assert payload["after"] == after

    assert payload["ip_hash"] == hashed_ip
    assert payload["ts"] == "2023-01-01T12:00:00+00:00"
    expected_prev_hash = audit_logger._GENESIS_HASH  # pylint: disable=protected-access
    assert payload["prev_hash"] == expected_prev_hash
    assert "hash" in payload
    assert payload["sensitive"] is False

    assert audit_logger.psycopg.connect.called
    cursor.execute.assert_called_once()
    sql, params = cursor.execute.call_args[0]
    assert "INSERT INTO audit_log" in sql
    assert params[:6] == (
        "alice",
        "approve",
        "order-9",
        json.dumps(before, separators=(",", ":"), sort_keys=True),
        json.dumps(after, separators=(",", ":"), sort_keys=True),
        dt.datetime(2023, 1, 1, 12, 0, tzinfo=dt.timezone.utc),
    )

    db_entry_hash, db_prev_hash = params[6:]
    assert db_prev_hash == expected_prev_hash
    assert db_entry_hash == payload["hash"]


def test_log_audit_allows_missing_ip(monkeypatch, tmp_path, capsys):
    _, cursor = _call_log_audit(
        monkeypatch,
        tmp_path,
        actor="bob",
        action="delete",
        entity="session-1",
        before={},
        after={},
        ip_hash=None,
    )

    captured = capsys.readouterr().out.strip()
    payload = json.loads(captured)
    assert payload["ip_hash"] is None
    assert payload["sensitive"] is False

    chain_file = tmp_path / "chain.log"
    with chain_file.open("r", encoding="utf-8") as fh:
        lines = [json.loads(line) for line in fh if line.strip()]
    assert len(lines) == 1
    assert lines[0]["ip_hash"] is None

    params = cursor.execute.call_args[0][1]
    assert len(params) == 8


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
            ip_hash=None,
        )


def test_verify_audit_chain(tmp_path, monkeypatch, capsys):
    hashed_ip = audit_logger.hash_ip("1.2.3.4")
    monkeypatch.setenv("AUDIT_DATABASE_URL", "postgresql://audit:audit@localhost:5432/audit")
    monkeypatch.setenv("AUDIT_CHAIN_LOG", str(tmp_path / "chain.log"))
    monkeypatch.setenv("AUDIT_CHAIN_STATE", str(tmp_path / "chain_state.json"))

    conn, cursor = _make_connection_mock()
    monkeypatch.setattr(audit_logger, "psycopg", MagicMock(connect=MagicMock(return_value=conn)))

    audit_logger.log_audit(
        actor="dave",
        action="create",
        entity="ticket",
        before={},
        after={"status": "open"},
        ip_hash=hashed_ip,
    )

    capsys.readouterr()

    assert audit_logger.verify_audit_chain() is True
    out = capsys.readouterr().out
    assert "Audit chain verified successfully." in out


def test_verify_audit_chain_detects_state_tampering(tmp_path, monkeypatch, capsys):
    hashed_ip = audit_logger.hash_ip("203.0.113.9")
    chain_log = tmp_path / "chain.log"
    chain_state = tmp_path / "chain_state.json"
    monkeypatch.setenv("AUDIT_DATABASE_URL", "postgresql://audit:audit@localhost:5432/audit")
    monkeypatch.setenv("AUDIT_CHAIN_LOG", str(chain_log))
    monkeypatch.setenv("AUDIT_CHAIN_STATE", str(chain_state))

    conn, cursor = _make_connection_mock()
    monkeypatch.setattr(audit_logger, "psycopg", MagicMock(connect=MagicMock(return_value=conn)))

    audit_logger.log_audit(
        actor="erin",
        action="update",
        entity="record",
        before={},
        after={"status": "done"},
        ip_hash=hashed_ip,
    )

    capsys.readouterr()

    # tamper with the state file by resetting the stored hash to genesis
    chain_state.write_text(json.dumps({"last_hash": audit_logger._GENESIS_HASH}))  # pylint: disable=protected-access

    assert audit_logger.verify_audit_chain() is False
    out = capsys.readouterr().out
    assert "State hash mismatch" in out


def test_cli_verify_command(tmp_path, monkeypatch, capsys):
    chain_log = tmp_path / "cli_chain.log"
    chain_state = tmp_path / "cli_state.json"
    monkeypatch.setenv("AUDIT_DATABASE_URL", "postgresql://audit:audit@localhost:5432/audit")
    monkeypatch.setenv("AUDIT_CHAIN_LOG", str(chain_log))
    monkeypatch.setenv("AUDIT_CHAIN_STATE", str(chain_state))

    conn, cursor = _make_connection_mock()
    monkeypatch.setattr(audit_logger, "psycopg", MagicMock(connect=MagicMock(return_value=conn)))

    audit_logger.log_audit(
        actor="ivy",
        action="create",
        entity="ticket",
        before={},
        after={"status": "open"},
        ip_hash=None,
    )

    capsys.readouterr()

    exit_code = audit_logger.main(["verify", "--log", str(chain_log), "--state", str(chain_state)])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "Audit chain verified successfully." in out


def test_sensitive_action_flag(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("AUDIT_DATABASE_URL", "postgresql://audit:audit@localhost:5432/audit")
    monkeypatch.setenv("AUDIT_CHAIN_LOG", str(tmp_path / "chain.log"))
    monkeypatch.setenv("AUDIT_CHAIN_STATE", str(tmp_path / "chain_state.json"))

    conn, cursor = _make_connection_mock()
    monkeypatch.setattr(audit_logger, "psycopg", MagicMock(connect=MagicMock(return_value=conn)))

    audit_logger.log_audit(
        actor="zoe",
        action="config.change.requested",
        entity="account-1:key",
        before={},
        after={"value": "42"},
        ip_hash=audit_logger.hash_ip("198.51.100.10"),
    )

    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["sensitive"] is True

    sql, params = cursor.execute.call_args[0]
    assert "INSERT INTO audit_log" in sql
    assert params[6] == payload["hash"]
    assert params[7] == payload["prev_hash"]
