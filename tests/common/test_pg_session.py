"""Tests for the PostgreSQL session context manager utilities."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from pg_session import db_session


class StubConnection:
    def __init__(self, dialect_name: str = "postgresql") -> None:
        self.dialect = SimpleNamespace(name=dialect_name)
        self.commands: list[tuple[str, dict | None]] = []
        self.execution_calls: list[dict] = []

    def exec_driver_sql(self, statement: str, parameters: dict | None = None) -> None:
        self.commands.append((statement, parameters))

    def execution_options(self, **kwargs):  # type: ignore[override]
        self.execution_calls.append(kwargs)
        return self


class StubSession:
    def __init__(self, connection: StubConnection) -> None:
        self._connection = connection
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0
        self._in_transaction = True

    def connection(self):  # type: ignore[override]
        return self._connection

    def in_transaction(self) -> bool:  # type: ignore[override]
        return self._in_transaction

    def get_transaction(self):  # type: ignore[override]
        return object() if self._in_transaction else None

    def commit(self) -> None:  # type: ignore[override]
        self.commits += 1
        self._in_transaction = False

    def rollback(self) -> None:  # type: ignore[override]
        self.rollbacks += 1
        self._in_transaction = False

    def close(self) -> None:  # type: ignore[override]
        self.closed += 1


def _build_request(session: StubSession, scopes) -> SimpleNamespace:
    factory = lambda: session
    app = SimpleNamespace(state=SimpleNamespace(db_sessionmaker=factory))
    state = SimpleNamespace(account_scopes=scopes)
    return SimpleNamespace(app=app, state=state)


def test_db_session_sets_and_resets_account_scopes() -> None:
    connection = StubConnection()
    session = StubSession(connection)
    request = _build_request(session, [" acct-1 ", "acct-2", "acct-1"])

    with db_session(request) as active_session:
        assert active_session is session
        # Account scopes should be normalised and applied once upon entry.
        assert connection.commands == [
            ("SELECT set_config('app.account_scopes', %(scopes)s, true)", {"scopes": "acct-1,acct-2"})
        ]
        assert session.commits == 0

    # Context manager commits and closes the session when exiting cleanly.
    assert session.commits == 1
    assert session.rollbacks == 0
    assert session.closed == 1
    assert connection.execution_calls == [{"isolation_level": "AUTOCOMMIT"}]
    assert connection.commands[-1] == ("RESET app.account_scopes", None)


def test_db_session_rolls_back_on_error() -> None:
    connection = StubConnection()
    session = StubSession(connection)
    request = _build_request(session, ["acct-1"])

    with pytest.raises(RuntimeError):
        with db_session(request):
            raise RuntimeError("boom")

    assert session.commits == 0
    assert session.rollbacks == 1
    assert session.closed == 1
    assert connection.commands[0][0].startswith("SELECT set_config")
    assert connection.commands[-1] == ("RESET app.account_scopes", None)


def test_db_session_requires_account_scopes() -> None:
    connection = StubConnection()
    session = StubSession(connection)
    request = _build_request(session, [])

    with pytest.raises(RuntimeError, match="account scopes"):
        with db_session(request):
            pass

    # Session factory should not have been invoked because scopes were invalid.
    assert session.commits == 0
    assert session.rollbacks == 0
    assert session.closed == 0
    assert connection.commands == []


def test_db_session_noop_for_non_postgres() -> None:
    connection = StubConnection(dialect_name="sqlite")
    session = StubSession(connection)
    request = _build_request(session, ["acct-1"])

    with db_session(request):
        pass

    # No Postgres-specific commands should be emitted for other dialects.
    assert connection.commands == []
    assert connection.execution_calls == []
    assert session.commits == 1
    assert session.closed == 1
