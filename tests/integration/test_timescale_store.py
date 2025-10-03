"""Integration tests for the Timescale-backed telemetry store."""

from __future__ import annotations

import os
import uuid
from typing import Callable

import pytest

from services.common.adapters import _TimescaleStore
from services.common.config import TimescaleSession

pytestmark = pytest.mark.integration


@pytest.fixture
def timescale_environment() -> tuple[_TimescaleStore, Callable[[str], TimescaleSession], str, str]:
    psycopg = pytest.importorskip("psycopg")
    dsn = os.getenv("AETHER_TIMESCALE_TEST_DSN")
    if not dsn:
        pytest.skip("AETHER_TIMESCALE_TEST_DSN is not configured for integration tests")

    schema = f"acct_test_{uuid.uuid4().hex[:8]}"
    account_id = "integration-test"

    def _session_factory(_: str) -> TimescaleSession:
        return TimescaleSession(dsn=dsn, account_schema=schema)

    store = _TimescaleStore(
        account_id,
        session_factory=_session_factory,
        max_retries=3,
        backoff_seconds=0.05,
    )

    yield store, _session_factory, dsn, schema

    _TimescaleStore.reset_account(account_id)
    with psycopg.connect(dsn) as conn:  # type: ignore[arg-type]
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_events_persist_across_instances(timescale_environment: tuple[_TimescaleStore, Callable[[str], TimescaleSession], str, str]) -> None:
    store, session_factory, _, _ = timescale_environment

    store.record_ack({"order_id": "abc-123"})
    store.record_event("order.created", {"foo": "bar"})

    # A second adapter instance sharing the same account should be able to read the data.
    peer = _TimescaleStore(
        store.account_id,
        session_factory=session_factory,
        max_retries=3,
        backoff_seconds=0.05,
    )
    peer_events = peer.fetch_events()
    assert peer_events["acks"], "Expected ack events to be visible to peer instances"
    assert peer_events["events"], "Expected generic events to be visible to peer instances"

    # Simulate a process restart by dropping the cached connection state.
    with _TimescaleStore._lock:  # type: ignore[attr-defined]
        state = _TimescaleStore._connections.pop(store.account_id, None)  # type: ignore[attr-defined]
    if state is not None:
        state.connection.close()

    restarted = _TimescaleStore(
        store.account_id,
        session_factory=session_factory,
        max_retries=3,
        backoff_seconds=0.05,
    )
    persisted = restarted.fetch_events()
    assert persisted["acks"], "Ack events should persist after restart"
    assert persisted["events"], "Generic events should persist after restart"
