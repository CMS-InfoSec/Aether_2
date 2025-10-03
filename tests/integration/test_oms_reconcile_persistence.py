import asyncio
import json
import os
import uuid
from typing import Dict, Iterator

import pytest

psycopg = pytest.importorskip(
    "psycopg", reason="psycopg is required for OMS reconciliation persistence tests"
)

pytestmark = pytest.mark.integration

_TEST_DSN = os.getenv("AETHER_OMS_TEST_DSN") or os.getenv("AETHER_TIMESCALE_TEST_DSN")
if not _TEST_DSN:
    pytest.skip(
        "AETHER_OMS_TEST_DSN or AETHER_TIMESCALE_TEST_DSN must be configured for OMS reconciliation tests",
        allow_module_level=True,
    )


def _with_schema(dsn: str, schema: str) -> str:
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit, quote_plus

    parts = urlsplit(dsn)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["options"] = f"-csearch_path={schema}"
    new_query = urlencode(query, doseq=True, quote_via=quote_plus)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


@pytest.fixture
def reconcile_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[Dict[str, str]]:
    schema = f"oms_reconcile_int_{uuid.uuid4().hex[:8]}"
    dsn = _with_schema(_TEST_DSN, schema)

    with psycopg.connect(_TEST_DSN, autocommit=True) as conn:  # type: ignore[arg-type]
        with conn.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            cursor.execute(f'CREATE SCHEMA "{schema}"')

    monkeypatch.setenv("TIMESCALE_DSN", dsn)
    monkeypatch.delenv("OMS_RECONCILE_DSN", raising=False)

    try:
        yield {"schema": schema, "dsn": dsn}
    finally:
        with psycopg.connect(_TEST_DSN, autocommit=True) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.mark.asyncio
async def test_reconcile_logs_persist_and_are_shared(reconcile_environment: Dict[str, str]) -> None:
    from services.oms.reconcile import ReconcileLogStore

    primary = ReconcileLogStore()
    secondary = ReconcileLogStore()

    await primary.record("acct-primary", {"balances_remaining": ["usd"]})
    await secondary.record("acct-secondary", {"orders_remaining": ["ABC123"]})

    await asyncio.gather(
        primary.record("acct-primary", {"balances_fixed": ["usd"]}),
        secondary.record("acct-secondary", {"orders_fixed": ["ABC123"]}),
    )

    with psycopg.connect(reconcile_environment["dsn"]) as conn:  # type: ignore[arg-type]
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT account_id, mismatches, mismatches_json FROM reconcile_log ORDER BY ts"
            )
            rows = cursor.fetchall()

    assert len(rows) == 4, "Expected all reconciliation entries to be persisted"

    observed_accounts = [row[0] for row in rows]
    assert observed_accounts.count("acct-primary") == 2
    assert observed_accounts.count("acct-secondary") == 2

    payloads = []
    for _, mismatch_count, payload in rows:
        if isinstance(payload, memoryview):
            payload = payload.tobytes().decode("utf-8")
        if isinstance(payload, str):
            payload = json.loads(payload)
        payloads.append(payload)
        expected_count = sum(len(values) for values in payload.values())
        assert mismatch_count == expected_count

    assert any(entry.get("balances_remaining") == ["usd"] for entry in payloads)
    assert any(entry.get("orders_remaining") == ["ABC123"] for entry in payloads)
    assert any(entry.get("balances_fixed") == ["usd"] for entry in payloads)
    assert any(entry.get("orders_fixed") == ["ABC123"] for entry in payloads)
