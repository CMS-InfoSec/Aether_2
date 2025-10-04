from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from tests.helpers.benchmark_service import bootstrap_benchmark_service


def _record_snapshot(
    client: TestClient,
    account_id: str,
    ts: datetime,
    *,
    aether: float,
    btc: float,
    eth: float,
    basket: float | None = None,
) -> None:
    payload = {
        "account_id": account_id,
        "ts": ts.isoformat(),
        "aether_return": aether,
        "btc_return": btc,
        "eth_return": eth,
    }
    if basket is not None:
        payload["basket_return"] = basket
    response = client.post(
        "/benchmark/curves",
        json=payload,
        headers={"X-Account-ID": "company"},
    )
    assert response.status_code == 204


def _compare_snapshot(client: TestClient, account_id: str, when: datetime) -> dict[str, float]:
    response = client.get(
        "/benchmark/compare",
        params={"account_id": account_id, "date": when.isoformat()},
        headers={"X-Account-ID": "company"},
    )
    assert response.status_code == 200
    return response.json()


@pytest.mark.smoke
def test_benchmark_snapshots_persist_across_restarts_and_replicas(tmp_path, monkeypatch) -> None:
    """Snapshots persist across restarts and are visible to new replicas."""

    db_filename = "benchmark-smoke.db"
    account = "acct-123"
    first_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    second_ts = first_ts + timedelta(days=1)

    module_a = bootstrap_benchmark_service(tmp_path, monkeypatch, reset=True, db_filename=db_filename)
    with TestClient(module_a.app) as client_a:
        _record_snapshot(
            client_a,
            account,
            first_ts,
            aether=0.12,
            btc=0.05,
            eth=0.03,
            basket=0.04,
        )
        initial = _compare_snapshot(client_a, account, first_ts)
        assert initial["aether_return"] == pytest.approx(0.12)
        assert initial["basket_return"] == pytest.approx(0.04)
        assert initial["excess_return"] == pytest.approx(initial["aether_return"] - initial["basket_return"])
    module_a.ENGINE.dispose()

    module_b = bootstrap_benchmark_service(tmp_path, monkeypatch, db_filename=db_filename)
    with TestClient(module_b.app) as client_b:
        persisted = _compare_snapshot(client_b, account, first_ts)
        assert persisted["btc_return"] == pytest.approx(0.05)
        _record_snapshot(
            client_b,
            account,
            second_ts,
            aether=0.2,
            btc=0.06,
            eth=0.04,
        )
    module_b.ENGINE.dispose()

    module_c = bootstrap_benchmark_service(tmp_path, monkeypatch, db_filename=db_filename)
    with TestClient(module_c.app) as client_c:
        latest = _compare_snapshot(client_c, account, second_ts)
        assert latest["aether_return"] == pytest.approx(0.2)
        assert latest["basket_return"] == pytest.approx(0.05)
        assert latest["excess_return"] == pytest.approx(latest["aether_return"] - latest["basket_return"])

        historic = _compare_snapshot(client_c, account, first_ts)
        assert historic["aether_return"] == pytest.approx(0.12)
    module_c.ENGINE.dispose()

    sys.modules.pop("benchmark_service", None)

