from datetime import datetime
from typing import List, Mapping

import pytest
from fastapi.testclient import TestClient

import report_service


@pytest.fixture(autouse=True)
def override_auth():
    report_service.app.dependency_overrides[report_service.require_admin_account] = lambda: "alpha"
    yield
    report_service.app.dependency_overrides.pop(report_service.require_admin_account, None)


def test_trade_history_endpoint_streams_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _read_trade_log(
        *, account_id: str | None = None, start=None, end=None, path=None
    ) -> List[Mapping[str, str]]:
        captured["account_id"] = account_id
        captured["start"] = start
        captured["end"] = end
        return [
            {
                "timestamp": "2024-06-10T12:00:00+00:00",
                "account_id": "alpha",
                "client_order_id": "cid-1",
                "exchange_order_id": "ex-1",
                "symbol": "BTC-USD",
                "side": "buy",
                "quantity": "1",
                "price": "25000",
                "pre_trade_mid": "24995",
                "pnl": "15",
                "transport": "websocket",
                "simulated": "false",
            }
        ]

    monkeypatch.setattr(report_service.trade_logging, "read_trade_log", _read_trade_log)

    client = TestClient(report_service.app)
    start = "2024-06-10T00:00:00Z"
    end = "2024-06-11T00:00:00Z"
    response = client.get(
        "/reports/trades",
        params={"account_id": "alpha", "start": start, "end": end},
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "Content-Disposition" in response.headers
    assert "cid-1" in response.text
    assert captured["account_id"] == "alpha"
    assert isinstance(captured["start"], datetime)
    assert isinstance(captured["end"], datetime)


def test_trade_history_endpoint_rejects_cross_account() -> None:
    client = TestClient(report_service.app)
    response = client.get("/reports/trades", params={"account_id": "beta"})

    assert response.status_code == 403
