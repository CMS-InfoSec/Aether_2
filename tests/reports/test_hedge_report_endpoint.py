from datetime import datetime
from typing import Any, Mapping

from fastapi.testclient import TestClient

import report_service
def setup_module() -> None:  # pragma: no cover - pytest hook
    report_service.app.dependency_overrides[report_service.require_admin_account] = (
        lambda: "alpha"
    )


def teardown_module() -> None:  # pragma: no cover - pytest hook
    report_service.app.dependency_overrides.pop(
        report_service.require_admin_account, None
    )


def test_hedge_report_returns_summary(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _read_hedge_log(
        *, account_id: str | None = None, start=None, end=None, path=None
    ) -> list[Mapping[str, str]]:
        captured["account_id"] = account_id
        captured["start"] = start
        captured["end"] = end
        return [
            {
                "timestamp": "2024-06-11T12:00:00+00:00",
                "account_id": "alpha",
                "symbol": "USDT-USD",
                "side": "buy",
                "previous_allocation": "100000",
                "target_allocation": "125000",
                "delta_usd": "25000",
                "quantity": "24950",
                "price": "1.002",
                "risk_score": "1.5",
                "drawdown_pct": "0.12",
                "atr": "15.2",
                "realized_vol": "0.35",
                "order_id": "HEDGE-xyz",
            }
        ]

    monkeypatch.setattr(report_service.hedge_logging, "read_hedge_log", _read_hedge_log)

    client = TestClient(report_service.app)
    start = "2024-06-10T00:00:00Z"
    end = "2024-06-12T00:00:00Z"
    response = client.get(
        "/reports/hedge",
        params={"account_id": "alpha", "start": start, "end": end},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["account_id"] == "alpha"
    assert payload["summary"]["entries"] == 1
    assert payload["summary"]["current_allocation"] == 125000.0
    assert payload["summary"]["last_side"] == "buy"
    assert payload["summary"]["total_notional_delta"] == 25000.0
    assert payload["summary"]["max_drawdown_pct"] == 0.12
    assert payload["history"][0]["order_id"] == "HEDGE-xyz"

    assert captured["account_id"] == "alpha"
    assert isinstance(captured["start"], datetime)
    assert isinstance(captured["end"], datetime)


def test_hedge_report_rejects_cross_account() -> None:
    client = TestClient(report_service.app)
    response = client.get("/reports/hedge", params={"account_id": "beta"})

    assert response.status_code == 403
