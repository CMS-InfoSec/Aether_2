from __future__ import annotations

from decimal import Decimal
import sys
import types

import pytest
from fastapi.testclient import TestClient

if "metrics" not in sys.modules:
    metrics_stub = types.ModuleType("metrics")
    metrics_stub.setup_metrics = lambda app: None
    metrics_stub.record_abstention_rate = lambda *args, **kwargs: None
    metrics_stub.record_drift_score = lambda *args, **kwargs: None
    sys.modules["metrics"] = metrics_stub

import policy_service
from services.common.schemas import ActionTemplate, ConfidenceMetrics
from services.models.model_server import Intent


@pytest.fixture(name="client")
def _client() -> TestClient:
    return TestClient(policy_service.app)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _intent(
    *,
    edge_bps: float,
    maker_edge: float,
    taker_edge: float,
    approved: bool,
    selected: str,
    reason: str | None = None,
) -> Intent:
    return Intent(
        edge_bps=edge_bps,
        confidence=ConfidenceMetrics(
            model_confidence=0.8,
            state_confidence=0.78,
            execution_confidence=0.76,
            overall_confidence=0.8,
        ),
        take_profit_bps=25.0,
        stop_loss_bps=12.0,
        selected_action=selected,
        action_templates=[
            ActionTemplate(
                name="maker",
                venue_type="maker",
                edge_bps=maker_edge,
                fee_bps=0.0,
                confidence=0.9,
            ),
            ActionTemplate(
                name="taker",
                venue_type="taker",
                edge_bps=taker_edge,
                fee_bps=0.0,
                confidence=0.85,
            ),
        ],
        approved=approved,
        reason=reason,
    )


def test_policy_decide_approves_when_edge_beats_costs(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    recorded: dict[str, object] = {}

    async def _fake_fee(account_id: str, symbol: str, liquidity: str, notional: float) -> float:
        recorded.update(
            {
                "account_id": account_id,
                "symbol": symbol,
                "liquidity": liquidity,
                "notional": notional,
            }
        )
        return 4.5

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    monkeypatch.setattr(
        policy_service,
        "predict_intent",
        lambda **_: _intent(
            edge_bps=22.0,
            maker_edge=18.0,
            taker_edge=12.0,
            approved=True,
            selected="maker",
        ),
    )

    payload = {
        "account_id": "company",
        "symbol": "btc-usd",
        "side": "buy",
        "qty": 0.1234567,
        "price": 30120.4567,
        "impact_bps": 1.0,
        "features": {"alpha": 0.4, "beta": -0.1, "gamma": 2.8},
        "book_snapshot": {"mid_price": 30125.4, "spread_bps": 2.4, "imbalance": 0.05},
    }

    response = client.post("/policy/decide", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["approved"] is True
    assert data["action"] == "maker"
    assert data["side"] == "buy"
    assert data["qty"] == pytest.approx(0.1235)
    assert data["price"] == pytest.approx(30120.5)
    assert data["limit_px"] == pytest.approx(30120.5)
    assert data["effective_fee_bps"] == pytest.approx(4.5)
    assert data["expected_edge_bps"] == pytest.approx(18.0)
    assert data["expected_cost_bps"] == pytest.approx(7.9)

    expected_notional = float(Decimal("30120.5") * Decimal("0.1235"))
    assert recorded == {
        "account_id": "company",
        "symbol": "BTC-USD",
        "liquidity": "maker",
        "notional": pytest.approx(expected_notional),
    }


def test_policy_decide_rejects_when_costs_exceed_edge(
    monkeypatch: pytest.MonkeyPatch, client: TestClient
) -> None:
    async def _fake_fee(*_: object, **__: object) -> float:
        return 9.0

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    monkeypatch.setattr(
        policy_service,
        "predict_intent",
        lambda **_: _intent(
            edge_bps=10.0,
            maker_edge=8.0,
            taker_edge=6.0,
            approved=True,
            selected="maker",
        ),
    )

    payload = {
        "account_id": "company",
        "symbol": "BTC-USD",
        "side": "sell",
        "qty": 0.25,
        "price": 25000.0,
        "impact_bps": 3.0,
        "features": [0.1, 0.2],
        "book_snapshot": {"mid_price": 25010.0, "spread_bps": 4.0, "imbalance": -0.2},
    }

    response = client.post("/policy/decide", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["approved"] is False
    assert data["action"] == "hold"
    assert data["side"] == "none"
    assert data["qty"] == 0.0
    assert data["reason"] == "Fee-adjusted edge non-positive"


@pytest.mark.anyio("asyncio")
async def test_fetch_effective_fee_parses_flat_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    response_payload = {
        "bps": "5.25",
        "usd": 1.23,
        "tier_id": "tier_1",
        "basis_ts": "2023-01-01T00:00:00+00:00",
    }
    recorded: dict[str, object] = {}

    monkeypatch.setattr(policy_service, "FEES_SERVICE_URL", "https://fees.test", raising=False)
    monkeypatch.setattr(policy_service, "FEES_REQUEST_TIMEOUT", 1.5, raising=False)

    class _DummyResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            recorded["status_checked"] = True

        def json(self) -> dict[str, object]:
            return self._payload

    class _DummyClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            recorded["client_kwargs"] = kwargs

        async def __aenter__(self) -> "_DummyClient":
            recorded["entered"] = True
            return self

        async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: object,
        ) -> None:
            recorded["exited"] = True

        async def get(
            self,
            path: str,
            *,
            params: dict[str, object] | None = None,
            headers: dict[str, str] | None = None,
        ) -> _DummyResponse:
            recorded["request"] = {
                "path": path,
                "params": params,
                "headers": headers,
            }
            return _DummyResponse(response_payload)

    monkeypatch.setattr(policy_service.httpx, "AsyncClient", _DummyClient)

    fee = await policy_service._fetch_effective_fee(
        account_id="acct-123",
        symbol="ETH-USD",
        liquidity="MaKeR",
        notional=123.456789,
    )

    assert fee == pytest.approx(5.25)
    request = recorded["request"]
    assert request["path"] == "/fees/effective"
    assert request["headers"] == {"X-Account-ID": "acct-123"}
    assert request["params"] == {
        "pair": "ETH-USD",
        "liquidity": "maker",
        "notional": "123.45678900",
    }
    assert recorded["status_checked"] is True


def test_health_endpoints(client: TestClient) -> None:
    health = client.get("/health")
    ready = client.get("/ready")

    assert health.status_code == 200
    assert ready.status_code == 200
    assert health.json() == {"status": "ok"}
    assert ready.json() == {"status": "ready"}

