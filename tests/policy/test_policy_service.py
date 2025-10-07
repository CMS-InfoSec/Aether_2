from __future__ import annotations

import contextlib
import importlib
import os
import sys
import types
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Iterator
import pytest
from fastapi import status
from fastapi.testclient import TestClient

from auth.service import InMemorySessionStore

if "metrics" not in sys.modules:
    metrics_stub = types.ModuleType("metrics")
    def _setup_metrics(app, *args, **kwargs):
        if not any(route.path == "/metrics" for route in app.routes):
            @app.get("/metrics")  # pragma: no cover - simple stub endpoint
            def _metrics_endpoint():
                return {"status": "ok"}

    metrics_stub.setup_metrics = _setup_metrics
    metrics_stub.record_abstention_rate = lambda *args, **kwargs: None
    metrics_stub.record_drift_score = lambda *args, **kwargs: None
    metrics_stub.record_scaling_state = lambda *args, **kwargs: None
    metrics_stub.observe_scaling_evaluation = lambda *args, **kwargs: None
    metrics_stub.get_request_id = lambda: None

    class _TransportMember:
        def __init__(self, value: str) -> None:
            self.value = value

        def __str__(self) -> str:
            return self.value

    class _TransportType:
        UNKNOWN = _TransportMember("unknown")
        INTERNAL = _TransportMember("internal")
        REST = _TransportMember("rest")
        WEBSOCKET = _TransportMember("websocket")
        FIX = _TransportMember("fix")
        BATCH = _TransportMember("batch")

    metrics_stub.TransportType = _TransportType
    metrics_stub.bind_metric_context = lambda *args, **kwargs: contextlib.nullcontext()
    metrics_stub.metric_context = lambda *args, **kwargs: contextlib.nullcontext()
    sys.modules["metrics"] = metrics_stub

os.environ.setdefault("SESSION_REDIS_URL", "memory://policy-tests")

import services.common.security as security
from services.common.schemas import ActionTemplate, ConfidenceMetrics, PolicyDecisionResponse

import policy_service


@pytest.fixture(autouse=True)
def _reset_regime() -> None:
    policy_service._reset_regime_state()
    yield
    policy_service._reset_regime_state()


@dataclass
class DataclassIntent:
    edge_bps: float
    confidence: ConfidenceMetrics
    take_profit_bps: float
    stop_loss_bps: float
    selected_action: str
    action_templates: list[ActionTemplate]
    approved: bool
    reason: str | None = None


@pytest.fixture(name="session_store")
def _session_store() -> Iterator[InMemorySessionStore]:
    previous_store = getattr(policy_service.app.state, "session_store", None)
    previous_global = getattr(policy_service, "SESSION_STORE", None)
    previous_default = getattr(security, "_DEFAULT_SESSION_STORE", None)

    store = InMemorySessionStore()
    policy_service.app.state.session_store = store
    policy_service.SESSION_STORE = store
    security.set_default_session_store(store)

    try:
        yield store
    finally:
        if previous_store is None:
            policy_service.app.state.session_store = None
        else:
            policy_service.app.state.session_store = previous_store
        policy_service.SESSION_STORE = previous_global
        security.set_default_session_store(previous_default)


@pytest.fixture(name="client")
def _client(
    session_store: InMemorySessionStore,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[TestClient]:
    del session_store  # Fixture ensures session store configured
    monkeypatch.setattr(
        "shared.graceful_shutdown.install_sigterm_handler",
        lambda manager: None,
    )
    async def _noop_place_order(*args: object, **kwargs: object) -> dict[str, str]:
        return {"order_id": "test"}

    monkeypatch.setattr(
        policy_service.EXCHANGE_ADAPTER,
        "place_order",
        _noop_place_order,
    )
    with TestClient(policy_service.app) as client:
        yield client


@pytest.fixture
def auth_headers(session_store: InMemorySessionStore) -> Callable[[str], dict[str, str]]:
    def _build(account: str) -> dict[str, str]:
        session = session_store.create(account)
        return {
            "Authorization": f"Bearer {session.token}",
            "X-Account-ID": account,
        }

    return _build


def test_decide_policy_accepts_dataclass_intent(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
    auth_headers: Callable[[str], dict[str, str]],
) -> None:
    intent = DataclassIntent(
        edge_bps=20.0,
        confidence=ConfidenceMetrics(
            model_confidence=0.4,
            state_confidence=0.4,
            execution_confidence=0.4,
            overall_confidence=0.4,
        ),
        take_profit_bps=30.0,
        stop_loss_bps=10.0,
        selected_action="maker",
        action_templates=[
            ActionTemplate(
                name="maker",
                venue_type="maker",
                edge_bps=20.0,
                fee_bps=0.0,
                confidence=0.5,
            ),
            ActionTemplate(
                name="taker",
                venue_type="taker",
                edge_bps=18.0,
                fee_bps=0.0,
                confidence=0.45,
            ),
        ],
        approved=True,
    )

    async def _fake_fee(
        account_id: str, symbol: str, liquidity: str, notional: float | Decimal
    ) -> Decimal:
        return {"maker": Decimal("4.0"), "taker": Decimal("7.0")}[liquidity]

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: intent)
    policy_service._ATR_CACHE.clear()

    account_id = "company"
    payload = {
        "account_id": account_id,
        "order_id": "unit-test",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 0.5,
        "price": 100.0,
        "fee": {"currency": "USD", "maker": 4.0, "taker": 6.0},
        "features": [0.1, -0.2, 0.3],
        "book_snapshot": {"mid_price": 100.5, "spread_bps": 2.0, "imbalance": 0.1},
        "state": {
            "regime": "expansion",
            "volatility": 0.2,
            "liquidity_score": 0.9,
            "conviction": 0.7,
        },
        "confidence": {
            "model_confidence": 0.9,
            "state_confidence": 0.85,
            "execution_confidence": 0.88,
        },
    }

    response = client.post("/policy/decide", json=payload, headers=auth_headers(account_id))
    assert response.status_code == 200

    body = PolicyDecisionResponse.model_validate(response.json())
    assert body.approved is True
    assert body.selected_action == "maker"
    assert body.expected_edge_bps == pytest.approx(20.0)
    assert body.fee_adjusted_edge_bps == pytest.approx(16.0)
    assert body.take_profit_bps == pytest.approx(30.0)
    assert body.stop_loss_bps == pytest.approx(10.0)


def test_metrics_endpoint_exposed_after_import():
    client = TestClient(policy_service.app)

    response = client.get("/metrics")

    assert response.status_code == 200


def test_regime_endpoint_returns_latest_snapshot(
    client: TestClient,
    auth_headers: Callable[[str], dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    intent = DataclassIntent(
        edge_bps=18.0,
        confidence=ConfidenceMetrics(
            model_confidence=0.7,
            state_confidence=0.68,
            execution_confidence=0.66,
            overall_confidence=0.7,
        ),
        take_profit_bps=25.0,
        stop_loss_bps=12.0,
        selected_action="maker",
        action_templates=[
            ActionTemplate(
                name="maker",
                venue_type="maker",
                edge_bps=18.0,
                fee_bps=0.0,
                confidence=0.7,
            ),
            ActionTemplate(
                name="taker",
                venue_type="taker",
                edge_bps=16.0,
                fee_bps=0.0,
                confidence=0.65,
            ),
        ],
        approved=True,
    )

    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: intent)

    async def _fake_fee(
        account_id: str, symbol: str, liquidity: str, notional: float | Decimal
    ) -> Decimal:
        return Decimal("3.0")

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    account_id = "company"
    base_payload = {
        "account_id": account_id,
        "order_id": "regime-test",
        "instrument": "ETH-USD",
        "side": "BUY",
        "quantity": 0.5,
        "price": 1500.0,
        "fee": {"currency": "USD", "maker": 2.0, "taker": 4.0},
        "features": [0.2, 0.1, -0.05],
        "book_snapshot": {"mid_price": 1500.0, "spread_bps": 1.5, "imbalance": 0.05},
    }

    for idx, mid_price in enumerate([1500.0, 1502.0, 1504.5, 1507.0, 1510.0, 1512.5], start=1):
        payload = dict(base_payload)
        payload["order_id"] = f"regime-{idx}"
        payload["book_snapshot"] = {
            "mid_price": mid_price,
            "spread_bps": 1.5,
            "imbalance": 0.05,
        }
        client.post("/policy/decide", json=payload, headers=auth_headers(account_id))
        policy_service.regime_classifier.observe(payload["instrument"], mid_price)

    response = client.get(
        "/policy/regime",
        params={"symbol": "ETH-USD", "account_id": account_id},
        headers=auth_headers(account_id),
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "ETH-USD"
    assert payload["regime"] in {"trend", "range", "high_vol"}
    assert payload["sample_count"] >= 5


def test_regime_endpoint_requires_authentication(client: TestClient) -> None:
    response = client.get("/policy/regime", params={"symbol": "BTC-USD"})
    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_regime_endpoint_rejects_mismatched_account(
    client: TestClient,
    auth_headers: Callable[[str], dict[str, str]],
) -> None:
    headers = auth_headers("company")
    headers["X-Account-ID"] = "intruder"

    response = client.get(
        "/policy/regime",
        params={"symbol": "ETH-USD", "account_id": "intruder"},
        headers=headers,
    )

    assert response.status_code == status.HTTP_403_FORBIDDEN


def test_regime_endpoint_allows_authenticated_access(
    client: TestClient,
    auth_headers: Callable[[str], dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    intent = DataclassIntent(
        edge_bps=22.0,
        confidence=ConfidenceMetrics(
            model_confidence=0.8,
            state_confidence=0.78,
            execution_confidence=0.76,
            overall_confidence=0.8,
        ),
        take_profit_bps=28.0,
        stop_loss_bps=14.0,
        selected_action="maker",
        action_templates=[
            ActionTemplate(
                name="maker",
                venue_type="maker",
                edge_bps=22.0,
                fee_bps=0.0,
                confidence=0.8,
            ),
            ActionTemplate(
                name="taker",
                venue_type="taker",
                edge_bps=18.0,
                fee_bps=0.0,
                confidence=0.72,
            ),
        ],
        approved=True,
    )

    monkeypatch.setattr(policy_service, "predict_intent", lambda **_: intent)

    async def _fake_fee(
        account_id: str, symbol: str, liquidity: str, notional: float | Decimal
    ) -> Decimal:
        return Decimal("2.5")

    monkeypatch.setattr(policy_service, "_fetch_effective_fee", _fake_fee)
    account_id = "company"
    payload = {
        "account_id": account_id,
        "order_id": "regime-auth",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 0.25,
        "price": 30250.0,
        "fee": {"currency": "USD", "maker": 2.0, "taker": 4.0},
        "features": [0.1, 0.2, -0.1],
        "book_snapshot": {"mid_price": 30250.0, "spread_bps": 1.0, "imbalance": 0.02},
    }

    client.post("/policy/decide", json=payload, headers=auth_headers(account_id))
    policy_service.regime_classifier.observe(payload["instrument"], payload["book_snapshot"]["mid_price"])

    response = client.get(
        "/policy/regime",
        params={"symbol": "BTC-USD", "account_id": account_id},
        headers=auth_headers(account_id),
    )

    assert response.status_code == status.HTTP_200_OK


def test_policy_service_requires_session_store_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "policy_service"
    cached_module = sys.modules.pop(module_name, None)
    monkeypatch.delenv("SESSION_REDIS_URL", raising=False)

    with pytest.raises(RuntimeError, match="SESSION_REDIS_URL is not configured"):
        importlib.import_module(module_name)

    sys.modules.pop(module_name, None)
    if cached_module is not None:
        sys.modules[module_name] = cached_module
    else:
        monkeypatch.setenv("SESSION_REDIS_URL", "memory://policy-tests")
        importlib.import_module(module_name)


def test_policy_service_rejects_memory_session_store_outside_pytest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi import FastAPI

    app = FastAPI()
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://policy-prod")
    cached_pytest = sys.modules.get("pytest")
    if cached_pytest is not None:
        sys.modules.pop("pytest")

    try:
        with pytest.raises(RuntimeError, match="must use a redis:// or rediss:// DSN outside pytest"):
            policy_service._configure_session_store(app)
    finally:
        if cached_pytest is not None:
            sys.modules["pytest"] = cached_pytest


