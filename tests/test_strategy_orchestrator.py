from __future__ import annotations

import importlib
from typing import Any, Dict
from types import SimpleNamespace
import sys

import pytest

pytest.importorskip("pydantic")
pytest.importorskip("sqlalchemy")


class _HTTPError(Exception):
    pass


class _HTTPStatusError(_HTTPError):
    def __init__(self, *args: Any, response: Any | None = None, **kwargs: Any) -> None:
        super().__init__(*args)
        self.response = response or SimpleNamespace(status_code=500, text="")


sys.modules.setdefault(
    "httpx",
    SimpleNamespace(AsyncClient=None, HTTPError=_HTTPError, HTTPStatusError=_HTTPStatusError),
)


class _HTTPException(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class _FastAPI:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._startup_handlers = []

    def post(self, *args: Any, **kwargs: Any):  # type: ignore[override]
        def decorator(func):
            return func

        return decorator

    def get(self, *args: Any, **kwargs: Any):  # type: ignore[override]
        def decorator(func):
            return func

        return decorator

    def on_event(self, event: str):
        def decorator(func):
            if event == "startup":
                self._startup_handlers.append(func)
            return func

        return decorator


def _depends(*args: Any, **kwargs: Any) -> Any:
    return None


sys.modules.setdefault(
    "fastapi",

    SimpleNamespace(FastAPI=_FastAPI, HTTPException=_HTTPException, Depends=_depends),

)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import OperationalError

import strategy_orchestrator
from services.common.schemas import (
    FeeBreakdown,
    PolicyDecisionPayload,
    PolicyDecisionRequest,
    PortfolioState,
    RiskIntentMetrics,
    RiskIntentPayload,
    RiskValidationRequest,
)


class _DummyResponse:
    def __init__(self, payload: Dict[str, Any] | None = None) -> None:
        self._payload = payload or {
            "valid": True,
            "reasons": [],
            "fee": {"currency": "USD", "maker": 0.0, "taker": 0.0},
        }

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Dict[str, Any]:
        return self._payload


class _DummyAsyncClient:
    def __init__(self, captured: Dict[str, Any], *args: Any, **kwargs: Any) -> None:
        self._captured = captured

    async def __aenter__(self) -> "_DummyAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        return None

    async def post(self, url: str, *, json: Dict[str, Any], headers: Dict[str, str] | None = None) -> _DummyResponse:
        self._captured["url"] = url
        self._captured["json"] = json
        self._captured["headers"] = headers
        return _DummyResponse()


@pytest.fixture
def registry(monkeypatch: pytest.MonkeyPatch) -> tuple[strategy_orchestrator.StrategyRegistry, Dict[str, Any]]:
    engine = create_engine(
        "sqlite:///:memory:", future=True, connect_args={"check_same_thread": False}, poolclass=NullPool
    )
    strategy_orchestrator.Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)

    captured: Dict[str, Any] = {}
    monkeypatch.setattr(
        strategy_orchestrator.httpx,
        "AsyncClient",
        lambda *args, **kwargs: _DummyAsyncClient(captured),
    )

    registry = strategy_orchestrator.StrategyRegistry(
        session_factory,
        risk_engine_url="https://risk.example.com",
        default_strategies=[("alpha", "Alpha Strategy", 0.5)],
    )
    return registry, captured


def _make_request(account_id: str = "company") -> RiskValidationRequest:
    fee = FeeBreakdown(currency="USD", maker=0.1, taker=0.2)
    policy_request = PolicyDecisionRequest(
        account_id=account_id,
        order_id="abc-123",
        instrument="ETH-USD",
        side="BUY",
        quantity=1.0,
        price=1_000.0,
        fee=fee,
    )
    intent = RiskIntentPayload(
        policy_decision=PolicyDecisionPayload(request=policy_request),
        metrics=RiskIntentMetrics(
            net_exposure=1000.0,
            gross_notional=500.0,
            projected_loss=10.0,
            projected_fee=1.0,
            var_95=50.0,
            spread_bps=5.0,
            latency_ms=10.0,
        ),
    )
    portfolio_state = PortfolioState(
        nav=1_000_000.0,
        loss_to_date=0.0,
        fee_to_date=0.0,
        instrument_exposure={"ETH-USD": 250.0},
    )
    return RiskValidationRequest(
        account_id=account_id,
        intent=intent,
        portfolio_state=portfolio_state,
    )


@pytest.mark.asyncio
async def test_route_trade_intent_forwards_account_header(
    registry: tuple[strategy_orchestrator.StrategyRegistry, Dict[str, Any]]
) -> None:
    orchestrator, captured = registry
    request = _make_request()

    response = await orchestrator.route_trade_intent("alpha", request)

    assert response.valid is True
    assert captured["headers"] == {"X-Account-ID": request.account_id}


@pytest.mark.asyncio
async def test_startup_retry_recovers_after_transient_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.reload(strategy_orchestrator)

    attempts = 0
    real_create_all = module.Base.metadata.create_all

    def flaky_create_all(*args: Any, **kwargs: Any) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise OperationalError("select 1", {}, Exception("db offline"))
        return real_create_all(*args, **kwargs)

    monkeypatch.setattr(module.Base.metadata, "create_all", flaky_create_all)

    ensure_calls = 0

    def ensure_stub(*args: Any, **kwargs: Any) -> None:
        nonlocal ensure_calls
        ensure_calls += 1

    monkeypatch.setattr(module, "ensure_signal_tables", ensure_stub)

    async def immediate_sleep(*_: Any) -> None:
        return None

    monkeypatch.setattr(module.asyncio, "sleep", immediate_sleep)

    await module._initialise_with_retry(max_attempts=5, base_delay=0.0)

    assert attempts == 3
    assert ensure_calls == 1
    assert module.REGISTRY is not None
    assert module.SIGNAL_BUS is not None
    assert module.INITIALIZATION_ERROR is None


@pytest.mark.asyncio
async def test_requests_return_503_until_initialised(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.reload(strategy_orchestrator)
    module.REGISTRY = None
    module.SIGNAL_BUS = None
    module.INITIALIZATION_ERROR = RuntimeError("database unavailable")

    with pytest.raises(module.HTTPException) as excinfo:
        await module.strategy_status()

    assert excinfo.value.status_code == 503
    assert "database unavailable" in excinfo.value.detail
