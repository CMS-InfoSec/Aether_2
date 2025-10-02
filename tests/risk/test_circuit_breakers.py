from __future__ import annotations

from typing import Generator

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from services.common.adapters import TimescaleAdapter
from services.risk.circuit_breakers import (
    CircuitBreakerConfigStore,
    CircuitBreakerMonitor,
    CircuitBreakerSymbolStatus,
    get_circuit_breaker,
    reset_circuit_breakers,
)
from services.risk.main import app
from tests.risk.test_validate import make_request


@pytest.fixture(autouse=True)
def reset_state() -> Generator[None, None, None]:
    TimescaleAdapter.reset()
    CircuitBreakerConfigStore.reset()
    reset_circuit_breakers()
    yield
    TimescaleAdapter.reset()
    CircuitBreakerConfigStore.reset()
    reset_circuit_breakers()


def _configure_thresholds(account_id: str, symbol: str, *, safe_mode: bool = False) -> CircuitBreakerMonitor:
    store = CircuitBreakerConfigStore(account_id)
    store.upsert(
        symbol,
        max_spread_bps=5.0,
        max_volatility=0.001,
        trigger_safe_mode=safe_mode,
        actor_id="director-1",
    )
    adapter = TimescaleAdapter(account_id=account_id)
    monitor = get_circuit_breaker(account_id, timescale=adapter)
    return monitor


def test_circuit_breaker_blocks_on_spread_exceedance() -> None:
    account = "company"
    symbol = "ETH-USD"
    monitor = _configure_thresholds(account, symbol)
    monitor.record_quote(symbol, bid=100.0, ask=101.0)

    request = make_request(account_id=account, instrument=symbol, spread_bps=25.0)
    decision = monitor.evaluate(request)

    assert decision is not None
    assert symbol in decision.reason
    status = monitor.status()
    assert status and isinstance(status[0], CircuitBreakerSymbolStatus)


def test_circuit_breaker_allows_hedges_when_blocked() -> None:
    account = "company"
    symbol = "ETH-USD"
    monitor = _configure_thresholds(account, symbol)
    monitor.record_quote(symbol, bid=100.0, ask=101.0)

    request = make_request(account_id=account, instrument=symbol, spread_bps=25.0)
    request.portfolio_state.metadata["hedge"] = True

    decision = monitor.evaluate(request)
    assert decision is None


def test_circuit_breaker_status_endpoint_returns_blocked_symbols() -> None:
    account = "company"
    symbol = "ETH-USD"
    monitor = _configure_thresholds(account, symbol)
    monitor.record_quote(symbol, bid=100.0, ask=101.0)
    request = make_request(account_id=account, instrument=symbol, spread_bps=25.0)
    monitor.evaluate(request)

    client = TestClient(app)
    response = client.get(
        "/risk/circuit/status",
        params={"account_id": account},
        headers={"X-Account-ID": account},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["account_id"] == account
    assert payload["blocked"]
    assert payload["blocked"][0]["symbol"] == symbol


def test_circuit_breaker_configuration_updates_are_audited() -> None:
    account = "company"
    store = CircuitBreakerConfigStore(account)
    store.upsert(
        "BTC-USD",
        max_spread_bps=7.5,
        max_volatility=0.002,
        trigger_safe_mode=False,
        actor_id="director-2",
    )
    entries = tuple(CircuitBreakerConfigStore.audit_entries())
    assert entries
    assert entries[-1].action == "risk.circuit_breaker.update"


def test_circuit_breaker_triggers_safe_mode_when_configured() -> None:
    account = "company"
    symbol = "ETH-USD"
    monitor = _configure_thresholds(account, symbol, safe_mode=True)
    monitor.record_quote(symbol, bid=100.0, ask=101.0)

    request = make_request(account_id=account, instrument=symbol, spread_bps=25.0)
    decision = monitor.evaluate(request)

    assert decision is not None
    assert decision.safe_mode_engaged is True
    config = TimescaleAdapter(account_id=account).load_risk_config()
    assert config["safe_mode"] is True

