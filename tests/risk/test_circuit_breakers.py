from __future__ import annotations

from typing import Generator

import atexit
from copy import deepcopy
import os
import tempfile
from pathlib import Path

import pytest

from tests.helpers.risk import MANAGED_RISK_DSN, patch_sqlalchemy_for_risk

from services.common import adapters as adapters_module

os.environ.setdefault("COMPLIANCE_ALLOW_SQLITE_FOR_TESTS", "1")
os.environ.setdefault("COMPLIANCE_DATABASE_URL", MANAGED_RISK_DSN)
os.environ.setdefault("RISK_DATABASE_URL", MANAGED_RISK_DSN)
os.environ.setdefault("ESG_DATABASE_URL", MANAGED_RISK_DSN)

_restore_sqlalchemy = patch_sqlalchemy_for_risk(
    Path(tempfile.gettempdir()) / "circuit-breaker-tests.db"
)
atexit.register(_restore_sqlalchemy)


class _StubTimescaleAdapter:
    _configs: dict[str, dict] = {}
    _events: dict[str, list] = {}
    _audit_logs: list[dict] = []

    def __init__(self, account_id: str, *_, **__) -> None:
        self.account_id = account_id
        self._configs.setdefault(account_id, {})
        self._events.setdefault(account_id, [])

    @classmethod
    def reset(cls, account_id: str | None = None) -> None:
        if account_id is None:
            cls._configs.clear()
            cls._events.clear()
            cls._audit_logs.clear()
            return
        cls._configs.pop(account_id, None)
        cls._events.pop(account_id, None)

    def load_risk_config(self) -> dict:
        config = self._configs.setdefault(
            self.account_id, {"circuit_breakers": {}, "safe_mode": False}
        )
        return deepcopy(config)

    def save_risk_config(self, config: dict) -> None:
        self._configs[self.account_id] = deepcopy(config)

    def record_event(self, event_type: str, payload: dict) -> None:
        self._events[self.account_id].append((event_type, deepcopy(payload)))

    def set_safe_mode(self, *, engaged: bool, reason: str, actor: str) -> None:
        config = self._configs.setdefault(
            self.account_id, {"circuit_breakers": {}, "safe_mode": False}
        )
        config["safe_mode"] = bool(engaged)

    def record_audit_log(self, record: dict) -> None:  # pragma: no cover - compatibility stub
        self._events[self.account_id].append(("audit", deepcopy(record)))
        self._audit_logs.append(deepcopy(record))

    def audit_logs(self) -> list[dict]:  # pragma: no cover - compatibility stub
        return list(self._audit_logs)

    @classmethod
    async def flush_event_buffers(cls) -> None:  # pragma: no cover - compatibility stub
        return None


adapters_module.TimescaleAdapter = _StubTimescaleAdapter  # type: ignore[attr-defined]
TimescaleAdapter = _StubTimescaleAdapter  # type: ignore[assignment]

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
    assert decision.symbol == symbol
    assert "Spread" in decision.reason
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


def test_circuit_breaker_config_rejects_derivative_symbols() -> None:
    store = CircuitBreakerConfigStore("company")
    with pytest.raises(ValueError):
        store.upsert(
            "BTC-PERP",
            max_spread_bps=5.0,
            max_volatility=0.001,
            trigger_safe_mode=False,
            actor_id="director-3",
        )


def test_circuit_breaker_ignores_non_spot_quotes_and_requests() -> None:
    account = "company"
    monitor = _configure_thresholds(account, "ETH-USD")

    monitor.record_quote("ETH-PERP", bid=100.0, ask=101.0)
    assert "ETH-PERP" not in monitor._quotes  # type: ignore[attr-defined]

    request = make_request(account_id=account, instrument="ETH-USD", spread_bps=10.0)
    request.instrument = "ETH-PERP"
    request.intent.policy_decision.request.instrument = "ETH-PERP"
    monitor.observe_request(request)
    assert monitor.evaluate(request) is None
    assert monitor.status() == []

