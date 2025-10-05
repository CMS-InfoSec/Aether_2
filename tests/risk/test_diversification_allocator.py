from __future__ import annotations

import importlib
import importlib.util
import math
import sys
import types
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

importlib.import_module("services")
common_pkg = importlib.import_module("services.common")


class _StubTimescaleAdapter:
    _configs: Dict[str, Dict[str, Any]] = {}
    _exposures: Dict[str, Dict[str, float]] = {}

    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self._configs.setdefault(account_id, {})
        self._exposures.setdefault(account_id, {})

    @classmethod
    def reset(cls, account_id: str) -> None:
        cls._configs.pop(account_id, None)
        cls._exposures.pop(account_id, None)

    def load_risk_config(self) -> Dict[str, Any]:
        return dict(self._configs.get(self.account_id, {}))

    def save_risk_config(self, config: Dict[str, Any]) -> None:
        self._configs[self.account_id] = dict(config)

    def open_positions(self) -> Dict[str, float]:
        return dict(self._exposures.get(self.account_id, {}))

    def record_instrument_exposure(self, symbol: str, notional: float) -> None:
        self._exposures.setdefault(self.account_id, {})[symbol] = float(notional)


class _StubRedisFeastAdapter:
    _online_feature_store: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def __init__(self, account_id: str, repository: Any | None = None) -> None:
        self.account_id = account_id
        self._repository = repository

    def approved_instruments(self) -> list[str]:
        if self._repository is None:
            return []
        return list(self._repository.approved_universe())

    def fetch_online_features(self, symbol: str) -> Dict[str, Any]:
        store = self._online_feature_store.get(self.account_id, {})
        payload = store.get(symbol, {})
        return dict(payload)

    def fee_override(self, symbol: str) -> Dict[str, Any] | None:
        if self._repository is None:
            return None
        override = self._repository.fee_override(symbol)
        return dict(override) if override else None


stub_module = types.ModuleType("services.common.adapters")
stub_module.RedisFeastAdapter = _StubRedisFeastAdapter
stub_module.TimescaleAdapter = _StubTimescaleAdapter
sys.modules["services.common.adapters"] = stub_module
common_pkg.adapters = stub_module  # type: ignore[attr-defined]

security_stub = types.ModuleType("services.common.security")


def _require_admin_account(func):  # pragma: no cover - simple passthrough stub
    return func


security_stub.require_admin_account = _require_admin_account
sys.modules["services.common.security"] = security_stub
common_pkg.security = security_stub  # type: ignore[attr-defined]

RedisFeastAdapter = _StubRedisFeastAdapter
TimescaleAdapter = _StubTimescaleAdapter

import pytest
from sqlalchemy import create_engine as _sa_create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

pytest.importorskip("sqlalchemy")

_REAL_CREATE_ENGINE = _sa_create_engine
_MODULE_PATH = "services.risk.diversification_allocator"
_POSTGRES_URL = "postgresql+psycopg2://risk:test@localhost/diversification"


class _EngineProxy:
    """Proxy engine that mimics a PostgreSQL connection while using SQLite."""

    def __init__(self, inner: Engine) -> None:
        self._inner = inner
        self.url = make_url(_POSTGRES_URL)

    def __getattr__(self, item: str) -> Any:  # pragma: no cover - passthrough
        return getattr(self._inner, item)

    def dispose(self) -> None:  # pragma: no cover - passthrough
        self._inner.dispose()


def _reload_diversification_module(
    monkeypatch: pytest.MonkeyPatch, sqlite_url: str
):
    monkeypatch.setenv(
        "DIVERSIFICATION_DATABASE_URL",
        "postgresql://risk:test@localhost/diversification",
    )

    def _patched_create_engine(url: Any, *args: Any, **kwargs: Any) -> _EngineProxy:
        if hasattr(url, "render_as_string"):
            raw = url.render_as_string(hide_password=False)
        else:
            raw = str(url)
        assert raw.startswith("postgresql")
        inner = _REAL_CREATE_ENGINE(
            sqlite_url,
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        return _EngineProxy(inner)

    monkeypatch.setattr("sqlalchemy.create_engine", _patched_create_engine, raising=False)

    sys.modules.pop(_MODULE_PATH, None)

    spec = importlib.util.spec_from_file_location(
        _MODULE_PATH, ROOT / "services" / "risk" / "diversification_allocator.py"
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise RuntimeError("Failed to load diversification allocator module")

    module = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE_PATH] = module
    spec.loader.exec_module(module)
    module.init_diversification_storage()
    return module


@pytest.fixture()
def diversification_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    sqlite_path = tmp_path / "diversification-tests.db"
    module = _reload_diversification_module(monkeypatch, f"sqlite:///{sqlite_path}")
    try:
        yield module
    finally:
        module.ENGINE.dispose()


ACCOUNT_ID = "company"


class StubUniverseRepository:
    def __init__(self) -> None:
        self._approved: list[str] = []
        self._fees: dict[str, dict[str, float | str]] = {}

    def configure(
        self,
        instruments: list[str],
        fees: dict[str, dict[str, float | str]],
    ) -> None:
        self._approved = list(instruments)
        self._fees = {symbol: dict(payload) for symbol, payload in fees.items()}

    def approved_universe(self) -> list[str]:
        return list(self._approved)

    def fee_override(self, instrument: str) -> dict[str, float | str] | None:
        override = self._fees.get(instrument) or self._fees.get("default")
        return dict(override) if override else None


STUB_UNIVERSE_REPOSITORY = StubUniverseRepository()


def _setup_account_config() -> None:
    timescale = TimescaleAdapter(account_id=ACCOUNT_ID)
    config = timescale.load_risk_config()
    config.update(
        {
            "nav": 1_000_000.0,
            "diversification": {
                "max_concentration_pct_per_asset": 0.3,
                "max_sector_pct": 0.45,
                "correlation_threshold": 0.8,
                "correlation_penalty": 0.1,
                "rebalance_threshold_pct": 0.025,
                "buckets": [
                    {
                        "name": "btc_core",
                        "target_pct": 0.4,
                        "symbols": ["BTC-USD"],
                        "sector": "layer1",
                    },
                    {
                        "name": "top_cap",
                        "target_pct": 0.35,
                        "symbols": ["ETH-USD"],
                        "sector": "layer1",
                    },
                    {
                        "name": "growth",
                        "target_pct": 0.25,
                        "symbols": ["SOL-USD", "ADA-USD"],
                        "sector": "alts",
                    },
                ],
            },
            "correlation_matrix": {
                "BTC-USD": {"ETH-USD": 0.82, "SOL-USD": 0.7, "ADA-USD": 0.65},
                "ETH-USD": {"BTC-USD": 0.82, "SOL-USD": 0.78, "ADA-USD": 0.75},
                "SOL-USD": {"BTC-USD": 0.7, "ETH-USD": 0.78, "ADA-USD": 0.86},
                "ADA-USD": {"BTC-USD": 0.65, "ETH-USD": 0.75, "SOL-USD": 0.86},
            },
        }
    )
    timescale.save_risk_config(config)


def _seed_universe() -> None:
    STUB_UNIVERSE_REPOSITORY.configure(
        ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD"],
        {
            "BTC-USD": {"currency": "USD", "maker": 0.1, "taker": 0.15},
            "ETH-USD": {"currency": "USD", "maker": 0.35, "taker": 0.6},
            "SOL-USD": {"currency": "USD", "maker": 0.12, "taker": 0.2},
            "ADA-USD": {"currency": "USD", "maker": 0.12, "taker": 0.2},
            "default": {"currency": "USD", "maker": 0.1, "taker": 0.2},
        },
    )


def _seed_feature_store() -> None:
    store = RedisFeastAdapter._online_feature_store.setdefault(ACCOUNT_ID, {})
    store.update(
        {
            "BTC-USD": {"expected_edge_bps": 18.0},
            "ETH-USD": {"expected_edge_bps": 0.4},
            "SOL-USD": {"expected_edge_bps": 8.0},
            "ADA-USD": {"expected_edge_bps": 6.0},
        }
    )


def _build_allocator(module):
    engine = _REAL_CREATE_ENGINE(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    module.init_diversification_storage(engine)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    timescale = TimescaleAdapter(account_id=ACCOUNT_ID)
    universe = RedisFeastAdapter(account_id=ACCOUNT_ID, repository=STUB_UNIVERSE_REPOSITORY)
    return module.DiversificationAllocator(
        ACCOUNT_ID,
        timescale=timescale,
        universe=universe,
        session_factory=session_factory,
        enable_persistence=False,
    )


@pytest.fixture(autouse=True)
def _setup_environment():
    TimescaleAdapter.reset(account_id=ACCOUNT_ID)
    RedisFeastAdapter._online_feature_store.pop(ACCOUNT_ID, None)
    _setup_account_config()
    _seed_universe()
    _seed_feature_store()
    yield


def test_compute_targets_enforces_caps(diversification_module):
    allocator = _build_allocator(diversification_module)
    targets = allocator.compute_targets(persist=False)

    assert pytest.approx(sum(targets.weights.values()), abs=1e-6) == 1.0
    for symbol, weight in targets.weights.items():
        if symbol == "CASH":
            continue
        assert weight <= 0.3 + 1e-6

    sector_weights = {"layer1": 0.0, "alts": 0.0}
    for symbol, bucket in targets.buckets.items():
        if bucket == "cash":
            continue
        if bucket == "growth":
            sector_weights["alts"] += targets.weights[symbol]
        else:
            sector_weights["layer1"] += targets.weights[symbol]

    assert sector_weights["layer1"] <= 0.45 + 1e-6
    assert sector_weights["alts"] <= 0.45 + 1e-6


def test_adjust_intent_scales_down_when_concentration_exceeded(diversification_module):
    allocator = _build_allocator(diversification_module)
    # Existing BTC exposure representing 20% of NAV.
    TimescaleAdapter(account_id=ACCOUNT_ID).record_instrument_exposure(
        "BTC-USD", 200_000.0
    )
    intent = diversification_module.PolicyIntent(
        symbol="BTC-USD", side="BUY", notional=200_000.0, expected_edge_bps=18.0
    )

    adjustment = allocator.adjust_intent_for_diversification(intent)

    assert adjustment.reduced is True
    assert adjustment.approved_symbol == "BTC-USD"
    # Cap should restrict BTC to 30% of NAV => additional 100k notional.
    assert math.isclose(adjustment.notional, 100_000.0, rel_tol=1e-3)


def test_rebalance_plan_respects_fees_and_threshold(diversification_module):
    allocator = _build_allocator(diversification_module)
    # Current exposures in USD notionals.
    timescale = TimescaleAdapter(account_id=ACCOUNT_ID)
    timescale.record_instrument_exposure("BTC-USD", 350_000.0)
    timescale.record_instrument_exposure("ETH-USD", 320_000.0)
    timescale.record_instrument_exposure("SOL-USD", 280_000.0)

    plan = allocator.generate_rebalance_plan()

    symbols = {instruction.symbol: instruction for instruction in plan.instructions}

    # BTC should be reduced due to overweight.
    btc_instruction = symbols.get("BTC-USD")
    assert btc_instruction is not None
    assert btc_instruction.side == "SELL"
    assert btc_instruction.expected_edge_bps and btc_instruction.expected_edge_bps > btc_instruction.fee_bps

    # ETH should be skipped because fee exceeds expected edge.
    assert "ETH-USD" not in symbols

    # SOL diff is below threshold and should not trigger a trade.
    assert "SOL-USD" not in symbols

