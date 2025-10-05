"""Smoke test verifying diversification targets persist across restarts."""

from __future__ import annotations

import importlib
import importlib.util
import sys
import types
from pathlib import Path
from typing import Any, Dict

import pytest
from sqlalchemy import create_engine as _sa_create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import StaticPool

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("sqlalchemy")

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
        return dict(store.get(symbol, {}))

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


def _require_admin_account(func):  # pragma: no cover - passthrough stub
    return func


security_stub.require_admin_account = _require_admin_account
sys.modules["services.common.security"] = security_stub
common_pkg.security = security_stub  # type: ignore[attr-defined]

_REAL_CREATE_ENGINE = _sa_create_engine
_MODULE = "services.risk.diversification_allocator"
_POSTGRES_URL = "postgresql+psycopg2://risk:test@localhost/diversification"


class _EngineProxy:
    """Proxy that makes a SQLite engine quack like PostgreSQL."""

    def __init__(self, inner: Engine) -> None:
        self._inner = inner
        self.url = make_url(_POSTGRES_URL)

    def __getattr__(self, item: str) -> Any:  # pragma: no cover - passthrough
        return getattr(self._inner, item)

    def dispose(self) -> None:  # pragma: no cover - passthrough
        self._inner.dispose()


def _reload_allocator(monkeypatch: pytest.MonkeyPatch, sqlite_url: str):
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

    sys.modules.pop(_MODULE, None)

    spec = importlib.util.spec_from_file_location(
        _MODULE, ROOT / "services" / "risk" / "diversification_allocator.py"
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError("Failed to load diversification allocator module")

    module = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE] = module
    spec.loader.exec_module(module)
    module.init_diversification_storage()
    return module


@pytest.mark.smoke
def test_diversification_targets_survive_restart_and_replicas(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sqlite_path = tmp_path / "diversification.db"
    module = _reload_allocator(monkeypatch, f"sqlite:///{sqlite_path}")

    with module.SessionLocal() as session:
        record = module.DiversificationTargetRecord(
            account_id="ops",
            symbol="BTC-USD",
            bucket="layer1",
            weight=0.42,
            expected_edge_bps=15.0,
        )
        session.add(record)
        session.commit()
        stored_id = record.id

    module.ENGINE.dispose()
    sys.modules.pop(_MODULE, None)

    module_reloaded = _reload_allocator(monkeypatch, f"sqlite:///{sqlite_path}")

    with module_reloaded.SessionLocal() as primary, module_reloaded.SessionLocal() as replica:
        primary_rows = primary.execute(
            select(module_reloaded.DiversificationTargetRecord)
        ).scalars().all()
        replica_rows = replica.execute(
            select(module_reloaded.DiversificationTargetRecord)
        ).scalars().all()

    assert len(primary_rows) == len(replica_rows) == 1
    primary_record = primary_rows[0]
    replica_record = replica_rows[0]

    assert primary_record.id == stored_id
    assert primary_record.account_id == "ops"
    assert primary_record.symbol == "BTC-USD"

    assert replica_record.account_id == primary_record.account_id
    assert replica_record.symbol == primary_record.symbol
    assert primary_record.weight == pytest.approx(0.42, rel=1e-6)
    assert replica_record.weight == pytest.approx(primary_record.weight, rel=1e-6)

    module_reloaded.ENGINE.dispose()
