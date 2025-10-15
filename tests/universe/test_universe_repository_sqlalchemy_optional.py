"""Universe repository should operate with the SQLAlchemy shim installed."""

from __future__ import annotations

import builtins
import importlib
import sys
from datetime import datetime, timezone
from typing import Callable


def _block_sqlalchemy(monkeypatch) -> None:
    original_import: Callable[..., object] = builtins.__import__

    def _guard(  # type: ignore[override]
        name: str, globals=None, locals=None, fromlist=(), level: int = 0
    ) -> object:
        if name.startswith("sqlalchemy") and name not in sys.modules:
            raise ModuleNotFoundError(name)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guard)


def test_universe_repository_uses_fallback_without_sqlalchemy(monkeypatch) -> None:
    for module_name in list(sys.modules):
        if module_name.startswith("sqlalchemy"):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    monkeypatch.delitem(sys.modules, "services.universe.repository", raising=False)

    _block_sqlalchemy(monkeypatch)

    # Ensure the stub is installed before importing the repository module.
    stub = importlib.import_module("services.common.sqlalchemy_stub")
    stub.install()

    repository_module = importlib.import_module("services.universe.repository")
    MarketSnapshot = repository_module.MarketSnapshot
    UniverseRepository = repository_module.UniverseRepository

    UniverseRepository.reset()

    UniverseRepository.seed_market_snapshots(
        "acct",
        [
            MarketSnapshot(
                base_asset="BTC",
                quote_asset="USD",
                market_cap=UniverseRepository.MARKET_CAP_THRESHOLD * 2,
                global_volume_24h=UniverseRepository.GLOBAL_VOLUME_THRESHOLD * 2,
                kraken_volume_24h=UniverseRepository.KRAKEN_VOLUME_THRESHOLD * 2,
                volatility_30d=UniverseRepository.VOLATILITY_THRESHOLD * 2,
                source="coingecko",
                observed_at=datetime.now(timezone.utc),
            )
        ],
    )

    UniverseRepository.seed_fee_overrides(
        "acct",
        {"BTC-USD": {"currency": "USD", "maker": 0.1, "taker": 0.2}},
    )

    repo = UniverseRepository(account_id="acct")

    assert repo.approved_universe() == ["BTC-USD"]

    repo.set_manual_override("BTC-USD", approved=False, actor_id="ops")
    assert repo.approved_universe() == []

    repo.set_manual_override("BTC-USD", approved=True, actor_id="ops")
    assert repo.approved_universe() == ["BTC-USD"]

    assert repo.fee_override("BTC-USD") == {"currency": "USD", "maker": 0.1, "taker": 0.2}

    UniverseRepository.reset()
