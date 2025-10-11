from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


def _reload_module(monkeypatch: pytest.MonkeyPatch, state_dir: Path):
    monkeypatch.setenv("RISK_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("AETHER_STATE_DIR", str(state_dir))

    for variable in (
        "DIVERSIFICATION_DATABASE_URL",
        "RISK_DATABASE_URL",
        "TIMESCALE_DSN",
        "DATABASE_URL",
    ):
        monkeypatch.delenv(variable, raising=False)

    for name in list(sys.modules):
        if name.startswith("sqlalchemy"):
            sys.modules.pop(name)
        if name.startswith("services.risk.diversification_allocator"):
            sys.modules.pop(name)

    module = importlib.import_module("services.risk.diversification_allocator")
    return importlib.reload(module)


def test_diversification_allocator_uses_local_fallback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = _reload_module(monkeypatch, tmp_path)

    assert module.ENGINE is None
    assert module.SessionLocal().__class__.__name__ == "_NullSession"

    class _TimescaleStub:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def load_risk_config(self) -> dict[str, object]:
            return {
                "nav": 100_000.0,
                "diversification": {
                    "max_concentration_pct_per_asset": 0.6,
                    "max_sector_pct": 0.8,
                    "correlation_threshold": 0.5,
                    "correlation_penalty": 0.05,
                    "rebalance_threshold_pct": 0.01,
                    "buckets": [
                        {"name": "core", "target_pct": 0.6, "symbols": ["BTC-USD"], "sector": "layer1"},
                        {"name": "alts", "target_pct": 0.4, "symbols": ["ETH-USD"], "sector": "layer1"},
                    ],
                },
                "correlation_matrix": {
                    "BTC-USD": {"BTC-USD": 1.0, "ETH-USD": 0.25},
                    "ETH-USD": {"BTC-USD": 0.25, "ETH-USD": 1.0},
                },
            }

        def open_positions(self) -> dict[str, float]:
            return {"BTC-USD": 50_000.0, "ETH-USD": 50_000.0}

        def record_event(self, *args: object, **kwargs: object) -> None:
            return None

    class _UniverseStub:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def approved_instruments(self):
            return ("BTC-USD", "ETH-USD")

        def fetch_online_features(self, symbol: str) -> dict[str, object]:
            return {"expected_edge_bps": 10.0, "state": {"expected_edge_bps": 10.0}}

        def fee_override(self, symbol: str) -> dict[str, float]:
            return {"taker": 0.2}

    allocator = module.DiversificationAllocator(
        account_id="company",
        timescale=_TimescaleStub("company"),
        universe=_UniverseStub("company"),
        enable_persistence=True,
    )

    result = allocator.compute_targets(persist=False)

    assert result.account_id == "company"
    assert {"BTC-USD", "ETH-USD"}.issubset(set(result.weights.keys()))
    assert allocator._enable_persistence is False

    rendered = module.DATABASE_URL.render_as_string()
    assert rendered.startswith("sqlite")
    assert (tmp_path / "diversification_allocator").exists()
