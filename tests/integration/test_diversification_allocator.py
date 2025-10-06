"""Integration tests for the portfolio diversification allocator safeguards."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Mapping

import json

import pytest

from services.risk.portfolio_risk import PortfolioRiskAggregator


class _StubTimescaleAdapter:
    """Deterministic stand-in for the Timescale adapter used by the aggregator."""

    def __init__(
        self,
        *,
        exposures: Mapping[str, float],
        correlation_matrix: Mapping[str, Mapping[str, float]] | None = None,
        var95: float = 100_000.0,
        cvar95: float | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        self._exposures = {str(symbol): float(value) for symbol, value in exposures.items()}
        self._correlation_matrix = {
            str(symbol): {str(other): float(val) for other, val in row.items()}
            for symbol, row in (correlation_matrix or {}).items()
        }
        self._var95 = float(var95)
        self._cvar95 = float(cvar95 if cvar95 is not None else var95)
        self._timestamp = timestamp or datetime.now(timezone.utc)

    # The production adapter exposes these three methods which the aggregator relies on.
    def open_positions(self) -> Dict[str, float]:
        return dict(self._exposures)

    def load_risk_config(self) -> Dict[str, object]:
        return {"var_limit": self._var95, "correlation_matrix": self._correlation_matrix}

    def cvar_results(self):
        return [
            {
                "var95": self._var95,
                "cvar95": self._cvar95,
                "ts": self._timestamp,
            }
        ]


@pytest.fixture()
def adapter_factory():
    """Factory to provide per-account stub adapters to the aggregator."""

    registry: Dict[str, _StubTimescaleAdapter] = {}

    def register(account_id: str, adapter: _StubTimescaleAdapter) -> None:
        registry[account_id] = adapter

    def factory(account_id: str) -> _StubTimescaleAdapter:
        try:
            return registry[account_id]
        except KeyError as exc:  # pragma: no cover - defensive for unexpected accounts
            raise AssertionError(f"Unexpected account requested: {account_id}") from exc

    return register, factory


def test_portfolio_status_filters_non_spot(adapter_factory) -> None:
    register, factory = adapter_factory

    register(
        "delta",
        _StubTimescaleAdapter(
            exposures={
                "BTC-PERP": 250_000.0,
                "ETH-USD": 125_000.0,
                "adaup-usd": 10_000.0,
                "WBTC-USD": -50_000.0,
            },
            correlation_matrix={},
            var95=90_000.0,
            cvar95=120_000.0,
        ),
    )

    allocator = PortfolioRiskAggregator(
        accounts=("delta",),
        cluster_limits={"ETH": 500_000.0, "BTC": 500_000.0},
        instrument_clusters={"ETH-USD": "ETH", "WBTC-USD": "BTC"},
        instrument_betas={"ETH-USD": 0.8, "WBTC-USD": 1.0},
        beta_limit=2.0,
        correlation_limit=0.99,
        adapter_factory=factory,
    )

    response = allocator.portfolio_status()

    assert response.accounts[0].instrument_exposure == {
        "ETH-USD": pytest.approx(125_000.0),
        "WBTC-USD": pytest.approx(50_000.0),
    }
    assert set(response.totals.instrument_exposure) == {"ETH-USD", "WBTC-USD"}
    assert response.totals.gross_exposure == pytest.approx(175_000.0)
    # Cluster exposure only tracks instruments that survived the spot filter.
    assert response.totals.cluster_exposure == {"ETH": 125_000.0, "BTC": 50_000.0}


def test_overweight_btc_triggers_cluster_downscale(adapter_factory) -> None:
    register, factory = adapter_factory

    register(
        "alpha",
        _StubTimescaleAdapter(
            exposures={"BTC-USD": 400_000.0, "ETH-USD": 100_000.0},
            correlation_matrix={},
            var95=120_000.0,
            cvar95=150_000.0,
        ),
    )

    allocator = PortfolioRiskAggregator(
        accounts=("alpha",),
        cluster_limits={"BTC": 300_000.0, "ETH": 1_000_000.0},
        instrument_clusters={"BTC-USD": "BTC", "ETH-USD": "ETH"},
        instrument_betas={"BTC-USD": 1.0, "ETH-USD": 0.8},
        beta_limit=2.0,
        correlation_limit=0.99,
        adapter_factory=factory,
    )

    response = allocator.portfolio_status()

    assert response.constraints_ok is False
    assert response.totals.cluster_exposure["BTC"] == pytest.approx(400_000.0)

    breach_details = {breach.constraint: breach.detail for breach in response.breaches}
    assert "max_cluster_exposure" in breach_details
    assert breach_details["max_cluster_exposure"]["cluster"] == "BTC"

    # Downscale factor should reflect the BTC limit versus projected exposure.
    assert response.risk_adjustment == pytest.approx(300_000.0 / 400_000.0, rel=1e-6)


def test_correlation_cap_enforces_highly_correlated_exposures(adapter_factory) -> None:
    register, factory = adapter_factory

    correlation_matrix = {
        "BTC-USD": {"WBTC-USD": 0.95},
        "WBTC-USD": {"BTC-USD": 0.95},
    }

    register(
        "gamma",
        _StubTimescaleAdapter(
            exposures={"BTC-USD": 200_000.0, "WBTC-USD": 180_000.0},
            correlation_matrix=correlation_matrix,
            var95=110_000.0,
            cvar95=140_000.0,
        ),
    )

    allocator = PortfolioRiskAggregator(
        accounts=("gamma",),
        cluster_limits={"BTC": 1_000_000.0},
        instrument_clusters={"BTC-USD": "BTC", "WBTC-USD": "BTC"},
        instrument_betas={"BTC-USD": 1.0, "WBTC-USD": 1.0},
        beta_limit=2.0,
        correlation_limit=0.8,
        adapter_factory=factory,
    )

    response = allocator.portfolio_status()

    assert response.constraints_ok is False
    assert response.totals.max_correlation == pytest.approx(0.95, rel=1e-6)

    breach_map = {breach.constraint: breach for breach in response.breaches}
    assert "max_correlation_risk" in breach_map
    correlation_breach = breach_map["max_correlation_risk"]
    assert correlation_breach.limit == pytest.approx(0.8)
    assert correlation_breach.value == pytest.approx(0.95)

    expected_adjustment = 0.8 / 0.95
    assert response.risk_adjustment == pytest.approx(expected_adjustment, rel=1e-6)


def test_cluster_and_beta_maps_ignore_non_spot(monkeypatch) -> None:
    monkeypatch.setenv(
        "PORTFOLIO_CLUSTER_MAP",
        json.dumps({"BTC-PERP": "PERP", "eth-usd": "ETH", "ADAUP-USD": "ALT"}),
    )
    monkeypatch.setenv(
        "PORTFOLIO_BETA_MAP",
        json.dumps({"BTC-PERP": 2.0, "ETH-USD": 0.9, "WBTC-USD": 1.1}),
    )

    # Reload module-level caches by re-importing the helpers.
    from importlib import reload

    import services.risk.portfolio_risk as portfolio_risk

    reload(portfolio_risk)

    cluster_map = portfolio_risk._load_cluster_map()
    beta_map = portfolio_risk._load_beta_map()

    assert cluster_map == {"ETH-USD": "ETH"}
    assert beta_map == {"ETH-USD": 0.9, "WBTC-USD": 1.1}

    # Clean up environment variables for future tests.
    monkeypatch.delenv("PORTFOLIO_CLUSTER_MAP", raising=False)
    monkeypatch.delenv("PORTFOLIO_BETA_MAP", raising=False)
