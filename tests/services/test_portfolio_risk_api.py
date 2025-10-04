"""Security tests for the portfolio risk aggregation endpoint."""

from __future__ import annotations

import sys
from importlib import util as importlib_util
from pathlib import Path
from types import ModuleType

import pytest

pytest.importorskip("fastapi")

from fastapi import status
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

tests_path = str(ROOT / "tests")
if tests_path in sys.path:
    sys.path.remove(tests_path)
    sys.path.append(tests_path)


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib_util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module {name} from {path}")
    module = importlib_util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_load_module("services", ROOT / "services" / "__init__.py")
_load_module("services.common", ROOT / "services" / "common" / "__init__.py")
_load_module("services.common.security", ROOT / "services" / "common" / "security.py")
module = _load_module("services.risk.portfolio_risk", ROOT / "services" / "risk" / "portfolio_risk.py")


@pytest.fixture(autouse=True)
def _reset_overrides() -> None:
    module.app.dependency_overrides.clear()
    yield
    module.app.dependency_overrides.clear()


def test_portfolio_status_requires_authentication(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        module.aggregator,
        "portfolio_status",
        lambda: (_ for _ in ()).throw(AssertionError("aggregator should not run without auth")),
    )

    with TestClient(module.app) as client:
        response = client.get("/risk/portfolio/status")

    assert response.status_code in {status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN}


def test_portfolio_status_rejects_unauthorised_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "AUTHORIZED_VIEWERS", {"risk-admin"})
    module.app.dependency_overrides[module.require_admin_account] = lambda: "ops-analyst"
    monkeypatch.setattr(
        module.aggregator,
        "portfolio_status",
        lambda: (_ for _ in ()).throw(AssertionError("unauthorised requests must be rejected")),
    )

    with TestClient(module.app) as client:
        response = client.get("/risk/portfolio/status")

    assert response.status_code == status.HTTP_403_FORBIDDEN


def test_portfolio_status_allows_authorized_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "AUTHORIZED_VIEWERS", {"ops-admin"})
    module.app.dependency_overrides[module.require_admin_account] = lambda: "ops-admin"

    expected = module.PortfolioStatusResponse(
        totals=module.PortfolioTotals(
            gross_exposure=125_000.0,
            instrument_exposure={"BTC-USD": 70_000.0},
            cluster_exposure={"BTC": 70_000.0},
            var_95=22_500.0,
            cvar_95=30_000.0,
            beta_to_btc=0.85,
            max_correlation=0.42,
        ),
        accounts=[],
        constraints_ok=True,
        breaches=[],
        risk_adjustment=1.0,
    )
    monkeypatch.setattr(module.aggregator, "portfolio_status", lambda: expected)

    with TestClient(module.app) as client:
        response = client.get("/risk/portfolio/status")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == expected.model_dump()
