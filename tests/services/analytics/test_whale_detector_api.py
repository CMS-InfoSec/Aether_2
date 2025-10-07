from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest
from fastapi import Header, HTTPException, Request, status
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise ImportError(f"Unable to load module spec for {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


for module_name, module_path in (
    ("services", ROOT / "services" / "__init__.py"),
    ("services.common", ROOT / "services" / "common" / "__init__.py"),
    ("services.common.security", ROOT / "services" / "common" / "security.py"),
    ("services.analytics", ROOT / "services" / "analytics" / "__init__.py"),
    ("services.analytics.whale_detector", ROOT / "services" / "analytics" / "whale_detector.py"),
):
    if module_name not in sys.modules:
        _load_module(module_name, module_path)


import services.analytics.whale_detector as module
from tests.helpers.authentication import override_admin_auth


@pytest.fixture()
def client() -> TestClient:
    with TestClient(module.app) as test_client:
        yield test_client


@pytest.fixture(autouse=True)
def reset_detector_state() -> None:
    module.detector._events.clear()
    module.detector._metrics.clear()
    module.detector._aggressive_windows.clear()
    yield
    module.detector._events.clear()
    module.detector._metrics.clear()
    module.detector._aggressive_windows.clear()


def test_recent_whales_requires_authentication(client: TestClient) -> None:
    with override_admin_auth(client.app, module.require_admin_account, "ops-admin"):
        response = client.get("/whales/recent")

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_recent_whales_rejects_unauthorized_account(client: TestClient) -> None:
    def _deny(
        _request: Request,
        authorization: str | None = Header(None, alias="Authorization"),
        x_account_id: str | None = Header(None, alias="X-Account-ID"),
    ) -> str:
        _ = authorization, x_account_id
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is not authorized for administrative access.",
        )

    client.app.dependency_overrides[module.require_admin_account] = _deny
    try:
        response = client.get("/whales/recent", headers={"Authorization": "Bearer stub"})
    finally:
        client.app.dependency_overrides.pop(module.require_admin_account, None)

    assert response.status_code == status.HTTP_403_FORBIDDEN


def test_recent_whales_returns_results_for_authorized_admin(client: TestClient) -> None:
    with override_admin_auth(client.app, module.require_admin_account, "ops-admin") as headers:
        response = client.get("/whales/recent", headers=headers)

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == []


def test_recent_whales_rejects_non_spot_symbol(client: TestClient) -> None:
    with override_admin_auth(client.app, module.require_admin_account, "ops-admin") as headers:
        response = client.get(
            "/whales/recent",
            headers=headers,
            params={"symbol": "BTC-PERP"},
        )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert (
        response.json()["detail"]
        == "symbol 'BTC-PERP' is not a supported USD spot market instrument"
    )


def test_trade_observation_normalises_spot_symbol() -> None:
    observation = module.TradeObservation(
        symbol="eth/usd",
        size=1.0,
        notional=1000.0,
        side="buy",
        impact=0.5,
    )

    assert observation.symbol == "ETH-USD"


def test_trade_observation_rejects_derivative_symbol() -> None:
    with pytest.raises(ValueError):
        module.TradeObservation(
            symbol="BTC-PERP",
            size=1.0,
            notional=1000.0,
            side="buy",
            impact=0.5,
        )
