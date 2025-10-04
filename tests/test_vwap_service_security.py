"""Authorization tests for the VWAP analytics service."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import importlib.util
import sys
from types import ModuleType

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI, HTTPException, status
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:
        raise ImportError(f"Unable to load module {name} from {path}")
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_load_module("services", ROOT / "services" / "__init__.py")
_load_module("services.common", ROOT / "services" / "common" / "__init__.py")
_load_module("services.common.security", ROOT / "services" / "common" / "security.py")
_load_module("services.analytics", ROOT / "services" / "analytics" / "__init__.py")
vwap_service = _load_module(
    "services.analytics.vwap_service", ROOT / "services" / "analytics" / "vwap_service.py"
)

from services.common.security import require_admin_account


class _UnauthorizedService:
    def compute(self, _symbol: str) -> None:
        raise AssertionError("VWAP computation should not run when unauthorized")


class _RecordingService:
    def __init__(self) -> None:
        self.invocations: list[str] = []

    def compute(self, symbol: str) -> vwap_service.VWAPDivergenceResponse:
        self.invocations.append(symbol)
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        return vwap_service.VWAPDivergenceResponse(
            symbol=symbol,
            vwap=10_000.0,
            current_price=10_100.0,
            divergence_pct=1.0,
            std_dev_pct=0.5,
            overextended=False,
            window_start=now,
            window_end=now,
        )


def _build_test_client(service: object) -> TestClient:
    app = FastAPI()
    app.include_router(vwap_service.router)
    app.dependency_overrides[vwap_service.get_service] = lambda: service
    return TestClient(app)


def test_vwap_divergence_requires_authentication() -> None:
    with _build_test_client(_UnauthorizedService()) as client:
        client.app.dependency_overrides[require_admin_account] = _raise_unauthorized

        response = client.get("/vwap/divergence", params={"symbol": "BTCUSD"})

    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def _raise_unauthorized() -> str:
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing token")


def test_vwap_divergence_rejects_account_scope_mismatch() -> None:
    with _build_test_client(_UnauthorizedService()) as client:
        client.app.dependency_overrides[require_admin_account] = lambda: "ops-admin"

        response = client.get(
            "/vwap/divergence",
            params={"symbol": "BTCUSD", "account_id": "portfolio-alpha"},
        )

    assert response.status_code == status.HTTP_403_FORBIDDEN


def test_vwap_divergence_allows_authorized_callers() -> None:
    service = _RecordingService()
    with _build_test_client(service) as client:
        client.app.dependency_overrides[require_admin_account] = lambda: "portfolio-alpha"

        response = client.get(
            "/vwap/divergence",
            params={"symbol": "ETHUSD", "account_id": "Portfolio-Alpha"},
        )

    assert response.status_code == status.HTTP_200_OK
    payload = response.json()
    assert payload["symbol"] == "ETHUSD"
    assert service.invocations == ["ETHUSD"]

