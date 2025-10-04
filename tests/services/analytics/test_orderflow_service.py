from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Iterator, Tuple

import pytest
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
    ("auth", ROOT / "auth" / "__init__.py"),
    ("auth.service", ROOT / "auth" / "service.py"),
    ("services", ROOT / "services" / "__init__.py"),
    ("services.common", ROOT / "services" / "common" / "__init__.py"),
    ("services.common.security", ROOT / "services" / "common" / "security.py"),
    ("services.common.config", ROOT / "services" / "common" / "config.py"),
    ("services.analytics", ROOT / "services" / "analytics" / "__init__.py"),
):
    if module_name not in sys.modules:
        _load_module(module_name, module_path)


@pytest.fixture
def orderflow_module(monkeypatch: pytest.MonkeyPatch):
    module_name = "services.analytics.orderflow_service"
    monkeypatch.setenv("SESSION_REDIS_URL", "memory://")
    monkeypatch.setenv("SESSION_TTL_MINUTES", "5")
    sys.modules.pop(module_name, None)
    module = _load_module(module_name, ROOT / "services" / "analytics" / "orderflow_service.py")
    yield module
    module.app.dependency_overrides.clear()
    module.security.set_default_session_store(None)
    sys.modules.pop(module_name, None)


@pytest.fixture
def orderflow_client(orderflow_module) -> Iterator[Tuple[TestClient, object]]:
    with TestClient(orderflow_module.app) as client:
        yield client, orderflow_module


def test_orderflow_endpoints_require_auth(orderflow_client) -> None:
    client, _ = orderflow_client

    response = client.get("/orderflow/imbalance", params={"symbol": "BTC-USD"})

    assert response.status_code == 401
    assert response.json()["detail"] == "Missing Authorization header."


def test_orderflow_rejects_non_admin_sessions(orderflow_client) -> None:
    client, module = orderflow_client
    session = module.SESSION_STORE.create("viewer")

    response = client.get(
        "/orderflow/queue",
        params={"symbol": "BTC-USD"},
        headers={"Authorization": f"Bearer {session.token}"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Account is not authorized for administrative access."


def test_orderflow_metrics_available_to_admins(orderflow_client) -> None:
    client, module = orderflow_client
    session = module.SESSION_STORE.create("company")

    response = client.get(
        "/orderflow/liquidity_holes",
        params={"symbol": "ETH-USD", "window": 600},
        headers={"Authorization": f"Bearer {session.token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "ETH-USD"
    assert "liquidity_holes" in payload
    assert "impact_estimates" in payload

