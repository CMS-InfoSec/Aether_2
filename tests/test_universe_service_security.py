import importlib
import os
import sys
from pathlib import Path
from typing import Any, Iterator, Tuple

import pytest
from fastapi import Header, Request
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

services_init = ROOT / "services" / "__init__.py"
if services_init.exists():
    sys.modules.pop("services", None)
    sys.modules.pop("services.common", None)
    spec = importlib.util.spec_from_file_location(
        "services", services_init, submodule_search_locations=[str(services_init.parent)]
    )
    if spec is not None and spec.loader is not None:
        module = importlib.util.module_from_spec(spec)
        module.__path__ = [str(services_init.parent)]  # type: ignore[attr-defined]
        sys.modules["services"] = module
        spec.loader.exec_module(module)

os.environ.setdefault("DATABASE_URL", "sqlite:///./universe_service_auth_test.db")

import universe_service  # noqa: E402  # pylint: disable=wrong-import-position
from services.common import security  # noqa: E402  # pylint: disable=wrong-import-position
from auth.service import InMemorySessionStore  # noqa: E402  # pylint: disable=wrong-import-position


def test_universe_service_requires_configured_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    """Production deployments must not fall back to SQLite automatically."""

    global universe_service  # pylint: disable=global-statement

    module_name = "universe_service"
    monkeypatch.delenv("UNIVERSE_DATABASE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    sys.modules.pop(module_name, None)

    original_pytest = sys.modules.pop("pytest", None)
    try:
        with pytest.raises(RuntimeError, match="Universe database DSN"):
            importlib.import_module(module_name)
    finally:
        if original_pytest is not None:
            sys.modules["pytest"] = original_pytest

    monkeypatch.setenv("DATABASE_URL", "sqlite:///./universe_service_auth_test.db")
    universe_service = importlib.import_module(module_name)


@pytest.fixture
def universe_client(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[Tuple[TestClient, Any]]:
    db_path = tmp_path / "universe.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    module = importlib.reload(universe_service)

    security.set_default_session_store(InMemorySessionStore(ttl_minutes=240))

    monkeypatch.setattr(
        module,
        "fetch_coingecko_market_data",
        lambda: {
            "BTC": {
                "market_cap": 2_000_000_000.0,
                "global_volume": 500_000_000.0,
            }
        },
    )
    monkeypatch.setattr(
        module,
        "fetch_kraken_volume",
        lambda symbols: {symbol: 50_000_000.0 for symbol in symbols},
    )
    monkeypatch.setattr(
        module,
        "fetch_annualised_volatility",
        lambda symbols: {symbol: 0.5 for symbol in symbols},
    )
    monkeypatch.setattr(module, "_refresh_universe_periodically", lambda: None)

    with TestClient(module.app) as client:
        yield client, module

    module.app.dependency_overrides.clear()
    security.set_default_session_store(None)


def test_get_universe_requires_auth(universe_client: Tuple[TestClient, Any]) -> None:
    client, _ = universe_client

    response = client.get("/universe/approved")

    assert response.status_code == 401


def test_override_requires_auth(universe_client: Tuple[TestClient, Any]) -> None:
    client, _ = universe_client

    response = client.post(
        "/universe/override",
        json={"symbol": "BTC", "enabled": True, "reason": "force include"},
    )

    assert response.status_code == 401


def test_override_rejects_mismatched_scope(universe_client: Tuple[TestClient, Any]) -> None:
    client, module = universe_client

    def _stub_admin(
        request: Request,
        authorization: str | None = Header(None, alias="Authorization"),
        x_account_id: str | None = Header(None, alias="X-Account-ID"),
    ) -> str:
        request.state.account_scopes = ("director-1",)
        return "company"

    module.app.dependency_overrides[module.require_admin_account] = _stub_admin
    try:
        response = client.post(
            "/universe/override",
            json={"symbol": "ETH", "enabled": False, "reason": "risk"},
            headers=_admin_headers("company"),
        )
    finally:
        module.app.dependency_overrides.pop(module.require_admin_account, None)

    assert response.status_code == 403


def test_override_allows_authenticated_admin(universe_client: Tuple[TestClient, Any]) -> None:
    client, _ = universe_client

    response = client.post(
        "/universe/override",
        json={"symbol": "DOGE", "enabled": False, "reason": "illiquid"},
        headers=_admin_headers("company"),
    )

    assert response.status_code == 204
def _admin_headers(account_id: str = "company") -> dict[str, str]:
    store = security._DEFAULT_SESSION_STORE
    if store is None:  # pragma: no cover - safety net when autouse fixture missing
        raise RuntimeError("Default session store is not configured for tests.")
    session = store.create(account_id)
    return {
        "X-Account-ID": account_id,
        "Authorization": f"Bearer {session.token}",
    }

