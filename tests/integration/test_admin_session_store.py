from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Dict, Optional

import pyotp
import pytest
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[2]
SERVICE_PATH = ROOT / "auth" / "service.py"
ROUTES_PATH = ROOT / "auth" / "routes.py"
APP_PATH = ROOT / "app.py"


def _ensure_shared_correlation_stub() -> None:
    existing = sys.modules.get("shared.correlation")
    if existing is not None and hasattr(existing, "get_correlation_id"):
        return
    correlation_stub = ModuleType("shared.correlation")

    def _get_correlation_id() -> str:  # pragma: no cover - deterministic identifier
        return "test-correlation"

    correlation_stub.get_correlation_id = _get_correlation_id  # type: ignore[attr-defined]

    class _CorrelationIdMiddleware:  # pragma: no cover - minimal ASGI middleware stub
        def __init__(self, app):
            self.app = app

        async def __call__(self, scope, receive, send):
            await self.app(scope, receive, send)

    correlation_stub.CorrelationIdMiddleware = _CorrelationIdMiddleware  # type: ignore[attr-defined]
    shared_pkg = sys.modules.setdefault("shared", ModuleType("shared"))
    shared_pkg.__path__ = []  # type: ignore[attr-defined]
    setattr(shared_pkg, "correlation", correlation_stub)
    sys.modules["shared.correlation"] = correlation_stub


def _load_auth_service_module():
    _ensure_shared_correlation_stub()
    try:  # pragma: no cover - defensive guard when prometheus_client unavailable
        from prometheus_client import REGISTRY
    except Exception:  # pragma: no cover
        REGISTRY = None  # type: ignore[assignment]
    else:
        REGISTRY._names_to_collectors.clear()  # type: ignore[attr-defined]
        REGISTRY._collector_to_names.clear()  # type: ignore[attr-defined]
    spec = importlib.util.spec_from_file_location(
        "auth.service",
        SERVICE_PATH,
        submodule_search_locations=[str((ROOT / "auth").resolve())],
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise ImportError("Unable to load auth.service module for integration test")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_auth_routes_module() -> ModuleType:
    try:  # pragma: no cover - defensive guard when prometheus_client unavailable
        from prometheus_client import REGISTRY
    except Exception:  # pragma: no cover
        REGISTRY = None  # type: ignore[assignment]
    else:
        REGISTRY._names_to_collectors.clear()  # type: ignore[attr-defined]
        REGISTRY._collector_to_names.clear()  # type: ignore[attr-defined]
    spec = importlib.util.spec_from_file_location(
        "auth.routes",
        ROUTES_PATH,
        submodule_search_locations=[str((ROOT / "auth").resolve())],
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise ImportError("Unable to load auth.routes module for integration test")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_app_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("integration_app", APP_PATH)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise ImportError("Unable to load app module for integration test")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _MemoryCursor:
    def __init__(self, store: Dict[str, Dict[str, str]]) -> None:
        self._store = store
        self._result: Optional[tuple[str, str, str, str, Optional[str]]] = None

    def __enter__(self) -> "_MemoryCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str, params: Optional[tuple] = None) -> None:
        normalized = " ".join(query.strip().lower().split())
        if normalized.startswith("create table"):
            return
        if normalized.startswith("insert into"):
            assert params is not None
            email, admin_id, password_hash, mfa_secret, allowed_ips = params
            self._store[email] = {
                "email": email,
                "admin_id": admin_id,
                "password_hash": password_hash,
                "mfa_secret": mfa_secret,
                "allowed_ips": allowed_ips,
            }
            return
        if normalized.startswith("select"):
            assert params is not None
            email = params[0]
            record = self._store.get(email)
            if record is None:
                self._result = None
            else:
                self._result = (
                    record["email"],
                    record["admin_id"],
                    record["password_hash"],
                    record["mfa_secret"],
                    record["allowed_ips"],
                )
            return
        raise NotImplementedError(query)

    def fetchone(self) -> Optional[tuple[str, str, str, str, Optional[str]]]:
        return self._result


class _MemoryConnection:
    def __init__(self, store: Dict[str, Dict[str, str]]) -> None:
        self._store = store

    def __enter__(self) -> "_MemoryConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _MemoryCursor:
        return _MemoryCursor(self._store)

    def commit(self) -> None:
        return None


class _MemoryPsycopg:
    def __init__(self) -> None:
        self._store: Dict[str, Dict[str, str]] = {}

    def connect(self, dsn: str) -> _MemoryConnection:  # noqa: D401 - signature mirrors psycopg
        del dsn
        return _MemoryConnection(self._store)


@pytest.mark.integration
def test_session_token_survives_new_process() -> None:
    auth_module = _load_auth_service_module()
    previous_service = sys.modules.get("auth.service")
    previous_routes = sys.modules.get("auth.routes")
    sys.modules["auth.service"] = auth_module
    sys.modules.pop("auth.routes", None)
    routes_module = _load_auth_routes_module()
    sys.modules["auth.routes"] = routes_module
    try:
        app_module = _load_app_module()
    finally:
        if previous_routes is not None:
            sys.modules["auth.routes"] = previous_routes
        else:
            sys.modules.pop("auth.routes", None)
        if previous_service is not None:
            sys.modules["auth.service"] = previous_service
        else:
            sys.modules.pop("auth.service", None)

    psycopg = _MemoryPsycopg()
    admin_repo = auth_module.PostgresAdminRepository("postgresql://stub", psycopg_module=psycopg)
    app = app_module.create_app(admin_repository=admin_repo)
    client = TestClient(app)

    secret = pyotp.random_base32()
    admin = auth_module.AdminAccount(
        admin_id="admin-7",
        email="integration@example.com",
        password_hash=auth_module.hash_password("HorseBatteryStaple"),
        mfa_secret=secret,
    )
    admin_repo.add(admin)

    first_code = pyotp.TOTP(secret).now()
    login = client.post(
        "/auth/login",
        json={
            "email": admin.email,
            "password": "HorseBatteryStaple",
            "mfa_code": first_code,
        },
    )
    assert login.status_code == 200
    token = login.json()["token"]

    restarted_repo = auth_module.PostgresAdminRepository("postgresql://stub", psycopg_module=psycopg)
    restarted_app = app_module.create_app(admin_repository=restarted_repo)
    persisted = restarted_app.state.session_store.get(token)

    assert persisted is not None
    assert persisted.admin_id == admin.admin_id

    restarted_client = TestClient(restarted_app)
    second_code = pyotp.TOTP(secret).now()
    follow_up = restarted_client.post(
        "/auth/login",
        json={
            "email": admin.email,
            "password": "HorseBatteryStaple",
            "mfa_code": second_code,
        },
    )
    assert follow_up.status_code == 200
    assert follow_up.json()["token"]
