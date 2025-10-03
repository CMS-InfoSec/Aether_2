import importlib.util
import os
import sys
from pathlib import Path
from typing import Iterator, Tuple

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("CONFIG_SANDBOX_DATABASE_URL", "sqlite+pysqlite:///:memory:")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def _import_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise ModuleNotFoundError(module_name)
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(module_name, module)
    spec.loader.exec_module(module)
    return module


import config_sandbox

try:
    from auth.service import InMemorySessionStore
except ModuleNotFoundError:  # pragma: no cover - fallback for namespace packages
    auth_module = _import_module("tests.config_sandbox_auth", ROOT / "auth" / "service.py")
    InMemorySessionStore = getattr(auth_module, "InMemorySessionStore")  # type: ignore[assignment]

try:
    from services.common.security import set_default_session_store
except ModuleNotFoundError:  # pragma: no cover - fallback for namespace packages
    security_module = _import_module(
        "tests.config_sandbox_security", ROOT / "services" / "common" / "security.py"
    )
    set_default_session_store = getattr(
        security_module, "set_default_session_store"
    )  # type: ignore[assignment]


@pytest.fixture()
def sandbox_client() -> Iterator[Tuple[TestClient, InMemorySessionStore]]:
    store = InMemorySessionStore()
    set_default_session_store(store)
    config_sandbox.app.state.session_store = store

    try:
        with TestClient(config_sandbox.app) as client:
            yield client, store
    finally:
        set_default_session_store(None)
        if hasattr(config_sandbox.app.state, "session_store"):
            delattr(config_sandbox.app.state, "session_store")


def _auth_headers(store: InMemorySessionStore, account: str) -> dict[str, str]:
    session = store.create(account)
    return {"Authorization": f"Bearer {session.token}"}


def test_run_sandbox_requires_authentication(
    sandbox_client: Tuple[TestClient, InMemorySessionStore]
) -> None:
    client, _store = sandbox_client

    response = client.post(
        "/sandbox/test_config",
        json={"config_changes": {"feature_flag": True}},
    )

    assert response.status_code == 401


def test_single_director_attempt_is_rejected(
    sandbox_client: Tuple[TestClient, InMemorySessionStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, store = sandbox_client

    monkeypatch.setattr(config_sandbox, "_current_config", lambda: {"feature_flag": False})
    monkeypatch.setattr(
        config_sandbox,
        "_run_backtest",
        lambda config: {"pnl": 0.0, "sharpe": 0.0, "max_drawdown": 0.0},
    )

    called = False

    def _unexpected_promotion(_: dict[str, object]) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(config_sandbox, "_promote_config", _unexpected_promotion)

    headers = _auth_headers(store, "director-1")
    payload = {"config_changes": {"feature_flag": True}, "directors": ["director-1"]}

    response = client.post("/sandbox/test_config", json=payload, headers=headers)

    assert response.status_code == 403
    assert called is False


def test_dual_director_promotion_succeeds(
    sandbox_client: Tuple[TestClient, InMemorySessionStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, store = sandbox_client

    monkeypatch.setattr(config_sandbox, "_current_config", lambda: {"feature_flag": False})
    monkeypatch.setattr(
        config_sandbox,
        "_run_backtest",
        lambda config: {"pnl": 1.0, "sharpe": 0.5, "max_drawdown": -0.1},
    )

    promoted: dict[str, object] | None = None

    def _capture_promotion(candidate: dict[str, object]) -> None:
        nonlocal promoted
        promoted = candidate

    monkeypatch.setattr(config_sandbox, "_promote_config", _capture_promotion)

    headers = _auth_headers(store, "director-1")
    payload = {"config_changes": {"feature_flag": True}, "directors": ["director-2"]}

    response = client.post("/sandbox/test_config", json=payload, headers=headers)

    assert response.status_code == 200
    body = response.json()
    assert body["baseline_pnl"] == pytest.approx(1.0)
    assert body["new_pnl"] == pytest.approx(1.0)
    assert promoted == {"feature_flag": True}
