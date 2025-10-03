import importlib.util
import sys
from pathlib import Path

import pytest
from fastapi import status
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
TESTS_DIR = ROOT / "tests"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

removed_tests_dir = False
if str(TESTS_DIR) in sys.path:
    sys.path.remove(str(TESTS_DIR))
    removed_tests_dir = True


def _load_module(name: str, relative_path: str) -> None:
    if name in sys.modules:
        return
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise ImportError(f"Unable to load module {name} from {relative_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)


_load_module("services", "services/__init__.py")
_load_module("services.common", "services/common/__init__.py")
_load_module("services.common.config", "services/common/config.py")
_load_module("services.common.security", "services/common/security.py")

import governance_simulator as module

if removed_tests_dir:
    sys.path.insert(0, str(TESTS_DIR))


@pytest.fixture
def governance_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setattr(module, "_ensure_tables", lambda: None)
    monkeypatch.setattr(module, "_record_simulation", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_current_config", lambda: {"risk": 1.0})

    def _mock_run_backtest(config: dict[str, float]) -> dict[str, float]:
        risk = float(config.get("risk", 1.0))
        return {"max_drawdown": risk * 0.1, "pnl": risk * 100.0}

    monkeypatch.setattr(module, "_run_backtest", _mock_run_backtest)

    from auth.service import InMemorySessionStore
    from services.common import security as security_module

    store = InMemorySessionStore(ttl_minutes=120)
    security_module.set_default_session_store(store)

    def _issue_token(account_id: str) -> str:
        session = store.create(account_id)
        return session.token

    with TestClient(module.app) as client:
        client.app.state.session_store = store
        setattr(client, "issue_token", _issue_token)
        try:
            yield client
        finally:
            security_module.set_default_session_store(None)


def test_simulation_requires_authentication(governance_client: TestClient) -> None:
    response = governance_client.post("/governance/simulate", json={"config_changes": {}})
    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_simulation_rejects_non_admin_account(governance_client: TestClient) -> None:
    token = governance_client.issue_token("researcher")  # type: ignore[attr-defined]
    response = governance_client.post(
        "/governance/simulate",
        json={"config_changes": {}},
        headers={"X-Account-ID": "researcher", "Authorization": f"Bearer {token}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN


def test_simulation_rejects_metadata_mismatch(governance_client: TestClient) -> None:
    token = governance_client.issue_token("company")  # type: ignore[attr-defined]
    response = governance_client.post(
        "/governance/simulate",
        json={
            "config_changes": {"risk": 2.0},
            "metadata": {"requested_by": "shadow"},
        },
        headers={"X-Account-ID": "company", "Authorization": f"Bearer {token}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN
    assert response.json()["detail"] == "Metadata account does not match authenticated session."


def test_simulation_accepts_matching_metadata(governance_client: TestClient) -> None:
    token = governance_client.issue_token("company")  # type: ignore[attr-defined]
    response = governance_client.post(
        "/governance/simulate",
        json={
            "config_changes": {"risk": 2.0},
            "metadata": {"requested_by": "company"},
        },
        headers={"X-Account-ID": "company", "Authorization": f"Bearer {token}"},
    )
    assert response.status_code == status.HTTP_200_OK

    body = response.json()
    assert body == {
        "baseline_drawdown": 0.1,
        "new_drawdown": 0.2,
        "drawdown_delta": 0.1,
        "baseline_pnl": 100.0,
        "new_pnl": 200.0,
        "pnl_delta": 100.0,
    }
