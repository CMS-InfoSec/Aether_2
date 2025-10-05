from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient


@pytest.fixture
def account_module(monkeypatch):
    key = Fernet.generate_key().decode("ascii")
    monkeypatch.setenv("ACCOUNT_ENCRYPTION_KEY", key)
    monkeypatch.setenv("ACCOUNTS_ALLOW_SQLITE_FOR_TESTS", "1")
    monkeypatch.setenv("ACCOUNTS_DATABASE_URL", "sqlite:///:memory:")
    root = Path(__file__).resolve().parents[2]
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

    package_path = root / "services"
    package_spec = importlib.util.spec_from_file_location(
        "services", package_path / "__init__.py", submodule_search_locations=[str(package_path)]
    )
    assert package_spec and package_spec.loader
    package_module = importlib.util.module_from_spec(package_spec)
    sys.modules["services"] = package_module
    package_spec.loader.exec_module(package_module)  # type: ignore[attr-defined]

    for dependency in ("k8s_sync_service", "kraken_test_service"):
        name = f"services.{dependency}"
        spec = importlib.util.spec_from_file_location(name, package_path / f"{dependency}.py")
        assert spec and spec.loader
        module_dep = importlib.util.module_from_spec(spec)
        sys.modules[name] = module_dep
        spec.loader.exec_module(module_dep)  # type: ignore[attr-defined]

    module_spec = importlib.util.spec_from_file_location(
        "services.account_service", package_path / "account_service.py"
    )
    assert module_spec and module_spec.loader
    module = importlib.util.module_from_spec(module_spec)
    sys.modules["services.account_service"] = module
    module_spec.loader.exec_module(module)  # type: ignore[attr-defined]
    module.Base.metadata.drop_all(module.ENGINE)
    module.Base.metadata.create_all(module.ENGINE)
    return module


@pytest.fixture
def admin_user(account_module):
    admin_id = uuid4()

    def _user() -> account_module.CurrentUser:  # type: ignore[name-defined]
        return account_module.CurrentUser(  # type: ignore[attr-defined]
            user_id=admin_id,
            role=account_module.Role.ADMIN,
            account_ids=set(),
        )

    return _user


@pytest.fixture
def client(account_module, admin_user):
    app = account_module.app
    app.dependency_overrides[account_module.get_current_user] = admin_user
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


def _create_account(client: TestClient, *, owner: UUID | None = None, with_keys: bool = False):
    owner_id = owner or uuid4()
    payload = {
        "name": "Alpha",
        "owner_user_id": str(owner_id),
        "base_currency": "USD",
        "sim_mode": False,
        "hedge_auto": True,
    }
    if with_keys:
        payload.update({
            "initial_api_key": "key123",
            "initial_api_secret": "secret456",
        })
    response = client.post("/accounts/create", json=payload)
    assert response.status_code == 200
    return response.json()


def test_create_account_with_api_keys(account_module, client, monkeypatch):
    called = {}

    def _fake_sync(account_id: str, api_key: str, api_secret: str) -> None:
        called["account_id"] = account_id
        called["api_key"] = api_key
        called["api_secret"] = api_secret

    def _fake_test(api_key: str, api_secret: str) -> dict[str, object]:
        return {"status": "ok", "permissions": ["trade"], "latency_ms": 1.0}

    monkeypatch.setattr(account_module, "sync_account_secret", _fake_sync)
    monkeypatch.setattr(account_module, "test_kraken_connection", _fake_test)

    result = _create_account(client, with_keys=True)
    assert UUID(result["account_id"])  # account id parsable
    assert result["kraken_status"]["status"] == "ok"
    assert called["account_id"] == result["account_id"]
    assert called["api_key"] == "key123"
    assert called["api_secret"] == "secret456"


def test_api_key_encryption_and_decryption(account_module, client, monkeypatch):
    monkeypatch.setattr(account_module, "sync_account_secret", lambda *args, **kwargs: None)
    monkeypatch.setattr(account_module, "test_kraken_connection", lambda *args, **kwargs: None)
    result = _create_account(client, with_keys=True)
    account_id = UUID(result["account_id"])
    with account_module.SessionLocal() as session:
        key_record = (
            session.query(account_module.KrakenKey)  # type: ignore[attr-defined]
            .filter_by(account_id=account_id)
            .one()
        )
        assert key_record.encrypted_api_key != "key123"
        assert key_record.encrypted_api_secret != "secret456"
        assert account_module._decrypt(key_record.encrypted_api_key) == "key123"  # type: ignore[attr-defined]
        assert account_module._decrypt(key_record.encrypted_api_secret) == "secret456"  # type: ignore[attr-defined]


def test_api_connectivity_success(account_module, client, monkeypatch):
    monkeypatch.setattr(account_module, "sync_account_secret", lambda *args, **kwargs: None)
    monkeypatch.setattr(account_module, "test_kraken_connection", lambda *args, **kwargs: {"status": "ok"})
    result = _create_account(client, with_keys=True)
    account_id = result["account_id"]

    def _director_user() -> account_module.CurrentUser:  # type: ignore[name-defined]
        return account_module.CurrentUser(  # type: ignore[attr-defined]
            user_id=uuid4(),
            role=account_module.Role.DIRECTOR,
            account_ids={UUID(account_id)},
        )

    previous = client.app.dependency_overrides[account_module.get_current_user]
    client.app.dependency_overrides[account_module.get_current_user] = _director_user
    try:
        response = client.get(f"/accounts/api/test/{account_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
    finally:
        client.app.dependency_overrides[account_module.get_current_user] = previous


def test_sim_toggle_reflects_in_db(account_module, client, monkeypatch):
    monkeypatch.setattr(account_module, "sync_account_secret", lambda *args, **kwargs: None)
    monkeypatch.setattr(account_module, "test_kraken_connection", lambda *args, **kwargs: None)
    result = _create_account(client, with_keys=False)
    account_id = UUID(result["account_id"])

    response = client.post(
        "/accounts/sim/toggle",
        json={"account_id": str(account_id), "enabled": True},
    )
    assert response.status_code == 200
    with account_module.SessionLocal() as session:
        account = session.get(account_module.Account, account_id)  # type: ignore[attr-defined]
        assert account.sim_mode is True


def test_governance_log_written_on_create(account_module, client, monkeypatch):
    monkeypatch.setattr(account_module, "sync_account_secret", lambda *args, **kwargs: None)
    monkeypatch.setattr(account_module, "test_kraken_connection", lambda *args, **kwargs: None)
    result = _create_account(client, with_keys=False)
    account_id = UUID(result["account_id"])

    with account_module.SessionLocal() as session:
        logs = (
            session.query(account_module.AuditLog)  # type: ignore[attr-defined]
            .filter_by(account_id=account_id)
            .all()
        )
        assert any(log.action == "account_created" for log in logs)


def test_k8s_secret_sync_called(account_module, client, monkeypatch):
    captured = {}

    def _capture(account_id: str, api_key: str, api_secret: str) -> None:
        captured["account_id"] = account_id
        captured["api_key"] = api_key
        captured["api_secret"] = api_secret

    monkeypatch.setattr(account_module, "sync_account_secret", _capture)
    monkeypatch.setattr(account_module, "test_kraken_connection", lambda *args, **kwargs: None)

    result = _create_account(client, with_keys=False)
    account_id = result["account_id"]

    response = client.post(
        "/accounts/api/upload",
        json={
            "account_id": account_id,
            "api_key": "live-key",
            "api_secret": "live-secret",
        },
    )
    assert response.status_code == 200
    assert captured["account_id"] == account_id
    assert captured["api_key"] == "live-key"
    assert captured["api_secret"] == "live-secret"
