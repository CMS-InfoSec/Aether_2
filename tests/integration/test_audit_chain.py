"""Integration test ensuring audit chain integrity across services."""
from __future__ import annotations

import base64
import hashlib
import importlib
import json
import sys
from types import ModuleType, SimpleNamespace
from typing import Any, Dict

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient
from unittest.mock import MagicMock

from common.utils import audit_logger


class _FakeApiException(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"status={status}")
        self.status = status


class _FakeConfigException(Exception):
    """Raised when Kubernetes configuration loading fails."""


class _FakeSecret:
    def __init__(self, name: str, annotations: Dict[str, str], data: Dict[str, str]) -> None:
        self.metadata = SimpleNamespace(name=name, annotations=annotations)
        self.data = data


class _FakeCoreV1Api:
    def __init__(self) -> None:
        self._store: Dict[str, _FakeSecret] = {}

    def patch_namespaced_secret(self, *, name: str, namespace: str, body: Dict[str, Any]) -> None:
        secret = self._store.get(name)
        if secret is None:
            raise _FakeApiException(404)
        secret.metadata.annotations = dict(body.get("metadata", {}).get("annotations", {}))
        secret.data = dict(body.get("data", {}))

    def create_namespaced_secret(self, *, namespace: str, body: Dict[str, Any]) -> None:
        name = body.get("metadata", {}).get("name")
        if not name:
            raise ValueError("Secret name is required")
        annotations = dict(body.get("metadata", {}).get("annotations", {}))
        data = dict(body.get("data", {}))
        self._store[name] = _FakeSecret(name=name, annotations=annotations, data=data)

    def read_namespaced_secret(self, name: str, namespace: str) -> _FakeSecret:
        secret = self._store.get(name)
        if secret is None:
            raise _FakeApiException(404)
        return secret


@pytest.mark.integration
def test_audit_chain_across_services(tmp_path, monkeypatch, capsys):
    fake_core = _FakeCoreV1Api()

    kubernetes_module = ModuleType("kubernetes")
    client_module = ModuleType("kubernetes.client")
    rest_module = ModuleType("kubernetes.client.rest")
    config_module = ModuleType("kubernetes.config")
    config_exc_module = ModuleType("kubernetes.config.config_exception")

    client_module.CoreV1Api = lambda: fake_core  # type: ignore[attr-defined]
    rest_module.ApiException = _FakeApiException  # type: ignore[attr-defined]
    config_module.load_incluster_config = lambda: None  # type: ignore[attr-defined]
    config_module.load_kube_config = lambda: None  # type: ignore[attr-defined]
    config_exc_module.ConfigException = _FakeConfigException  # type: ignore[attr-defined]

    kubernetes_module.client = client_module  # type: ignore[attr-defined]
    kubernetes_module.config = config_module  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "kubernetes", kubernetes_module)
    monkeypatch.setitem(sys.modules, "kubernetes.client", client_module)
    monkeypatch.setitem(sys.modules, "kubernetes.client.rest", rest_module)
    monkeypatch.setitem(sys.modules, "kubernetes.config", config_module)
    monkeypatch.setitem(sys.modules, "kubernetes.config.config_exception", config_exc_module)

    chain_log = tmp_path / "chain.log"
    chain_state = tmp_path / "chain_state.json"
    monkeypatch.setenv("AUDIT_DATABASE_URL", "postgresql://audit:audit@localhost/audit")
    monkeypatch.setenv("AUDIT_CHAIN_LOG", str(chain_log))
    monkeypatch.setenv("AUDIT_CHAIN_STATE", str(chain_state))

    monkeypatch.setenv("CONFIG_DATABASE_URL", f"sqlite:///{tmp_path / 'config.db'}")
    monkeypatch.setenv("OVERRIDE_DATABASE_URL", f"sqlite:///{tmp_path / 'override.db'}")
    encryption_key = base64.b64encode(b"0" * 32).decode()
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", encryption_key)
    monkeypatch.setenv("SECRETS_SERVICE_AUTH_TOKENS", "integration-token")

    conn_mock, cursor_mock = _make_connection_mock()
    monkeypatch.setattr(audit_logger, "psycopg", MagicMock(connect=MagicMock(return_value=conn_mock)))

    for module_name in ("config_service", "override_service", "secrets_service"):
        sys.modules.pop(module_name, None)

    config_service = importlib.import_module("config_service")
    override_service = importlib.import_module("override_service")
    secrets_service = importlib.import_module("secrets_service")

    config_service.reset_state()
    override_service.Base.metadata.drop_all(bind=override_service.ENGINE)
    override_service.Base.metadata.create_all(bind=override_service.ENGINE)
    secrets_service.secret_manager = secrets_service.KrakenSecretManager(
        secrets_service.SETTINGS.kubernetes_namespace
    )
    secrets_service.secret_manager._client = fake_core  # type: ignore[attr-defined]

    with TestClient(config_service.app) as config_client:
        response = config_client.post(
            "/config/update",
            params={"account_id": "global"},
            json={"key": "feature.enabled", "value": {"enabled": True}, "author": "alice"},
        )
        assert response.status_code == 200

    with TestClient(secrets_service.app) as secrets_client:
        response = secrets_client.post(
            "/secrets/kraken",
            json={
                "account_id": "admin-eu",
                "api_key": "api-key-123",
                "api_secret": "api-secret-xyz",
                "actor": "sre.bob",
            },
            headers={"Authorization": "Bearer integration-token"},
        )
        assert response.status_code == 201

    with TestClient(override_service.app) as override_client:
        response = override_client.post(
            "/override/trade",
            json={
                "intent_id": "order-42",
                "decision": "approve",
                "reason": "Risk mitigation check",
            },
            headers={"X-Account-ID": "company", "X-Actor": "director.carla"},
        )
        assert response.status_code == 201

    entries = []
    with chain_log.open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                entries.append(json.loads(line))

    assert [entry["action"] for entry in entries] == [
        "config.change.applied",
        "secret.kraken.rotate",
        "override.human_decision",
    ]

    prev_entry_hash = audit_logger._GENESIS_HASH  # pylint: disable=protected-access
    for entry in entries:
        expected_prev_hash = hashlib.sha256(prev_entry_hash.encode("utf-8")).hexdigest()
        assert entry["prev_hash"] == expected_prev_hash
        canonical = audit_logger._canonical_payload(entry)  # pylint: disable=protected-access
        serialized = audit_logger._canonical_serialized(canonical)  # pylint: disable=protected-access
        expected_hash = hashlib.sha256((expected_prev_hash + serialized).encode("utf-8")).hexdigest()
        assert entry["hash"] == expected_hash
        prev_entry_hash = entry["hash"]

    capsys.readouterr()
    result = audit_logger.main(["verify"])
    assert result == 0
    out = capsys.readouterr().out
    assert "Audit chain verified successfully." in out

    assert cursor_mock.execute.call_count == 3


def _make_connection_mock():
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value = cursor
    return conn, cursor
