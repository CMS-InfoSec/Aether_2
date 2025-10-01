"""Integration test covering secret rotation and OMS hot-reload."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter
from services.secrets import secrets_service
from services.secrets.secure_secrets import EncryptedSecretEnvelope, SecretsMetadataStore
from services.oms import main as oms_main
from services.oms.kraken_client import KrakenWSClient, _LoopbackSession
from services.oms.oms_kraken import KrakenCredentialWatcher
from shared.k8s import KrakenSecretStore


class _RecordingKrakenWSClient(KrakenWSClient):
    """Kraken client wrapper that records credential material used for sessions."""

    last_session_credentials: Dict[str, Any] | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401 - constructor shim
        def _recording_factory(credentials: Dict[str, Any]) -> Any:
            _RecordingKrakenWSClient.last_session_credentials = dict(credentials)
            return _LoopbackSession(credentials)

        super().__init__(*args, session_factory=_recording_factory, **kwargs)


class _FakeCoreV1Api:
    """Minimal CoreV1Api emulator backed by the in-memory secret store."""

    def __init__(self) -> None:
        self._store = KrakenSecretStore(namespace=secrets_service.KRAKEN_SECRET_NAMESPACE)

    @staticmethod
    def _account_from_secret(name: str) -> str:
        prefix = "kraken-keys-"
        return name[len(prefix) :] if name.startswith(prefix) else name

    def read_namespaced_secret(self, name: str, namespace: str) -> SimpleNamespace:
        if namespace != self._store.namespace:
            raise secrets_service.ApiException(status=404)
        payload = self._store.get_secret(name)
        if not payload:
            raise secrets_service.ApiException(status=404)
        metadata = payload.get("metadata") or {}
        annotations = metadata.get("annotations") or {}
        meta = SimpleNamespace(name=metadata.get("name", name), annotations=annotations)
        data = payload.get("data") or {}
        return SimpleNamespace(metadata=meta, data=data)

    def patch_namespaced_secret(self, name: str, namespace: str, body: Dict[str, Any]) -> None:
        if namespace != self._store.namespace:
            raise secrets_service.ApiException(status=404)
        ns_store = KrakenSecretStore._store.setdefault(namespace, {})  # type: ignore[attr-defined]
        if name not in ns_store:
            raise secrets_service.ApiException(status=404)
        data = dict(body.get("data") or {})
        envelope = EncryptedSecretEnvelope.from_secret_data(data)
        account_id = self._account_from_secret(name)
        self._store.write_encrypted_secret(account_id, envelope=envelope)
        record = ns_store[name]
        annotations = (body.get("metadata") or {}).get("annotations") or {}
        if annotations:
            record["annotations"] = annotations
            created = annotations.get(secrets_service.ANNOTATION_CREATED_AT)
            rotated = annotations.get(secrets_service.ANNOTATION_ROTATED_AT)
            if created:
                record["created_at"] = created
            if rotated:
                record["rotated_at"] = rotated
        ns_store[name] = record

    def create_namespaced_secret(self, namespace: str, body: Dict[str, Any]) -> None:
        if namespace != self._store.namespace:
            raise secrets_service.ApiException(status=404)
        name = str(body.get("metadata", {}).get("name"))
        data = dict(body.get("data") or {})
        envelope = EncryptedSecretEnvelope.from_secret_data(data)
        account_id = self._account_from_secret(name)
        self._store.write_encrypted_secret(account_id, envelope=envelope)
        ns_store = KrakenSecretStore._store.setdefault(namespace, {})  # type: ignore[attr-defined]
        record = ns_store[name]
        annotations = (body.get("metadata") or {}).get("annotations") or {}
        if annotations:
            record["annotations"] = annotations
            created = annotations.get(secrets_service.ANNOTATION_CREATED_AT)
            rotated = annotations.get(secrets_service.ANNOTATION_ROTATED_AT)
            if created:
                record["created_at"] = created
            if rotated:
                record["rotated_at"] = rotated
        ns_store[name] = record


@pytest.mark.integration
def test_rotate_secret_triggers_oms_reload(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """Rotating credentials should hot-reload the OMS without interrupting order flow."""

    account_id = "company"
    initial_key = "old-key-123456"
    initial_secret = "old-secret-abcdef"
    new_key = "new-key-654321"
    new_secret = "new-secret-fedcba"

    KafkaNATSAdapter.reset()
    TimescaleAdapter.reset()
    KrakenSecretStore.reset()
    SecretsMetadataStore._records = []  # type: ignore[attr-defined]
    KrakenCredentialWatcher.reset_instances()

    store = KrakenSecretStore()
    store.write_credentials(account_id, api_key=initial_key, api_secret=initial_secret)
    TimescaleAdapter(account_id=account_id).record_credential_rotation(
        secret_name=store.secret_name(account_id),
        rotated_at=datetime.now(timezone.utc),
    )

    watcher = KrakenCredentialWatcher.instance(account_id)
    snapshot, version = watcher.snapshot()
    assert snapshot["api_key"] == initial_key

    secrets_service.app.dependency_overrides[secrets_service.get_core_v1_api] = lambda: _FakeCoreV1Api()
    monkeypatch.setattr(oms_main, "KrakenWSClient", _RecordingKrakenWSClient)

    try:
        with TestClient(secrets_service.app, base_url="https://testserver") as secrets_client:
            with caplog.at_level(logging.INFO, logger="secrets_log"):
                response = secrets_client.post(
                    "/secrets/rotate",
                    json={
                        "account_id": account_id,
                        "api_key": new_key,
                        "api_secret": new_secret,
                    },
                    headers={"X-Account-ID": account_id, "X-MFA-Context": "verified"},
                )

        assert response.status_code == 200
        payload = response.json()
        assert payload["account_id"] == account_id
        assert payload["secret_name"] == store.secret_name(account_id)
        assert "kms_key_id" in payload

        assert watcher.wait_for_version(version + 1, timeout=5.0)

        log_records = [record for record in caplog.records if record.name == "secrets_log"]
        assert any(
            record.message == "credential_rotation"
            and getattr(record, "secret_rotation", {}).get("account_id") == account_id
            for record in log_records
        )

        with TestClient(oms_main.app) as oms_client:
            order_response = oms_client.post(
                "/oms/place",
                json={
                    "account_id": account_id,
                    "order_id": "rotate-test-1",
                    "instrument": "BTC-USD",
                    "side": "BUY",
                    "quantity": 0.1,
                    "price": 25000.0,
                    "fee": {"currency": "USD", "maker": 0.1, "taker": 0.2},
                },
                headers={"X-Account-ID": account_id},
            )

        assert order_response.status_code == 200
        order_payload = order_response.json()
        assert order_payload["accepted"] is True
        assert order_payload["routed_venue"] == "kraken"

        recorded = _RecordingKrakenWSClient.last_session_credentials or {}
        assert recorded.get("api_key") == new_key
        assert recorded.get("api_secret") == new_secret
    finally:
        secrets_service.app.dependency_overrides.pop(secrets_service.get_core_v1_api, None)
        KrakenCredentialWatcher.reset_instances()

