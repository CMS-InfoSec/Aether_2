from __future__ import annotations

import pytest

import sys
import types
import uuid
from dataclasses import replace
from types import MappingProxyType
from typing import Any, Mapping

if "cryptography" not in sys.modules:
    crypto_mod = types.ModuleType("cryptography")
    hazmat_mod = types.ModuleType("cryptography.hazmat")
    primitives_mod = types.ModuleType("cryptography.hazmat.primitives")
    ciphers_mod = types.ModuleType("cryptography.hazmat.primitives.ciphers")
    aead_mod = types.ModuleType("cryptography.hazmat.primitives.ciphers.aead")

    class _StubAESGCM:
        def __init__(self, *args: object, **kwargs: object) -> None:  # pragma: no cover - simple stub
            self._key = args[0] if args else None

        def encrypt(self, nonce: bytes, data: bytes, associated_data: bytes | None = None) -> bytes:
            return data

        def decrypt(self, nonce: bytes, data: bytes, associated_data: bytes | None = None) -> bytes:
            return data

    aead_mod.AESGCM = _StubAESGCM
    crypto_mod.hazmat = hazmat_mod
    hazmat_mod.primitives = primitives_mod
    primitives_mod.ciphers = ciphers_mod
    ciphers_mod.aead = aead_mod

    sys.modules["cryptography"] = crypto_mod
    sys.modules["cryptography.hazmat"] = hazmat_mod
    sys.modules["cryptography.hazmat.primitives"] = primitives_mod
    sys.modules["cryptography.hazmat.primitives.ciphers"] = ciphers_mod
    sys.modules["cryptography.hazmat.primitives.ciphers.aead"] = aead_mod

from services.common.adapters import TimescaleAdapter
from shared.audit import (
    AuditLogEntry,
    AuditLogStore,
    SensitiveActionRecorder,
    TimescaleAuditLogger,
)


class _FlakyBackend:
    def __init__(self) -> None:
        self.attempts = 0
        self.records: list[dict[str, Any]] = []

    def record_audit_log(self, record: Mapping[str, Any]) -> None:
        self.attempts += 1
        if self.attempts == 1:
            raise RuntimeError("transient failure")
        self.records.append(dict(record))

    def audit_logs(self) -> list[dict[str, Any]]:
        return [dict(entry) for entry in self.records]


def test_audit_log_entries_are_immutable():
    TimescaleAdapter.reset(account_id="immutable")
    store = AuditLogStore(account_id="immutable")
    logger = TimescaleAuditLogger(store)
    recorder = SensitiveActionRecorder(logger)

    entry = recorder.record(
        action="secret_rotation",
        actor_id="admin-9",
        before={"secret": "kraken", "status": "active"},
        after={"secret": "kraken", "status": "rotated"},
    )

    with pytest.raises(TypeError):
        entry.after["status"] = "tampered"

    entries = list(store.all())
    assert entries[0] == entry
    assert entries[0].account_id == "immutable"


def test_audit_log_store_persists_entries_across_instances():
    TimescaleAdapter.reset(account_id="persistence")
    store = AuditLogStore(account_id="persistence")
    logger = TimescaleAuditLogger(store)

    entry = logger.record(
        action="config.update",
        actor_id="system",
        before={"feature": False},
        after={"feature": True},
        correlation_id="corr-123",
    )

    fresh_store = AuditLogStore(account_id="persistence")
    persisted = list(fresh_store.all())

    assert persisted == [entry]
    assert persisted[0].account_id == "persistence"
    with pytest.raises(TypeError):
        persisted[0].before["feature"] = True


def test_audit_log_store_retries_transient_errors():
    backend = _FlakyBackend()
    store = AuditLogStore(
        backend=backend,
        max_retries=1,
        backoff_seconds=0.0,
        sleep_fn=lambda _: None,
    )
    logger = TimescaleAuditLogger(store)

    entry = logger.record(
        action="secret.rotate",
        actor_id="auditor",
        before={"status": "active"},
        after={"status": "rotated"},
    )

    assert backend.attempts == 2
    assert list(store.all()) == [entry]


class _AppendOnlyBackend:
    def __init__(self) -> None:
        self.records: dict[str, Mapping[str, Any]] = {}

    def record_audit_log(self, record: Mapping[str, Any]) -> None:
        entry_id = str(record.get("id"))
        if entry_id in self.records:
            return
        self.records[entry_id] = dict(record)

    def audit_logs(self) -> list[Mapping[str, Any]]:
        return [dict(value) for value in self.records.values()]


def test_audit_log_entries_are_append_only():
    backend = _AppendOnlyBackend()
    store = AuditLogStore(account_id="append-only", backend=backend)

    entry_id = uuid.uuid4()
    original = AuditLogEntry(
        id=entry_id,
        action="trade.submit",
        actor_id="desk-1",
        account_id="append-only",
        before=MappingProxyType({"intent": "buy"}),
        after=MappingProxyType({"status": "submitted"}),
        correlation_id="order-1",
    )
    tampered = replace(
        original,
        after=MappingProxyType({"status": "cancelled"}),
    )

    store.append(original)
    store.append(tampered)

    entries = list(store.all())
    assert entries == [original]
