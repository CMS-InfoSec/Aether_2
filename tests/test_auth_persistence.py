from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

import pyotp
from fastapi.testclient import TestClient

from app import create_app
from auth.service import (
    AdminAccount,
    PostgresAdminRepository,
    RedisSessionStore,
    hash_password,
)


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


class _MemoryRedis:
    def __init__(self) -> None:
        self._values: Dict[str, tuple[bytes, datetime]] = {}

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        expires = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        self._values[key] = (value.encode("utf-8"), expires)

    def get(self, key: str) -> Optional[bytes]:
        entry = self._values.get(key)
        if not entry:
            return None
        value, expires = entry
        if datetime.now(timezone.utc) >= expires:
            self._values.pop(key, None)
            return None
        return value

    def delete(self, key: str) -> None:
        self._values.pop(key, None)


def _build_app_with_memory_backends():
    psycopg = _MemoryPsycopg()
    redis_client = _MemoryRedis()
    admin_repo = PostgresAdminRepository("postgresql://stub", psycopg_module=psycopg)
    session_store = RedisSessionStore(redis_client, ttl_minutes=60)
    return create_app(admin_repository=admin_repo, session_store=session_store), admin_repo, session_store


def test_login_and_session_survive_restart_cycle():
    app, admin_repo, session_store = _build_app_with_memory_backends()
    client = TestClient(app)

    secret = pyotp.random_base32()
    admin = AdminAccount(
        admin_id="admin-42",
        email="persistence@example.com",
        password_hash=hash_password("CorrectHorseBatteryStaple"),
        mfa_secret=secret,
    )
    admin_repo.add(admin)

    first_code = pyotp.TOTP(secret).now()
    response = client.post(
        "/auth/login",
        json={
            "email": admin.email,
            "password": "CorrectHorseBatteryStaple",
            "mfa_code": first_code,
        },
    )
    assert response.status_code == 200
    token = response.json()["token"]

    # Simulate application restart with shared infrastructure dependencies
    restarted_app = create_app(admin_repository=admin_repo, session_store=session_store)
    restarted_client = TestClient(restarted_app)

    persisted_session = restarted_app.state.session_store.get(token)
    assert persisted_session is not None
    assert persisted_session.admin_id == admin.admin_id

    second_code = pyotp.TOTP(secret).now()
    response = restarted_client.post(
        "/auth/login",
        json={
            "email": admin.email,
            "password": "CorrectHorseBatteryStaple",
            "mfa_code": second_code,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["admin_id"] == admin.admin_id
    assert body["token"]
