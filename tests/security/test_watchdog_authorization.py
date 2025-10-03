"""Authorization tests for the watchdog oversight endpoint."""

from __future__ import annotations

import importlib.util
import os
import sys
import uuid
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit, quote_plus

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest.importorskip("fastapi", reason="fastapi is required for watchdog authorization tests")
psycopg = pytest.importorskip("psycopg", reason="psycopg is required for watchdog authorization tests")

from fastapi.testclient import TestClient

if TYPE_CHECKING:
    from auth.service import InMemorySessionStore

_WATCHDOG_TEST_DSN = os.getenv("AETHER_WATCHDOG_TEST_DSN") or os.getenv("AETHER_TIMESCALE_TEST_DSN")
if not _WATCHDOG_TEST_DSN:
    pytest.skip(
        "AETHER_WATCHDOG_TEST_DSN or AETHER_TIMESCALE_TEST_DSN must be set for watchdog tests",
        allow_module_level=True,
    )


def _with_schema(dsn: str, schema: str) -> str:
    parts = urlsplit(dsn)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["options"] = f"-csearch_path={schema}"
    new_query = urlencode(query, doseq=True, quote_via=quote_plus)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def _load_watchdog_module(name: str) -> ModuleType:
    module_path = Path(__file__).resolve().parents[3] / "watchdog.py"
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load watchdog module for tests")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


@pytest.fixture(scope="module")
def watchdog_module() -> ModuleType:
    schema = f"watchdog_auth_{uuid.uuid4().hex[:8]}"
    database_url = _with_schema(_WATCHDOG_TEST_DSN, schema)

    with psycopg.connect(_WATCHDOG_TEST_DSN) as conn:  # type: ignore[arg-type]
        with conn.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            cursor.execute(f'CREATE SCHEMA "{schema}"')
        conn.commit()

    previous_url = os.environ.get("WATCHDOG_DATABASE_URL")
    previous_timescale = os.environ.get("TIMESCALE_DSN")
    os.environ["WATCHDOG_DATABASE_URL"] = database_url
    os.environ.pop("TIMESCALE_DSN", None)

    module = _load_watchdog_module("watchdog")

    try:
        yield module
    finally:
        engine = getattr(module, "ENGINE", None)
        if engine is not None:
            engine.dispose()
        sys.modules.pop("watchdog", None)

        if previous_url is None:
            os.environ.pop("WATCHDOG_DATABASE_URL", None)
        else:
            os.environ["WATCHDOG_DATABASE_URL"] = previous_url

        if previous_timescale is None:
            os.environ.pop("TIMESCALE_DSN", None)
        else:
            os.environ["TIMESCALE_DSN"] = previous_timescale

        with psycopg.connect(_WATCHDOG_TEST_DSN) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            conn.commit()


@pytest.fixture
def watchdog_client(watchdog_module: ModuleType) -> tuple[TestClient, "InMemorySessionStore"]:
    """Provide a TestClient with an isolated session store."""

    from auth.service import InMemorySessionStore
    from services.common.security import set_default_session_store

    store = InMemorySessionStore()
    set_default_session_store(store)
    watchdog_module.app.state.session_store = store

    try:
        with TestClient(watchdog_module.app) as client:
            yield client, store
    finally:
        set_default_session_store(None)
        if hasattr(watchdog_module.app.state, "session_store"):
            delattr(watchdog_module.app.state, "session_store")


def test_oversight_status_requires_authenticated_session(
    watchdog_client: tuple[TestClient, "InMemorySessionStore"]
) -> None:
    client, _store = watchdog_client

    response = client.get("/oversight/status")

    assert response.status_code == 401


def test_oversight_status_rejects_non_admin_sessions(
    watchdog_client: tuple[TestClient, "InMemorySessionStore"]
) -> None:
    client, store = watchdog_client
    session = store.create("guest")

    response = client.get(
        "/oversight/status",
        headers={"Authorization": f"Bearer {session.token}"},
    )

    assert response.status_code == 403


def test_oversight_status_allows_admin_sessions(
    watchdog_client: tuple[TestClient, "InMemorySessionStore"]
) -> None:
    client, store = watchdog_client
    session = store.create("company")

    response = client.get(
        "/oversight/status",
        headers={"Authorization": f"Bearer {session.token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert "total_vetoes" in payload
