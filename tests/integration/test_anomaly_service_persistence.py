from __future__ import annotations

import importlib.util
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Iterator, List, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit, quote_plus

import pytest
from fastapi.testclient import TestClient

psycopg = pytest.importorskip("psycopg", reason="psycopg is required for anomaly persistence tests")

pytestmark = pytest.mark.integration

_TEST_DSN = os.getenv("AETHER_ANOMALY_TEST_DSN") or os.getenv("AETHER_TIMESCALE_TEST_DSN")
if not _TEST_DSN:
    pytest.skip(
        "AETHER_ANOMALY_TEST_DSN or AETHER_TIMESCALE_TEST_DSN must be configured for anomaly persistence tests",
        allow_module_level=True,
    )


def _with_schema(dsn: str, schema: str) -> str:
    parts = urlsplit(dsn)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["options"] = f"-csearch_path={schema}"
    new_query = urlencode(query, doseq=True, quote_via=quote_plus)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def _load_module(module_name: str, database_url: str) -> ModuleType:
    module_path = Path(__file__).resolve().parents[2] / "anomaly_service.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load anomaly_service module")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    os.environ["ANOMALY_DATABASE_URL"] = database_url
    os.environ.pop("TIMESCALE_DSN", None)
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


class ModuleFactory:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.loaded: List[Tuple[str, ModuleType]] = []

    def load(self, alias: str | None = None) -> ModuleType:
        name = alias or f"anomaly_instance_{uuid.uuid4().hex[:8]}"
        if name in sys.modules:
            sys.modules.pop(name, None)
        module = _load_module(name, self.database_url)
        module.Base.metadata.create_all(bind=module.ENGINE)

        class _Detector:
            def scan_account(self, account_id: str, lookback_minutes: int):
                return [
                    module.Incident(
                        account_id=account_id,
                        anomaly_type="integration_test",
                        details={"source": "integration"},
                        ts=datetime.now(timezone.utc),
                    )
                ]

        class _Factory(module.ResponseFactory):
            def __init__(self) -> None:
                super().__init__(
                    detector=_Detector(),
                    repository=module.IncidentRepository(session_factory=module.SessionLocal),
                )

            def _raise_alert(self, incident):
                return None

            def _block_account(self, incident):
                return None

            def _is_blocked(self, account_id: str) -> bool:
                return False

        module._coordinator = _Factory()
        self.loaded.append((name, module))
        return module

    def unload(self, alias: str) -> None:
        for index, (name, module) in enumerate(list(self.loaded)):
            if name != alias:
                continue
            engine = getattr(module, "ENGINE", None)
            if engine is not None:
                engine.dispose()
            sys.modules.pop(name, None)
            self.loaded.pop(index)
            break


@pytest.fixture
def anomaly_environment() -> Iterator[ModuleFactory]:
    schema = f"anomaly_int_{uuid.uuid4().hex[:8]}"
    database_url = _with_schema(_TEST_DSN, schema)

    with psycopg.connect(_TEST_DSN) as conn:  # type: ignore[arg-type]
        with conn.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            cursor.execute(f'CREATE SCHEMA "{schema}"')
        conn.commit()

    factory = ModuleFactory(database_url=database_url)
    original_url = os.environ.get("ANOMALY_DATABASE_URL")
    original_timescale = os.environ.get("TIMESCALE_DSN")

    try:
        yield factory
    finally:
        for name, module in list(factory.loaded):
            engine = getattr(module, "ENGINE", None)
            if engine is not None:
                engine.dispose()
            sys.modules.pop(name, None)
        factory.loaded.clear()

        if original_url is None:
            os.environ.pop("ANOMALY_DATABASE_URL", None)
        else:
            os.environ["ANOMALY_DATABASE_URL"] = original_url

        if original_timescale is None:
            os.environ.pop("TIMESCALE_DSN", None)
        else:
            os.environ["TIMESCALE_DSN"] = original_timescale

        with psycopg.connect(_TEST_DSN) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            conn.commit()


def test_incidents_persist_across_restarts(anomaly_environment: ModuleFactory) -> None:
    first = anomaly_environment.load("anomaly_primary")
    first_client = TestClient(first.app)
    first_client.app.dependency_overrides[first.require_admin_account] = lambda: "company"

    response = first_client.post(
        "/anomaly/scan",
        json={"account_id": "company", "lookback_minutes": 15},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["incidents"], "Expected detector to log an incident"

    anomaly_environment.unload("anomaly_primary")

    second = anomaly_environment.load("anomaly_restart")
    second_client = TestClient(second.app)
    second_client.app.dependency_overrides[second.require_admin_account] = lambda: "company"

    status_response = second_client.get("/anomaly/status", params={"account_id": "company"})

    assert status_response.status_code == 200
    payload = status_response.json()
    assert payload["incidents"], "Expected persisted incidents to be available after restart"
    assert payload["incidents"][0]["anomaly_type"] == "integration_test"
