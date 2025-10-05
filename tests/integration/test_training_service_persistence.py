"""Integration tests verifying training service durability across restarts."""

from __future__ import annotations

import importlib.util
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import List, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit, quote_plus

import pytest

psycopg = pytest.importorskip("psycopg", reason="psycopg is required for training persistence tests")

pytestmark = pytest.mark.integration

_TRAINING_TEST_DSN = os.getenv("AETHER_TRAINING_TEST_DSN") or os.getenv("AETHER_TIMESCALE_TEST_DSN")
if not _TRAINING_TEST_DSN:
    pytest.skip(
        "AETHER_TRAINING_TEST_DSN or AETHER_TIMESCALE_TEST_DSN must be set for training persistence tests",
        allow_module_level=True,
    )


def _with_schema(dsn: str, schema: str) -> str:
    parts = urlsplit(dsn)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["options"] = f"-csearch_path={schema}"
    new_query = urlencode(query, doseq=True, quote_via=quote_plus)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def _load_training_module(name: str) -> ModuleType:
    module_path = Path(__file__).resolve().parents[2] / "training_service.py"
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load training_service module for tests")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


@dataclass
class TrainingModuleFactory:
    database_url: str
    loaded: List[Tuple[str, ModuleType]] = field(default_factory=list)

    def load(self, alias: str | None = None) -> ModuleType:
        name = alias or f"training_service_{uuid.uuid4().hex[:8]}"
        if name in sys.modules:
            sys.modules.pop(name, None)
        os.environ["TRAINING_DATABASE_URL"] = self.database_url
        os.environ.pop("TIMESCALE_DSN", None)
        os.environ.pop("TRAINING_ALLOW_SQLITE_FOR_TESTS", None)
        module = _load_training_module(name)
        self.loaded.append((name, module))
        return module

    def unload(self, alias: str) -> None:
        for index, (name, module) in enumerate(list(self.loaded)):
            if name != alias:
                continue
            engine = getattr(module, "ENGINE", None) or getattr(module, "engine", None)
            if engine is not None:
                try:
                    engine.dispose()
                except Exception:
                    pass
            sys.modules.pop(name, None)
            self.loaded.pop(index)
            break


@pytest.fixture
def training_environment() -> TrainingModuleFactory:
    schema = f"training_int_{uuid.uuid4().hex[:8]}"
    database_url = _with_schema(_TRAINING_TEST_DSN, schema)

    with psycopg.connect(_TRAINING_TEST_DSN) as conn:  # type: ignore[arg-type]
        with conn.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            cursor.execute(f'CREATE SCHEMA "{schema}"')
        conn.commit()

    factory = TrainingModuleFactory(database_url=database_url)
    original_url = os.environ.get("TRAINING_DATABASE_URL")
    original_timescale = os.environ.get("TIMESCALE_DSN")
    original_sqlite = os.environ.get("TRAINING_ALLOW_SQLITE_FOR_TESTS")

    try:
        yield factory
    finally:
        for name, module in list(factory.loaded):
            engine = getattr(module, "ENGINE", None) or getattr(module, "engine", None)
            if engine is not None:
                try:
                    engine.dispose()
                except Exception:
                    pass
            sys.modules.pop(name, None)
        factory.loaded.clear()

        if original_url is None:
            os.environ.pop("TRAINING_DATABASE_URL", None)
        else:
            os.environ["TRAINING_DATABASE_URL"] = original_url

        if original_timescale is None:
            os.environ.pop("TIMESCALE_DSN", None)
        else:
            os.environ["TIMESCALE_DSN"] = original_timescale

        if original_sqlite is None:
            os.environ.pop("TRAINING_ALLOW_SQLITE_FOR_TESTS", None)
        else:
            os.environ["TRAINING_ALLOW_SQLITE_FOR_TESTS"] = original_sqlite

        with psycopg.connect(_TRAINING_TEST_DSN) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            conn.commit()


def test_training_history_survives_restart(training_environment: TrainingModuleFactory) -> None:
    first = training_environment.load("training_service_primary")

    run_id = f"run-{uuid.uuid4()}"
    now = datetime.now(timezone.utc)
    with first.session_scope() as session:
        session.add(
            first.TrainingRunRecord(
                run_id=run_id,
                run_name="integration-test",
                status="completed",
                current_step="finalize",
                request_payload={"symbols": ["BTC-USD"]},
                metrics={"sharpe": 1.23},
                correlation_id=str(uuid.uuid4()),
                feature_version="v1",
                label_horizon="15m",
                model_type="lstm",
                curriculum=0,
                started_at=now,
                finished_at=now,
            )
        )

    training_environment.unload("training_service_primary")

    restarted = training_environment.load("training_service_restart")
    try:
        with restarted.session_scope() as session:
            record = session.get(restarted.TrainingRunRecord, run_id)

        assert record is not None
        assert record.run_name == "integration-test"
        assert record.status == "completed"
    finally:
        training_environment.unload("training_service_restart")

