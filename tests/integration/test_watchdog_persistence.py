"""Integration tests that exercise watchdog persistence across instances."""

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

psycopg = pytest.importorskip("psycopg", reason="psycopg is required for watchdog integration tests")

pytestmark = pytest.mark.integration

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


@dataclass
class WatchdogModuleFactory:
    database_url: str
    loaded: List[Tuple[str, ModuleType]] = field(default_factory=list)

    def load(self, alias: str | None = None) -> ModuleType:
        name = alias or f"watchdog_instance_{uuid.uuid4().hex[:8]}"
        if name in sys.modules:
            sys.modules.pop(name, None)
        os.environ["WATCHDOG_DATABASE_URL"] = self.database_url
        os.environ.pop("TIMESCALE_DSN", None)
        module = _load_watchdog_module(name)
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
def watchdog_environment() -> WatchdogModuleFactory:
    schema = f"watchdog_int_{uuid.uuid4().hex[:8]}"
    database_url = _with_schema(_WATCHDOG_TEST_DSN, schema)

    with psycopg.connect(_WATCHDOG_TEST_DSN) as conn:  # type: ignore[arg-type]
        with conn.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            cursor.execute(f'CREATE SCHEMA "{schema}"')
        conn.commit()

    factory = WatchdogModuleFactory(database_url=database_url)
    original_url = os.environ.get("WATCHDOG_DATABASE_URL")
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
            os.environ.pop("WATCHDOG_DATABASE_URL", None)
        else:
            os.environ["WATCHDOG_DATABASE_URL"] = original_url

        if original_timescale is None:
            os.environ.pop("TIMESCALE_DSN", None)
        else:
            os.environ["TIMESCALE_DSN"] = original_timescale

        with psycopg.connect(_WATCHDOG_TEST_DSN) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            conn.commit()


def test_watchdog_veto_shared_across_instances(watchdog_environment: WatchdogModuleFactory) -> None:
    first = watchdog_environment.load("watchdog_instance_primary")

    veto = first.WatchdogVeto(
        intent_id="intent-123",
        account_id="company",
        reason="test veto",
        score=0.42,
        details={"source": "test"},
        ts=datetime.now(timezone.utc),
    )

    assert first.WATCHDOG_REPOSITORY.record(veto)

    peer = watchdog_environment.load("watchdog_instance_peer")
    total, entries, counts = peer.WATCHDOG_REPOSITORY.summary(limit=5)

    assert total == 1
    assert entries, "Expected peer instance to observe persisted vetoes"
    assert entries[0].intent_id == veto.intent_id
    assert any(reason == veto.reason for reason, _count in counts)

    watchdog_environment.unload("watchdog_instance_primary")
    watchdog_environment.unload("watchdog_instance_peer")

    restarted = watchdog_environment.load("watchdog_instance_restart")
    total_after_restart, entries_after_restart, _ = restarted.WATCHDOG_REPOSITORY.summary(limit=5)

    assert total_after_restart == 1
    assert entries_after_restart, "Expected vetoes to persist across restarts"
    assert entries_after_restart[0].intent_id == veto.intent_id
