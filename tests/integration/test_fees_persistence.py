"""Integration tests validating persistence for the fees service."""

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
from sqlalchemy import select

psycopg = pytest.importorskip("psycopg", reason="psycopg is required for fees integration tests")

pytestmark = pytest.mark.integration

_FEES_TEST_DSN = os.getenv("AETHER_FEES_TEST_DSN") or os.getenv("AETHER_TIMESCALE_TEST_DSN")
if not _FEES_TEST_DSN:
    pytest.skip(
        "AETHER_FEES_TEST_DSN or AETHER_TIMESCALE_TEST_DSN must be set for fees integration tests",
        allow_module_level=True,
    )


def _with_schema(dsn: str, schema: str) -> str:
    parts = urlsplit(dsn)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["options"] = f"-csearch_path={schema}"
    new_query = urlencode(query, doseq=True, quote_via=quote_plus)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def _load_fees_module(name: str) -> ModuleType:
    module_path = Path(__file__).resolve().parents[2] / "services" / "fees" / "fee_service.py"
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load fees service module for tests")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


@dataclass
class FeesModuleFactory:
    database_url: str
    loaded: List[Tuple[str, ModuleType]] = field(default_factory=list)

    def load(self, alias: str | None = None) -> ModuleType:
        name = alias or f"fees_instance_{uuid.uuid4().hex[:8]}"
        if name in sys.modules:
            sys.modules.pop(name, None)
        os.environ["FEES_DATABASE_URL"] = self.database_url
        os.environ.pop("TIMESCALE_DSN", None)
        module = _load_fees_module(name)
        self.loaded.append((name, module))
        return module

    def unload(self, alias: str) -> None:
        for index, (name, module) in enumerate(list(self.loaded)):
            if name != alias:
                continue
            session_local = getattr(module, "SessionLocal", None)
            if session_local is not None and hasattr(session_local, "close_all"):
                session_local.close_all()
            engine = getattr(module, "ENGINE", None)
            if engine is not None:
                engine.dispose()
            sys.modules.pop(name, None)
            self.loaded.pop(index)
            break


@pytest.fixture
def fees_environment() -> FeesModuleFactory:
    schema = f"fees_int_{uuid.uuid4().hex[:8]}"
    database_url = _with_schema(_FEES_TEST_DSN, schema)

    with psycopg.connect(_FEES_TEST_DSN) as conn:  # type: ignore[arg-type]
        with conn.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            cursor.execute(f'CREATE SCHEMA "{schema}"')
        conn.commit()

    factory = FeesModuleFactory(database_url=database_url)
    original_url = os.environ.get("FEES_DATABASE_URL")
    original_timescale = os.environ.get("TIMESCALE_DSN")

    try:
        yield factory
    finally:
        for name, module in list(factory.loaded):
            session_local = getattr(module, "SessionLocal", None)
            if session_local is not None and hasattr(session_local, "close_all"):
                session_local.close_all()
            engine = getattr(module, "ENGINE", None)
            if engine is not None:
                engine.dispose()
            sys.modules.pop(name, None)
        factory.loaded.clear()

        if original_url is None:
            os.environ.pop("FEES_DATABASE_URL", None)
        else:
            os.environ["FEES_DATABASE_URL"] = original_url

        if original_timescale is None:
            os.environ.pop("TIMESCALE_DSN", None)
        else:
            os.environ["TIMESCALE_DSN"] = original_timescale

        with psycopg.connect(_FEES_TEST_DSN) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            conn.commit()


def test_fee_volume_persists_across_instances(fees_environment: FeesModuleFactory) -> None:
    account_id = f"acct-{uuid.uuid4().hex[:8]}"

    primary = fees_environment.load("fees_service_primary")
    primary._on_startup()  # type: ignore[attr-defined]
    with primary.SessionLocal() as session:
        primary.update_account_volume_30d(
            session,
            account_id=account_id,
            fill_notional_usd=12500.0,
            fill_time=datetime.now(timezone.utc),
            liquidity="maker",
            actual_fee_usd=12.5,
        )

    peer = fees_environment.load("fees_service_peer")
    peer._on_startup()  # type: ignore[attr-defined]
    with peer.SessionLocal() as session:
        record = session.get(peer.AccountVolume30d, account_id)
        assert record is not None
        assert float(record.notional_usd_30d) > 0

    fees_environment.unload("fees_service_primary")
    fees_environment.unload("fees_service_peer")

    restart = fees_environment.load("fees_service_restart")
    restart._on_startup()  # type: ignore[attr-defined]
    with restart.SessionLocal() as session:
        persisted = session.get(restart.AccountVolume30d, account_id)
        assert persisted is not None
        assert float(persisted.notional_usd_30d) > 0

        fills = session.execute(
            select(restart.AccountFill).where(restart.AccountFill.account_id == account_id)
        ).scalars().all()
        assert fills, "Expected fills to persist across service restarts"

    fees_environment.unload("fees_service_restart")
