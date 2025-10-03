import asyncio
import importlib.util
import os
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import List, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit, quote_plus

import pytest

psycopg = pytest.importorskip("psycopg", reason="psycopg is required for HITL integration tests")

pytestmark = pytest.mark.integration

_HITL_TEST_DSN = os.getenv("AETHER_HITL_TEST_DSN") or os.getenv("AETHER_TIMESCALE_TEST_DSN")
if not _HITL_TEST_DSN:
    pytest.skip(
        "AETHER_HITL_TEST_DSN or AETHER_TIMESCALE_TEST_DSN must be set for HITL integration tests",
        allow_module_level=True,
    )


def _with_schema(dsn: str, schema: str) -> str:
    parts = urlsplit(dsn)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["options"] = f"-csearch_path={schema}"
    new_query = urlencode(query, doseq=True, quote_via=quote_plus)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def _load_hitl_module(name: str) -> ModuleType:
    module_path = Path(__file__).resolve().parents[3] / "hitl_service.py"
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load hitl_service module for tests")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


@dataclass
class HitlModuleFactory:
    database_url: str
    loaded: List[Tuple[str, ModuleType]] = field(default_factory=list)

    def load(self, alias: str | None = None) -> ModuleType:
        name = alias or f"hitl_instance_{uuid.uuid4().hex[:8]}"
        if name in sys.modules:
            sys.modules.pop(name, None)
        os.environ["HITL_DATABASE_URL"] = self.database_url
        os.environ["HITL_APPROVAL_TIMEOUT"] = "0"
        os.environ.pop("TIMESCALE_DSN", None)
        module = _load_hitl_module(name)
        self.loaded.append((name, module))
        return module

    def unload(self, alias: str) -> None:
        for index, (name, module) in enumerate(list(self.loaded)):
            if name != alias:
                continue
            engine = getattr(module, "ENGINE", None)
            if engine is not None:
                engine.dispose()
            service = getattr(module, "service", None)
            if service is not None:
                timeout_tasks = getattr(service, "_timeout_tasks", None)
                if isinstance(timeout_tasks, dict):
                    for task in list(timeout_tasks.values()):
                        try:
                            task.cancel()
                        except Exception:
                            pass
                    timeout_tasks.clear()
            sys.modules.pop(name, None)
            self.loaded.pop(index)
            break


@pytest.fixture
def hitl_environment() -> HitlModuleFactory:
    schema = f"hitl_int_{uuid.uuid4().hex[:8]}"
    database_url = _with_schema(_HITL_TEST_DSN, schema)

    with psycopg.connect(_HITL_TEST_DSN) as conn:  # type: ignore[arg-type]
        with conn.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            cursor.execute(f'CREATE SCHEMA "{schema}"')
        conn.commit()

    factory = HitlModuleFactory(database_url=database_url)
    original_url = os.environ.get("HITL_DATABASE_URL")
    original_timeout = os.environ.get("HITL_APPROVAL_TIMEOUT")
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
            os.environ.pop("HITL_DATABASE_URL", None)
        else:
            os.environ["HITL_DATABASE_URL"] = original_url

        if original_timeout is None:
            os.environ.pop("HITL_APPROVAL_TIMEOUT", None)
        else:
            os.environ["HITL_APPROVAL_TIMEOUT"] = original_timeout

        if original_timescale is None:
            os.environ.pop("TIMESCALE_DSN", None)
        else:
            os.environ["TIMESCALE_DSN"] = original_timescale

        with psycopg.connect(_HITL_TEST_DSN) as conn:  # type: ignore[arg-type]
            with conn.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            conn.commit()


def test_pending_approvals_visible_across_instances(hitl_environment: HitlModuleFactory) -> None:
    primary = hitl_environment.load("hitl_instance_primary")

    trade = primary.TradeDetails(
        symbol="AAPL",
        side="buy",
        notional=1_000_000,
        model_confidence=0.4,
        metadata={"source": "integration-test"},
    )
    request = primary.ReviewRequest(account_id="company", intent_id=f"intent-{uuid.uuid4().hex[:8]}", trade_details=trade)

    response = asyncio.run(primary.service.review_trade(request))
    assert response.requires_approval

    pending_primary = primary.service.pending_trades()
    assert any(item.intent_id == request.intent_id for item in pending_primary)

    peer = hitl_environment.load("hitl_instance_peer")
    pending_peer = peer.service.pending_trades()

    assert any(item.intent_id == request.intent_id for item in pending_peer)

    hitl_environment.unload("hitl_instance_primary")
    hitl_environment.unload("hitl_instance_peer")

    restarted = hitl_environment.load("hitl_instance_restart")

    pending_after_restart = restarted.service.pending_trades()
    assert any(item.intent_id == request.intent_id for item in pending_after_restart)

    restarted.service.record_decision(
        restarted.ApprovalRequest(
            intent_id=request.intent_id,
            approved=True,
            reviewer="integration-test",
        )
    )

    hitl_environment.unload("hitl_instance_restart")
