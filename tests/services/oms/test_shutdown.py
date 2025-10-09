from __future__ import annotations

import asyncio
import importlib
import sys
import types
from datetime import datetime
from pathlib import Path
from typing import Dict

import pytest
from httpx import ASGITransport, AsyncClient

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.mark.asyncio
async def test_drain_endpoint_flushes_kafka_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIM_MODE_DATABASE_URL", "postgresql://test:test@localhost/test")

    test_dir = str(Path(__file__).resolve().parent)
    tests_root = str(ROOT / "tests")
    original_sys_path = list(sys.path)
    original_sim_mode = sys.modules.get("shared.sim_mode")
    try:
        sys.path = [p for p in sys.path if p not in {test_dir, tests_root}]
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))

        sys.modules.pop("services", None)
        sys.modules.pop("services.common", None)
        sys.modules.pop("services.oms", None)
        sys.modules.pop("services.oms.kraken_rest", None)
        sys.modules.pop("services.oms.kraken_ws", None)

        async def _noop_async(*args, **kwargs):
            return None

        sim_mode_stub = types.ModuleType("shared.sim_mode")

        class _Snapshot:
            pass

        sim_mode_stub.SimulatedOrderSnapshot = _Snapshot
        sim_mode_stub.sim_broker = types.SimpleNamespace(
            lookup=_noop_async,
            cancel_order=_noop_async,
            place_order=_noop_async,
        )
        sim_mode_stub.sim_mode_broker = types.SimpleNamespace(
            lookup=_noop_async,
            cancel_order=_noop_async,
            place_order=_noop_async,
        )
        async def _sim_status(account_id: str, *, use_cache: bool = True):  # type: ignore[override]
            del account_id, use_cache
            return types.SimpleNamespace(account_id="stub", active=False, reason=None, ts=datetime.now())

        sim_mode_stub.sim_mode_repository = types.SimpleNamespace(
            get_status_async=_sim_status,
        )

        sim_mode_stub.sim_mode_state = types.SimpleNamespace(
            is_active=lambda account_id=None: False,
            activate=lambda account_id=None: None,
            deactivate=lambda account_id=None: None,
            enable=lambda account_id=None: None,
            disable=lambda account_id=None: None,
        )

        sys.modules["shared.sim_mode"] = sim_mode_stub

        importlib.import_module("services")
        importlib.import_module("services.common")
        oms_service = importlib.import_module("oms_service")
    finally:
        sys.path = original_sys_path
        if original_sim_mode is not None:
            sys.modules["shared.sim_mode"] = original_sim_mode
        else:
            sys.modules.pop("shared.sim_mode", None)

    calls = 0

    async def _flush_events(cls) -> Dict[str, int]:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return {"dummy": calls}

    monkeypatch.setattr(
        oms_service.KafkaNATSAdapter,
        "flush_events",
        classmethod(_flush_events),
    )

    # Ensure a clean state before the test begins.
    oms_service.shutdown_manager.reset()

    transport = ASGITransport(app=oms_service.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/ops/drain/start")

    assert response.status_code == 202
    assert calls == 1

    # Reset draining state so other tests are unaffected.
    oms_service.shutdown_manager.reset()
