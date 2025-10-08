from __future__ import annotations

import asyncio
import importlib.util
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import sqlalchemy as sa

if "fastapi" not in sys.modules:  # pragma: no cover - lightweight stub for unit tests
    fastapi_stub = types.ModuleType("fastapi")

    class _HTTPException(Exception):
        def __init__(self, status_code: int, detail: str | None = None) -> None:
            super().__init__(detail or "")
            self.status_code = status_code
            self.detail = detail

    class _APIRouter:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.routes = []

        def get(self, *args: object, **kwargs: object):
            def decorator(func):
                self.routes.append(func)
                return func

            return decorator

    class _FastAPI:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.state = SimpleNamespace()

        def include_router(self, router: _APIRouter) -> None:  # type: ignore[name-defined]
            self.router = router

    fastapi_stub.FastAPI = _FastAPI  # type: ignore[attr-defined]
    fastapi_stub.APIRouter = _APIRouter  # type: ignore[attr-defined]
    fastapi_stub.HTTPException = _HTTPException  # type: ignore[attr-defined]
    sys.modules["fastapi"] = fastapi_stub

_MODULE_PATH = Path(__file__).resolve().parents[4] / "services" / "core" / "startup_manager.py"
_SPEC = importlib.util.spec_from_file_location("_startup_manager", _MODULE_PATH)
if _SPEC is None or _SPEC.loader is None:  # pragma: no cover - import machinery failure
    raise RuntimeError("Unable to locate startup_manager module for testing")
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

StartupManager = _MODULE.StartupManager
StartupMode = _MODULE.StartupMode
SQLStartupStateStore = _MODULE.SQLStartupStateStore


def _create_engine() -> sa.engine.Engine:
    return sa.create_engine("sqlite:///:memory:", future=True)


def test_cold_start_bootstraps_exchange_state_and_persists() -> None:
    asyncio.run(_exercise_cold_start())


async def _exercise_cold_start() -> None:
    engine = _create_engine()
    store = SQLStartupStateStore(engine)

    balance_calls: list[str] = []
    order_calls: list[str] = []
    position_calls: list[str] = []
    replay_payloads: list[Mapping[str, int]] = []
    reconcile_calls: list[str] = []

    async def balance_loader() -> None:
        balance_calls.append("balances")

    async def order_loader() -> None:
        order_calls.append("orders")

    async def position_loader() -> None:
        position_calls.append("positions")

    async def replay_handler(offsets: Mapping[str, int]) -> Mapping[str, int]:
        replay_payloads.append(dict(offsets))
        return {"events:0": 5}

    async def reconcile_runner() -> None:
        reconcile_calls.append("reconcile")

    manager = StartupManager(
        balance_loader=balance_loader,
        order_loader=order_loader,
        position_loader=position_loader,
        kafka_replay_handler=replay_handler,
        reconcile_runner=reconcile_runner,
        state_store=store,
        snapshot_probe=lambda: False,
        clock=lambda: datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    mode = await manager.start()

    assert mode is StartupMode.COLD
    assert balance_calls == ["balances"]
    assert order_calls == ["orders"]
    assert position_calls == ["positions"]
    assert replay_payloads == [{}]
    assert reconcile_calls == ["reconcile"]

    status = await manager.status()
    assert status["mode"] == "cold"
    assert status["synced"] is True
    assert status["offsets"]["last_offset"] == 5
    assert status["offsets"]["sources"] == {"events:0": 5}
    assert status["dedupe"]["orders"]["total"] == 0
    assert status["dedupe"]["fills"]["total"] == 0

    persisted = await store.read_state()
    assert persisted["startup_mode"] == "cold"
    assert persisted["last_offset"] == 5
    assert persisted["offsets"] == {"events:0": 5}


def test_warm_restart_replays_from_offsets_and_reconciles() -> None:
    asyncio.run(_exercise_warm_restart())


async def _exercise_warm_restart() -> None:
    engine = _create_engine()
    store = SQLStartupStateStore(engine)

    await store.write_state(
        startup_mode="cold",
        last_offset=3,
        offsets={"events:0": 3, "events:1": 4},
        updated_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    replay_payloads: list[Mapping[str, int]] = []
    reconcile_calls: list[str] = []

    async def replay_handler(offsets: Mapping[str, int]) -> Mapping[str, int]:
        replay_payloads.append(dict(offsets))
        return {"events:0": 4, "events:1": 6}

    async def reconcile_runner() -> None:
        reconcile_calls.append("ran")

    manager = StartupManager(
        reconcile_runner=reconcile_runner,
        kafka_replay_handler=replay_handler,
        state_store=store,
        snapshot_probe=lambda: True,
        clock=lambda: datetime(2024, 1, 2, tzinfo=timezone.utc),
    )

    mode = await manager.start()

    assert mode is StartupMode.WARM
    assert replay_payloads and replay_payloads[0]["events:1"] == 4
    assert reconcile_calls == ["ran"]

    status = await manager.status()
    assert status["mode"] == "warm"
    assert status["synced"] is True
    assert status["offsets"]["last_offset"] == 6
    assert status["offsets"]["sources"] == {"events:0": 4, "events:1": 6}
    assert status["dedupe"]["orders"]["total"] == 0
    assert status["dedupe"]["fills"]["total"] == 0

    persisted = await store.read_state()
    assert persisted["startup_mode"] == "warm"
    assert persisted["last_offset"] == 6
    assert persisted["offsets"] == {"events:0": 4, "events:1": 6}


def test_duplicate_protection_tracks_unique_orders_and_fills() -> None:
    asyncio.run(_exercise_duplicate_protection())


async def _exercise_duplicate_protection() -> None:
    balance_calls: list[str] = []

    async def balance_loader() -> None:
        balance_calls.append("balances")

    async def order_loader() -> list[dict[str, str]]:
        return [
            {"order_id": "A"},
            {"order_id": "A"},
            {"order_id": "B"},
        ]

    async def replay_handler(offsets: Mapping[str, int]) -> Mapping[str, Any]:
        base = dict(offsets)
        base.setdefault("events:0", 0)
        return {
            "offsets": {"events:0": base["events:0"] + 1},
            "orders": [
                {"order_id": "B"},
                {"order_id": "C"},
            ],
            "fills": [
                {"fill_id": "F1"},
                {"fill_id": "F1"},
                {"fill_id": "F2"},
            ],
        }

    async def reconcile_runner() -> None:
        return None

    manager = StartupManager(
        balance_loader=balance_loader,
        order_loader=order_loader,
        reconcile_runner=reconcile_runner,
        kafka_replay_handler=replay_handler,
    )

    mode = await manager.start()
    assert mode is StartupMode.COLD
    assert balance_calls == ["balances"]

    status = await manager.status()
    assert status["dedupe"]["orders"]["total"] == 3
    assert status["dedupe"]["fills"]["total"] == 2
    assert status["dedupe"]["orders"]["sources"]["bootstrap"] == 2
    assert status["dedupe"]["orders"]["sources"]["replay"] == 1
    assert status["dedupe"]["fills"]["sources"]["replay"] == 2


def test_startup_status_endpoint_reports_manager_state() -> None:
    asyncio.run(_exercise_status_endpoint())


async def _exercise_status_endpoint() -> None:
    manager = StartupManager()
    await manager.start(StartupMode.COLD)

    app = _MODULE.FastAPI()  # type: ignore[attr-defined]
    _MODULE.register(app, manager)

    try:
        response = await _MODULE.startup_status()
    finally:
        setattr(_MODULE, "_MANAGER", None)

    assert response["mode"] == "cold"
    assert response["synced"] is True
