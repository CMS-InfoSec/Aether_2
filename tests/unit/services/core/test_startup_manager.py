import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import pytest

_MODULE_PATH = Path(__file__).resolve().parents[4] / "services" / "core" / "startup_manager.py"
_SPEC = importlib.util.spec_from_file_location("_startup_manager", _MODULE_PATH)
if _SPEC is None or _SPEC.loader is None:  # pragma: no cover - import machinery failure
    raise RuntimeError("Unable to locate startup_manager module for testing")
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

StartupManager = _MODULE.StartupManager
StartupMode = _MODULE.StartupMode


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_cold_start_loads_state_and_persists(tmp_path):
    balance_calls: list[str] = []
    position_calls: list[str] = []
    replay_offsets: list[Dict[str, int]] = []

    async def balance_loader() -> None:
        balance_calls.append("balance")

    async def position_loader() -> None:
        position_calls.append("position")

    async def replay_handler(offsets: Dict[str, int]) -> None:
        replay_offsets.append(dict(offsets))

    offset_file = tmp_path / "offset.log"
    offset_file.write_text(json.dumps({"partitions": {"0": {"offset": 7}}}), encoding="utf-8")
    state_file = tmp_path / "startup_state.json"

    manager = StartupManager(
        balance_loader=balance_loader,
        position_loader=position_loader,
        kafka_replay_handler=replay_handler,
        offset_log_path=offset_file,
        startup_state_path=state_file,
        clock=lambda: datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    mode = await manager.start()

    assert mode is StartupMode.COLD
    assert balance_calls == ["balance"]
    assert position_calls == ["position"]
    assert replay_offsets == [{"offset:0": 7}]

    status = await manager.status()
    assert status == {"mode": "cold", "synced": True, "offset": 7}

    persisted = json.loads(state_file.read_text(encoding="utf-8"))
    assert persisted["mode"] == "cold"
    assert persisted["last_offset"] == 7
    assert persisted["ts"] == datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat()


@pytest.mark.anyio
async def test_warm_restart_replays_and_reconciles(tmp_path):
    state_file = tmp_path / "startup_state.json"
    state_file.write_text(
        json.dumps({"mode": "cold", "last_offset": 2, "ts": "2023-01-01T00:00:00Z"}),
        encoding="utf-8",
    )

    offset_dir = tmp_path / "offsets"
    offset_dir.mkdir()
    (offset_dir / "replay.json").write_text(
        json.dumps({"partitions": {"0": {"offset": 3}, "1": {"offset": 5}}}),
        encoding="utf-8",
    )

    replay_calls: list[Dict[str, int]] = []
    async def replay_handler(offsets: Dict[str, int]) -> None:
        replay_calls.append(dict(offsets))

    reconcile_calls: list[str] = []
    async def reconcile_runner() -> None:
        reconcile_calls.append("ran")

    manager = StartupManager(
        reconcile_runner=reconcile_runner,
        kafka_replay_handler=replay_handler,
        offset_log_path=offset_dir,
        startup_state_path=state_file,
        clock=lambda: datetime(2024, 1, 2, tzinfo=timezone.utc),
    )

    mode = await manager.start()

    assert mode is StartupMode.WARM
    assert reconcile_calls == ["ran"]
    assert replay_calls and replay_calls[0]["replay:1"] == 5

    status = await manager.status()
    assert status["mode"] == "warm"
    assert status["synced"] is True
    assert status["offset"] == 5

    persisted = json.loads(state_file.read_text(encoding="utf-8"))
    assert persisted["mode"] == "warm"
    assert persisted["last_offset"] == 5
    assert persisted["ts"] == datetime(2024, 1, 2, tzinfo=timezone.utc).isoformat()
