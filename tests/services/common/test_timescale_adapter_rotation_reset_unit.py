from __future__ import annotations

import datetime as dt
import importlib
import importlib.util
import pathlib
import threading
import sys
import types
from typing import Any, Dict, List, Mapping, Optional

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[3]
root_str = str(ROOT)
if root_str in sys.path:
    sys.path.remove(root_str)
sys.path.insert(0, root_str)
importlib.invalidate_caches()

module_name = "services.common.adapters"
services_pkg = sys.modules.get("services")
if services_pkg is None:
    services_pkg = types.ModuleType("services")
    services_pkg.__path__ = [str(ROOT / "services")]
    sys.modules["services"] = services_pkg
else:
    services_pkg.__path__ = [str(ROOT / "services")]

common_pkg = types.ModuleType("services.common")
common_pkg.__path__ = [str(ROOT / "services" / "common")]
sys.modules[module_name.rsplit(".", 1)[0]] = common_pkg

spec = importlib.util.spec_from_file_location(module_name, ROOT / "services" / "common" / "adapters.py")
if spec is None or spec.loader is None:
    raise RuntimeError("Unable to load services.common.adapters module for testing")
adapters_module = importlib.util.module_from_spec(spec)
sys.modules[module_name] = adapters_module
spec.loader.exec_module(adapters_module)
TimescaleAdapter = adapters_module.TimescaleAdapter


class _FakeStore:
    _lock = threading.RLock()
    _connections: Dict[str, object] = {}
    clear_calls: List[Optional[str]] = []
    credential_events: List[Dict[str, Any]] = []
    rotation_rows: Dict[str, List[str]] = {}

    def __init__(self, account_id: str, **_: Any) -> None:
        self.account_id = account_id

    def record_credential_event(
        self,
        *,
        event: str,
        event_type: str,
        secret_name: Optional[str],
        metadata: Mapping[str, Any],
        recorded_at: dt.datetime,
    ) -> None:
        _FakeStore.credential_events.append(
            {
                "event": event,
                "event_type": event_type,
                "secret_name": secret_name,
                "metadata": dict(metadata),
                "recorded_at": recorded_at,
            }
        )

    @classmethod
    def fetch_credential_events(cls) -> List[Dict[str, Any]]:
        return [dict(event) for event in cls.credential_events]

    @classmethod
    def clear_all_rotation_state(cls, account_id: Optional[str] = None) -> None:
        cls.clear_calls.append(account_id)
        if account_id is None:
            cls.rotation_rows.clear()
            return
        cls.rotation_rows.pop(account_id, None)

    @classmethod
    def reset_account(cls, account_id: str) -> None:
        cls.clear_calls.append(f"reset:{account_id}")
        cls._connections.pop(account_id, None)
        cls.rotation_rows.pop(account_id, None)

    @classmethod
    def reset(cls) -> None:
        cls._connections.clear()
        cls.clear_calls.clear()
        cls.credential_events.clear()
        cls.rotation_rows.clear()


@pytest.fixture(autouse=True)
def _patch_store(monkeypatch: pytest.MonkeyPatch) -> None:
    original_store = adapters_module._TimescaleStore
    _FakeStore.reset()
    monkeypatch.setattr(adapters_module, "_TimescaleStore", _FakeStore)
    yield
    _FakeStore.reset()
    monkeypatch.setattr(adapters_module, "_TimescaleStore", original_store)


@pytest.fixture(autouse=True)
def _clear_adapter_caches() -> None:
    caches = (
        TimescaleAdapter._metrics,
        TimescaleAdapter._kill_events,
        TimescaleAdapter._daily_usage,
        TimescaleAdapter._instrument_exposures,
        TimescaleAdapter._rolling_volume,
        TimescaleAdapter._cvar_results,
        TimescaleAdapter._nav_forecasts,
    )
    for cache in caches:
        cache.clear()
    yield
    for cache in caches:
        cache.clear()


def test_reset_rotation_state_handles_mixed_case_account_id() -> None:
    store_account_id = "Example-Account"
    normalized = "example-account"
    with _FakeStore._lock:
        _FakeStore._connections = {store_account_id: object()}
    _FakeStore.rotation_rows = {store_account_id: ["row-1"]}
    TimescaleAdapter._metrics[normalized] = {"limit": 1.0, "usage": 0.0}

    TimescaleAdapter.reset_rotation_state(account_id=" Example Account ")

    assert _FakeStore.clear_calls == [store_account_id]
    assert store_account_id not in _FakeStore.rotation_rows
    assert normalized in TimescaleAdapter._metrics

    _FakeStore.rotation_rows = {
        store_account_id: ["row-2"],
        "another": ["row-3"],
    }

    TimescaleAdapter.reset_rotation_state()

    assert _FakeStore.clear_calls == [store_account_id, None]
    assert _FakeStore.rotation_rows == {}


def test_record_credential_access_masks_sensitive_values() -> None:
    adapter = TimescaleAdapter(account_id="Example Account")

    adapter.record_credential_access(
        secret_name="kraken/api",
        metadata={"api_key": "keyvalue", "api_secret": "secretvalue"},
    )

    assert len(_FakeStore.credential_events) == 1
    event = _FakeStore.credential_events[0]
    assert event["event"] == "access"
    assert event["event_type"] == "kraken.credentials.access"
    assert event["secret_name"] == "kraken/api"
    metadata = event["metadata"]
    assert metadata["api_key"] == "***"
    assert metadata["api_secret"] == "***"
    assert metadata["material_present"] is True
