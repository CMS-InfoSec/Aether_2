import datetime as dt
from typing import Any, Dict, List, Mapping, Optional

import pytest

from services.common import adapters as adapters_module
from services.common.adapters import TimescaleAdapter


class _FakeStore:
    clear_calls: List[Optional[str]] = []
    credential_events: List[Dict[str, Any]] = []

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

    @classmethod
    def reset(cls) -> None:
        cls.clear_calls.clear()
        cls.credential_events.clear()


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


def test_reset_rotation_state_normalizes_and_delegates() -> None:
    TimescaleAdapter.reset_rotation_state(account_id=" Example Account ")

    assert _FakeStore.clear_calls == ["example-account"]

    TimescaleAdapter.reset_rotation_state()

    assert _FakeStore.clear_calls == ["example-account", None]


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
