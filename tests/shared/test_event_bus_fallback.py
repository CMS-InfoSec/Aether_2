import asyncio
import importlib
import sys
from types import ModuleType


def test_event_bus_fallback_handles_missing_adapter(monkeypatch):
    dummy_module = ModuleType("services.common.adapters")
    monkeypatch.setitem(sys.modules, "services.common.adapters", dummy_module)
    sys.modules.pop("shared.event_bus", None)

    event_bus = importlib.import_module("shared.event_bus")

    adapter = event_bus.KafkaNATSAdapter(account_id=" test ")
    asyncio.run(adapter.publish("topic", {"value": 1}))

    history = adapter.history()
    assert history
    assert history[0]["payload"]["value"] == 1
    assert history[0]["delivered"] is True
    assert history[0]["partial_delivery"] is False

    drained = asyncio.run(event_bus.KafkaNATSAdapter.flush_events())
    assert drained == {"test": 1}
    assert adapter.history() == []

    sys.modules.pop("shared.event_bus", None)
