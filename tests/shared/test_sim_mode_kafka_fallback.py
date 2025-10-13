import asyncio
import importlib
import sys
from types import ModuleType


def test_in_memory_kafka_adapter_enriches_events(monkeypatch):
    """Fallback Kafka adapter should mirror correlation semantics from production."""

    dummy_module = ModuleType("services.common.adapters")
    monkeypatch.setitem(sys.modules, "services.common.adapters", dummy_module)
    sys.modules.pop("shared.sim_mode", None)

    sim_mode = importlib.import_module("shared.sim_mode")

    adapter = sim_mode.KafkaNATSAdapter(account_id="acct-123")
    payload = {"foo": "bar"}

    asyncio.run(adapter.publish("topic", dict(payload)))

    history = adapter.history()
    assert len(history) == 1

    record = history[0]
    assert record["delivered"] is True
    assert record["partial_delivery"] is False
    assert record["payload"] != payload
    assert payload == {"foo": "bar"}

    correlation_id = record["payload"].get("correlation_id")
    assert correlation_id
    assert record["correlation_id"] == correlation_id
    assert adapter.history(correlation_id) == [record]

    drained = asyncio.run(sim_mode.KafkaNATSAdapter.flush_events())
    assert drained == {"acct-123": 1}
    assert adapter.history() == []

    sys.modules.pop("shared.sim_mode", None)
