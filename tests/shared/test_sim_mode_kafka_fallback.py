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


def test_in_memory_kafka_reset_normalises_account_ids(monkeypatch):
    dummy_module = ModuleType("services.common.adapters")
    monkeypatch.setitem(sys.modules, "services.common.adapters", dummy_module)
    sys.modules.pop("shared.sim_mode", None)

    sim_mode = importlib.import_module("shared.sim_mode")

    adapter = sim_mode.KafkaNATSAdapter(account_id="  acct-456  ")
    asyncio.run(adapter.publish("topic", {"payload": "value"}))
    assert adapter.history()

    sim_mode.KafkaNATSAdapter.reset("  acct-456  ")
    assert adapter.history() == []

    sys.modules.pop("shared.sim_mode", None)


def test_sim_mode_service_imports_without_adapters(monkeypatch):
    """Service should fall back to the shared in-memory Kafka adapter when needed."""

    dummy_module = ModuleType("services.common.adapters")
    monkeypatch.setitem(sys.modules, "services.common.adapters", dummy_module)
    sys.modules.pop("shared.sim_mode", None)
    sys.modules.pop("sim_mode", None)

    shared_module = importlib.import_module("shared.sim_mode")
    service_module = importlib.import_module("sim_mode")

    assert service_module.KafkaNATSAdapter is shared_module.KafkaNATSAdapter

    adapter = service_module.KafkaNATSAdapter(account_id="acct-789")
    asyncio.run(adapter.publish("topic", {"payload": "value"}))
    assert adapter.history()

    sys.modules.pop("sim_mode", None)
    sys.modules.pop("shared.sim_mode", None)
