import asyncio
import importlib
import sys
from datetime import datetime, timezone
from types import ModuleType

from common.utils.tracing import attach_correlation


def test_event_bus_fallback_handles_missing_adapter(monkeypatch):
    dummy_module = ModuleType("services.common.adapters")
    monkeypatch.setitem(sys.modules, "services.common.adapters", dummy_module)
    sys.modules.pop("shared.event_bus", None)

    event_bus = importlib.import_module("shared.event_bus")

    # ``ensure_common_helpers`` may reload the adapters module even after the
    # test inserts a dummy placeholder.  Force the shared event bus wrapper to
    # exercise its in-memory fallback so the assertions remain stable when the
    # real adapter becomes available mid-import.
    monkeypatch.setattr(event_bus, "_delegate_cls", None, raising=False)
    monkeypatch.setattr(event_bus, "_force_fallback", True, raising=False)

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


def test_event_bus_fallback_normalizes_account_ids(monkeypatch):
    dummy_module = ModuleType("services.common.adapters")
    monkeypatch.setitem(sys.modules, "services.common.adapters", dummy_module)
    sys.modules.pop("shared.event_bus", None)

    event_bus = importlib.import_module("shared.event_bus")

    adapter = event_bus.KafkaNATSAdapter(account_id="  MASTER Account  ")
    asyncio.run(adapter.publish("topic", {"value": 1}))

    drained = asyncio.run(event_bus.KafkaNATSAdapter.flush_events())
    assert drained == {"master-account": 1}

    sys.modules.pop("shared.event_bus", None)


def test_event_bus_wrapper_defers_to_delegate(monkeypatch):
    event_bus = importlib.import_module("shared.event_bus")

    class _StubDelegate:
        _history: dict[str, list[dict[str, object]]] = {}
        _reset_called = False
        _shutdown_called = False
        _flush_calls = 0

        def __init__(self, account_id: str, **_: object) -> None:
            self.account_id = account_id

        async def publish(self, topic: str, payload: dict[str, object]) -> None:
            record = attach_correlation(payload)
            entry = {
                "topic": topic,
                "payload": record,
                "timestamp": datetime.now(timezone.utc),
                "correlation_id": record["correlation_id"],
                "delivered": True,
                "partial_delivery": False,
            }
            self._history.setdefault(self.account_id, []).append(entry)

        def history(self, correlation_id: str | None = None) -> list[dict[str, object]]:
            records = list(self._history.get(self.account_id, []))
            if correlation_id is not None:
                return [r for r in records if r["correlation_id"] == correlation_id]
            return records

        @classmethod
        def reset(cls, account_id: str | None = None) -> None:
            cls._reset_called = True
            if account_id is None:
                cls._history.clear()
            else:
                cls._history.pop(account_id, None)

        @classmethod
        async def flush_events(cls) -> dict[str, int]:
            cls._flush_calls += 1
            drained = {acct: len(records) for acct, records in cls._history.items() if records}
            cls._history.clear()
            return drained

        @classmethod
        def shutdown(cls) -> None:
            cls._shutdown_called = True
            cls._history.clear()

    monkeypatch.setattr(event_bus, "_delegate_cls", _StubDelegate, raising=False)
    monkeypatch.setattr(event_bus, "_KafkaNATSAdapter", _StubDelegate, raising=False)

    event_bus.KafkaNATSAdapter.reset()
    adapter = event_bus.KafkaNATSAdapter(account_id="acct-123")
    asyncio.run(adapter.publish("topic", {"value": 2}))

    history = adapter.history()
    assert len(history) == 1
    assert history[0]["payload"]["value"] == 2
    assert "correlation_id" in history[0]["payload"]

    drained = asyncio.run(event_bus.KafkaNATSAdapter.flush_events())
    assert drained == {"acct-123": 1}
    assert _StubDelegate._flush_calls == 1

    adapter = event_bus.KafkaNATSAdapter(account_id="acct-123")
    assert adapter.history() == []

    event_bus.KafkaNATSAdapter.reset()
    assert _StubDelegate._reset_called is True

    event_bus.KafkaNATSAdapter.shutdown()
    assert _StubDelegate._shutdown_called is True


def test_event_bus_wrapper_records_delegate_failures(monkeypatch):
    event_bus = importlib.import_module("shared.event_bus")

    class _BrokenDelegate:
        def __init__(self, account_id: str, **_: object) -> None:
            self.account_id = account_id

        async def publish(self, topic: str, payload: dict[str, object]) -> None:
            raise RuntimeError("boom")

        def history(self, correlation_id: str | None = None) -> list[dict[str, object]]:  # noqa: ARG002
            return []

        @classmethod
        def reset(cls, account_id: str | None = None) -> None:  # noqa: ARG003
            return None

        @classmethod
        async def flush_events(cls) -> dict[str, int]:
            return {}

        @classmethod
        def shutdown(cls) -> None:
            return None

    monkeypatch.setattr(event_bus, "_delegate_cls", _BrokenDelegate, raising=False)
    monkeypatch.setattr(event_bus, "_KafkaNATSAdapter", _BrokenDelegate, raising=False)

    adapter = event_bus.KafkaNATSAdapter(account_id="acct-456")
    asyncio.run(adapter.publish("topic", {"value": 7}))

    history = adapter.history()
    assert len(history) == 1
    assert history[0]["delivered"] is False
    assert history[0]["partial_delivery"] is True

    drained = asyncio.run(event_bus.KafkaNATSAdapter.flush_events())
    assert drained == {"acct-456": 1}


def test_event_bus_wrapper_marks_undelivered_delegate_records(monkeypatch):
    event_bus = importlib.import_module("shared.event_bus")

    class _UndeliveredDelegate:
        _history: dict[str, list[dict[str, object]]] = {}

        def __init__(self, account_id: str, **_: object) -> None:
            self.account_id = account_id

        async def publish(self, topic: str, payload: dict[str, object]) -> None:
            record = attach_correlation(payload)
            entry = {
                "topic": topic,
                "payload": record,
                "timestamp": datetime.now(timezone.utc),
                "correlation_id": record["correlation_id"],
                "delivered": False,
                "partial_delivery": False,
            }
            self._history.setdefault(self.account_id, []).append(entry)

        def history(self, correlation_id: str | None = None) -> list[dict[str, object]]:
            records = list(self._history.get(self.account_id, []))
            if correlation_id is not None:
                return [r for r in records if r["correlation_id"] == correlation_id]
            return records

        @classmethod
        def reset(cls, account_id: str | None = None) -> None:  # noqa: ARG003
            cls._history.clear()

        @classmethod
        async def flush_events(cls) -> dict[str, int]:
            drained = {acct: len(records) for acct, records in cls._history.items() if records}
            cls._history.clear()
            return drained

        @classmethod
        def shutdown(cls) -> None:
            cls._history.clear()

    monkeypatch.setattr(event_bus, "_delegate_cls", _UndeliveredDelegate, raising=False)
    monkeypatch.setattr(event_bus, "_KafkaNATSAdapter", _UndeliveredDelegate, raising=False)

    adapter = event_bus.KafkaNATSAdapter(account_id="acct-789")
    asyncio.run(adapter.publish("topic", {"value": 9}))

    history = adapter.history()
    assert len(history) == 1
    assert history[0]["delivered"] is False
    assert history[0]["partial_delivery"] is False

    drained = asyncio.run(event_bus.KafkaNATSAdapter.flush_events())
    assert drained == {"acct-789": 1}


def test_event_bus_flush_normalizes_delegate_counts(monkeypatch):
    event_bus = importlib.import_module("shared.event_bus")

    class _PartialDelegate:
        _history: dict[str, list[dict[str, object]]] = {}

        def __init__(self, account_id: str, **_: object) -> None:
            self.account_id = account_id

        async def publish(self, topic: str, payload: dict[str, object]) -> None:
            record = attach_correlation(payload)
            entry = {
                "topic": topic,
                "payload": record,
                "timestamp": datetime.now(timezone.utc),
                "correlation_id": record["correlation_id"],
                "delivered": False,
                "partial_delivery": True,
            }
            self._history.setdefault(self.account_id, []).append(entry)

        def history(self, correlation_id: str | None = None) -> list[dict[str, object]]:
            records = list(self._history.get(self.account_id, []))
            if correlation_id is not None:
                return [r for r in records if r["correlation_id"] == correlation_id]
            return records

        @classmethod
        def reset(cls, account_id: str | None = None) -> None:
            if account_id is None:
                cls._history.clear()
            else:
                cls._history.pop(account_id, None)

        @classmethod
        async def flush_events(cls) -> dict[str, int]:
            drained = {acct: len(records) for acct, records in cls._history.items() if records}
            cls._history.clear()
            return drained

        @classmethod
        def shutdown(cls) -> None:
            cls._history.clear()

    monkeypatch.setattr(event_bus, "_delegate_cls", _PartialDelegate, raising=False)
    monkeypatch.setattr(event_bus, "_KafkaNATSAdapter", _PartialDelegate, raising=False)

    event_bus.KafkaNATSAdapter.reset()
    adapter = event_bus.KafkaNATSAdapter(account_id="  MASTER Account  ")
    asyncio.run(adapter.publish("topic", {"value": 11}))

    drained = asyncio.run(event_bus.KafkaNATSAdapter.flush_events())
    assert drained == {"master-account": 1}
    assert "  MASTER Account  " not in drained
