from __future__ import annotations

from datetime import datetime, timezone

import importlib

import pytest

try:
    from services.core import backpressure
except ImportError:  # pragma: no cover - fallback for namespace import issues
    backpressure = importlib.import_module("services.core.backpressure")


@pytest.mark.asyncio
async def test_default_publisher_runs_inside_event_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    published: list[tuple[str, dict[str, object]]] = []

    class _DummyAdapter:
        def __init__(self, account_id: str) -> None:  # pragma: no cover - trivial attribute assignment
            self.account_id = account_id

        async def publish(self, topic: str, payload: dict[str, object]) -> None:
            published.append((topic, payload))

    monkeypatch.setattr(backpressure, "KafkaNATSAdapter", _DummyAdapter)

    ts = datetime.now(timezone.utc)

    await backpressure._default_publisher("company", 3, ts)

    assert published == [
        (
            "backpressure.events",
            {
                "account_id": "company",
                "dropped_count": 3,
                "ts": ts.isoformat(),
                "type": "backpressure_event",
            },
        )
    ]
