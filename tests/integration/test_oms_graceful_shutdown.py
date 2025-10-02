import asyncio
import importlib
import os
import signal
import threading
import time
from typing import Any, Dict

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


@pytest.mark.integration
def test_sigterm_drains_inflight_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify SIGTERM waits for in-flight OMS requests before terminating."""

    shutdown_timeout = 0.75
    monkeypatch.setenv("OMS_SHUTDOWN_TIMEOUT", str(shutdown_timeout))

    # Reload the module so the configured shutdown timeout is applied.
    oms_main = importlib.reload(importlib.import_module("services.oms.main"))

    from services.common.adapters import KafkaNATSAdapter, TimescaleAdapter

    KafkaNATSAdapter.reset()
    TimescaleAdapter.flush_event_buffers()
    oms_main.shadow_oms.reset()

    flush_counts: Dict[str, int] = {"kafka": 0, "timescale": 0}

    original_kafka_flush = KafkaNATSAdapter.flush_events.__func__  # type: ignore[attr-defined]

    def _tracked_kafka_flush(cls: type[KafkaNATSAdapter]) -> Dict[str, int]:
        flush_counts["kafka"] += 1
        return original_kafka_flush(cls)

    monkeypatch.setattr(
        KafkaNATSAdapter,
        "flush_events",
        classmethod(_tracked_kafka_flush),
    )

    original_timescale_flush = TimescaleAdapter.flush_event_buffers.__func__  # type: ignore[attr-defined]

    def _tracked_timescale_flush(
        cls: type[TimescaleAdapter],
    ) -> Dict[str, Dict[str, int]]:
        flush_counts["timescale"] += 1
        return original_timescale_flush(cls)

    monkeypatch.setattr(
        TimescaleAdapter,
        "flush_event_buffers",
        classmethod(_tracked_timescale_flush),
    )

    def _permit_account(*args: Any, **kwargs: Any) -> str:
        return "company"

    monkeypatch.setattr(oms_main, "require_admin_account", _permit_account)

    guard_waits: list[float | None] = []

    async def _fake_wait_for_idle(timeout: float | None = None) -> bool:
        guard_waits.append(timeout)
        await asyncio.sleep(0)
        return True

    monkeypatch.setattr(oms_main.rate_limit_guard, "wait_for_idle", _fake_wait_for_idle)

    import shared.graceful_shutdown as graceful_shutdown

    exit_codes: list[int] = []

    def _fake_exit(code: int = 0) -> None:
        exit_codes.append(code)

    monkeypatch.setattr(graceful_shutdown.sys, "exit", _fake_exit)

    class _SlowKrakenClient:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        def add_order(self, payload: Dict[str, Any], timeout: float = 1.0) -> Dict[str, Any]:
            time.sleep(0.35)
            return {"status": "ok", "txid": "SIM-123"}

        def open_orders(self) -> Dict[str, Any]:
            return {"open": []}

        def own_trades(self, txid: str | None = None) -> Dict[str, Any]:
            return {"trades": []}

        def close(self) -> None:
            return None

    monkeypatch.setattr(oms_main, "KrakenWSClient", _SlowKrakenClient)

    request_payload = {
        "order_id": "OID-123",
        "account_id": "company",
        "instrument": "BTC-USD",
        "side": "BUY",
        "quantity": 1.0,
        "price": 30000.0,
        "fee": 12.5,
        "post_only": False,
        "reduce_only": False,
    }

    responses: Dict[str, Any] = {}

    with TestClient(oms_main.app) as client:

        def _post_order() -> None:
            responses["response"] = client.post(
                "/oms/place",
                json=request_payload,
                headers={"X-Account-ID": "company"},
            )

        worker = threading.Thread(target=_post_order)
        worker.start()

        time.sleep(0.05)
        start = time.perf_counter()
        os.kill(os.getpid(), signal.SIGTERM)

        worker.join(timeout=5.0)
        elapsed = time.perf_counter() - start

        assert not worker.is_alive(), "Request thread did not finish"
        response = responses.get("response")
        assert response is not None, "Expected OMS response"
        assert response.status_code == 200
        assert response.json().get("accepted") is True
        assert elapsed < shutdown_timeout + 0.5

    assert guard_waits, "Rate limit guard should be awaited during shutdown"
    assert flush_counts["kafka"] >= 1
    assert flush_counts["timescale"] >= 1
    assert exit_codes, "SIGTERM handler should trigger sys.exit"

    kafka_history = KafkaNATSAdapter(account_id="company").history()
    assert kafka_history == []

    account_events = TimescaleAdapter(account_id="company").events()
    assert all(not bucket for bucket in account_events.values())
