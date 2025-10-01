"""Flash crash scenario regression test."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Iterable, Tuple

import pytest

pytest.importorskip("fastapi")

import safe_mode
from shared.audit import AuditLogStore, SensitiveActionRecorder, TimescaleAuditLogger


PriceObservation = Tuple[datetime, float]


def _detect_flash_crash(
    observations: Iterable[PriceObservation],
    *,
    drop_threshold: float = -0.2,
    max_duration: timedelta = timedelta(minutes=5),
) -> bool:
    """Return ``True`` when observations capture a sharp price crash."""

    ordered = sorted(observations, key=lambda item: item[0])
    if len(ordered) < 2:
        return False

    start_time, start_price = ordered[0]
    end_time, end_price = ordered[-1]

    if start_price <= 0:
        return False

    pct_move = (end_price - start_price) / start_price
    duration = end_time - start_time
    return pct_move <= drop_threshold and duration <= max_duration


def test_flash_crash_triggers_safe_mode_and_audit(
    monkeypatch: pytest.MonkeyPatch, kraken_mock_server
) -> None:
    """Validate the system reaction to a flash crash style move."""

    controller = safe_mode.controller
    controller.reset()
    safe_mode.clear_safe_mode_log()

    audit_store = AuditLogStore()
    recorder = SensitiveActionRecorder(TimescaleAuditLogger(audit_store))

    original_safe_mode_log = safe_mode.safe_mode_log
    state_tracker = {"active": False}

    def patched_safe_mode_log(reason: str, ts: datetime, actor: str | None, state: str) -> None:
        original_safe_mode_log(reason, ts, actor, state)
        recorder.record(
            action="safe_mode.transition",
            actor_id=actor or "system",
            before={"active": state_tracker["active"]},
            after={"active": state == "entered", "reason": reason},
        )
        state_tracker["active"] = state == "entered"

    monkeypatch.setattr(safe_mode, "safe_mode_log", patched_safe_mode_log)

    try:
        initial_price = kraken_mock_server.config.base_price
        start_time = datetime.now(timezone.utc)

        response = asyncio.run(
            kraken_mock_server.add_order(
                pair="BTC/USD",
                side="sell",
                volume=1.5,
                price=initial_price * 1.05,
                ordertype="limit",
                account="company",
                userref="risk-exposure",
            )
        )
        order_payload = response["order"]
        assert order_payload["status"] == "open"
        open_order_id = order_payload["order_id"]
        controller.order_controls.open_orders.append(open_order_id)

        crash_time = start_time + timedelta(minutes=4, seconds=30)
        kraken_mock_server.config.base_price = initial_price * 0.8
        crash_price = kraken_mock_server.config.base_price

        observations = [(start_time, initial_price), (crash_time, crash_price)]
        assert _detect_flash_crash(observations)

        cancel_result = asyncio.run(
            kraken_mock_server.cancel_order(open_order_id, account="company")
        )
        assert cancel_result["status"] == "cancelled"

        event = controller.enter(reason="flash_crash", actor="risk-engine")
        assert event.reason == "flash_crash"

        assert open_order_id in controller.order_controls.cancelled_orders
        assert controller.order_controls.open_orders == []
        assert controller.order_controls.hedging_only is True

        assert controller.intent_guard.allow_new_intents is False
        with pytest.raises(RuntimeError):
            controller.guard_new_intent()

        status = controller.status()
        assert status.active is True
        assert status.reason == "flash_crash"

        async def attempt_speculative_trade() -> None:
            controller.guard_new_intent()
            await kraken_mock_server.add_order(
                pair="BTC/USD",
                side="buy",
                volume=0.1,
                price=kraken_mock_server.config.base_price,
                ordertype="limit",
                account="company",
                userref="speculative",
            )

        with pytest.raises(RuntimeError):
            asyncio.run(attempt_speculative_trade())

        entries = list(audit_store.all())
        assert entries, "Expected flash crash to be written to the audit log"
        last_entry = entries[-1]
        assert dict(last_entry.after)["reason"] == "flash_crash"
    finally:
        controller.reset()
        safe_mode.clear_safe_mode_log()
        setattr(safe_mode, "safe_mode_log", original_safe_mode_log)
