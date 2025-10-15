from __future__ import annotations

import builtins
import importlib
import sys
from decimal import Decimal
from typing import Callable


def _block_sqlalchemy_imports(monkeypatch) -> None:
    original_import: Callable[..., object] = builtins.__import__

    def _guarded_import(  # type: ignore[override]
        name: str, globals=None, locals=None, fromlist=(), level: int = 0
    ) -> object:
        if name.startswith("sqlalchemy") and name not in sys.modules:
            raise ModuleNotFoundError(name)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)


def test_sim_mode_fallback_without_sqlalchemy(monkeypatch) -> None:
    """Module should expose fully functional fallbacks when SQLAlchemy is missing."""

    for module_name in list(sys.modules):
        if module_name.startswith("sqlalchemy"):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    monkeypatch.delitem(sys.modules, "shared.sim_mode", raising=False)
    monkeypatch.delitem(sys.modules, "shared._sim_mode_sqlalchemy", raising=False)
    monkeypatch.delitem(sys.modules, "shared.event_bus", raising=False)

    _block_sqlalchemy_imports(monkeypatch)

    sim_mode = importlib.import_module("shared.sim_mode")

    status = sim_mode.sim_mode_repository.get_status("acct-001")
    assert status.account_id == "acct-001"
    assert status.active is False

    updated = sim_mode.sim_mode_repository.set_status("acct-001", True, "maintenance")
    assert updated.active is True
    assert sim_mode.sim_mode_repository.get_status("acct-001").active is True

    recorded: list[tuple[str, dict]] = []

    async def _fake_publish(self, topic: str, payload: dict) -> dict:
        recorded.append((topic, payload))
        return {"topic": topic, "payload": payload}

    monkeypatch.setattr(sim_mode.KafkaNATSAdapter, "publish", _fake_publish, raising=False)

    execution = sim_mode.sim_broker.place_order(
        account_id="acct-001",
        client_id="client-1",
        symbol="BTC-USD",
        side="buy",
        order_type="market",
        qty=Decimal("1"),
        limit_px=None,
        pre_trade_mid=Decimal("25000"),
    )
    assert execution.fill_qty > 0
    assert recorded, "fallback broker should publish simulated fills"

    lookup = sim_mode.sim_broker.lookup("acct-001", "client-1")
    assert lookup is not None
    assert lookup.status in {"filled", "partially_filled"}

    cancelled = sim_mode.sim_broker.cancel_order("acct-001", "client-1")
    assert cancelled is not None
    assert cancelled.status == "cancelled"

