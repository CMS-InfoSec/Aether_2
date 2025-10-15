"""Startup manager should fall back to the SQLAlchemy shim when needed."""

from __future__ import annotations

import asyncio
import builtins
import importlib
import sys
from collections.abc import Callable


def _block_sqlalchemy_imports(monkeypatch) -> None:
    original_import: Callable[..., object] = builtins.__import__

    def _guarded_import(  # type: ignore[override]
        name: str, globals=None, locals=None, fromlist=(), level: int = 0
    ) -> object:
        if name.startswith("sqlalchemy") and name not in sys.modules:
            raise ModuleNotFoundError(name)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)


def test_startup_manager_installs_sqlalchemy_stub(monkeypatch) -> None:
    """The startup manager should reuse the repository SQLAlchemy stub."""

    for module_name in list(sys.modules):
        if module_name.startswith("sqlalchemy"):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    monkeypatch.delitem(sys.modules, "services.core.startup_manager", raising=False)
    monkeypatch.delitem(sys.modules, "services.common.sqlalchemy_stub", raising=False)

    _block_sqlalchemy_imports(monkeypatch)

    module = importlib.import_module("services.core.startup_manager")

    sa_module = getattr(module, "sa", None)
    assert sa_module is not None
    assert getattr(sa_module, "create_engine").__module__ == "services.common.sqlalchemy_stub"

    engine = sa_module.create_engine("sqlite:///:memory:")
    store = module.SQLStartupStateStore(engine)

    assert getattr(store, "_use_memory_only") is True

    async def _exercise() -> None:
        await store.write_state(
            startup_mode="warm",
            last_offset=3,
            offsets={"events:0": 3},
            updated_at=module.datetime(2024, 1, 1, tzinfo=module.timezone.utc),
        )

        state = await store.read_state()
        assert state is not None
        assert state["startup_mode"] == "warm"
        assert state["offsets"] == {"events:0": 3}
        assert await store.has_snapshot() is True

    asyncio.run(_exercise())
