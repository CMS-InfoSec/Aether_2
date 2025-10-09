"""Integration tests for persisted simulation mode state."""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool


@pytest.fixture
def sim_mode_loader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Provide a loader that (re)imports ``shared.sim_mode`` against a shared DB."""

    import sqlalchemy

    sqlite_url = f"sqlite:///{tmp_path / 'sim_mode.db'}"
    original_create_engine = create_engine

    try:  # pragma: no cover - real adapters may not be available in CI
        importlib.import_module("services.common.adapters")
    except Exception:
        adapters_stub = types.ModuleType("services.common.adapters")

        class _KafkaNATSAdapter:  # pragma: no cover - simple stub for tests
            def __init__(self, *args, **kwargs) -> None:
                del args, kwargs

            def publish(self, *args, **kwargs) -> None:
                del args, kwargs

        adapters_stub.KafkaNATSAdapter = _KafkaNATSAdapter  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "services.common.adapters", adapters_stub)
        if "services.common" not in sys.modules:
            services_common = types.ModuleType("services.common")
            services_common.adapters = adapters_stub  # type: ignore[attr-defined]
            monkeypatch.setitem(sys.modules, "services.common", services_common)

    def fake_create_engine(url: str, *args, **kwargs):  # type: ignore[override]
        del url
        real_kwargs = dict(kwargs)
        connect_args = dict(real_kwargs.pop("connect_args", {}))
        connect_args.pop("sslmode", None)
        connect_args["check_same_thread"] = False
        real_kwargs["connect_args"] = connect_args
        real_kwargs["poolclass"] = StaticPool
        real_kwargs["future"] = True
        real_kwargs["pool_pre_ping"] = True
        real_kwargs.pop("pool_size", None)
        real_kwargs.pop("max_overflow", None)
        real_kwargs.pop("pool_timeout", None)
        real_kwargs.pop("pool_recycle", None)
        return original_create_engine(sqlite_url, **real_kwargs)

    monkeypatch.setenv("SIM_MODE_DATABASE_URL", "postgresql+psycopg://tester:secret@db.example/sim")
    monkeypatch.setattr(sqlalchemy, "create_engine", fake_create_engine, raising=True)

    def _load():
        sys.modules.pop("shared.sim_mode", None)
        return importlib.import_module("shared.sim_mode")

    return _load


@pytest.mark.integration
@pytest.mark.smoke
def test_sim_mode_state_persists_across_restart(sim_mode_loader):
    module = sim_mode_loader()
    status = module.sim_mode_repository.set_status("company", True, "integration test")
    assert status.active is True

    module_after_restart = sim_mode_loader()
    refreshed = module_after_restart.sim_mode_repository.get_status("company", use_cache=False)
    assert refreshed.active is True
    assert refreshed.reason == "integration test"


@pytest.mark.integration
@pytest.mark.smoke
def test_sim_mode_state_shared_across_replicas(sim_mode_loader):
    module = sim_mode_loader()

    replica_a = module.SimModeRepository()
    replica_b = module.SimModeRepository()

    replica_a.set_status("company", True, "replica-a")
    b_status = replica_b.get_status("company", use_cache=False)
    assert b_status.active is True
    assert b_status.reason == "replica-a"

    replica_b.set_status("company", False, None)
    a_status = replica_a.get_status("company", use_cache=False)
    assert a_status.active is False
    assert a_status.reason is None
