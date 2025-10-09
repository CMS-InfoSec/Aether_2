from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy")


def _reload_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path/'kraken.sqlite'}")
    monkeypatch.setenv("KRAKEN_WS_ALLOW_SQLITE_FOR_TESTS", "1")
    monkeypatch.setenv("KRAKEN_WS_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("KRAKEN_WS_STATE_DIR", str(tmp_path / "state"))
    sys.modules.pop("data.ingest.kraken_ws", None)
    return importlib.import_module("data.ingest.kraken_ws")


def test_require_aiohttp_allows_insecure_defaults(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _reload_module(monkeypatch, tmp_path)
    module.aiohttp = None  # type: ignore[assignment]
    module._AIOHTTP_IMPORT_ERROR = ImportError("aiohttp missing")  # type: ignore[attr-defined]

    module._require_aiohttp()


def test_local_snapshot_roundtrip_generates_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _reload_module(monkeypatch, tmp_path)
    state_file = module._state_file_for_symbol("BTC/USD")  # type: ignore[attr-defined]
    assert not state_file.exists()

    payload = module._load_local_depth_snapshot("BTC/USD")  # type: ignore[attr-defined]
    assert payload["b"] and payload["a"]
    assert state_file.exists()

    next_payload = module._load_local_depth_snapshot("BTC/USD")  # type: ignore[attr-defined]
    assert next_payload["sequence"] == payload["sequence"] + 1


@pytest.mark.asyncio
async def test_consume_local_snapshots_persists_updates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _reload_module(monkeypatch, tmp_path)
    module.aiohttp = None  # type: ignore[assignment]
    module._AIOHTTP_IMPORT_ERROR = ImportError("aiohttp missing")  # type: ignore[attr-defined]
    module._POLL_INTERVAL_SECONDS = 0.0  # type: ignore[attr-defined]

    emitted = []
    module.publish_updates = lambda producer, updates: emitted.extend(updates)  # type: ignore[attr-defined]

    async def _no_nats(updates):  # type: ignore[no-untyped-def]
        emitted.extend(updates)

    module.publish_to_nats = _no_nats  # type: ignore[attr-defined]

    if module._SQLALCHEMY_AVAILABLE and module.metadata is not None and module.orderbook_events_table is not None:  # type: ignore[attr-defined]
        engine = module.create_engine(module.DATABASE_URL, future=True)
    else:
        engine = None

    await module._consume_via_local_snapshots(engine, None, ["BTC/USD"], iterations=1)  # type: ignore[attr-defined]

    assert emitted, "fallback ingestion should produce orderbook events"

    if not module._SQLALCHEMY_AVAILABLE:  # type: ignore[attr-defined]
        state_dir = module._state_root()  # type: ignore[attr-defined]
        events_file = state_dir / "btc_usd_events.jsonl"
        assert events_file.exists()
        assert events_file.read_text(encoding="utf-8").strip()
