from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import sys

import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import select

pytest.importorskip("fastapi")
pytest.importorskip("services.common.security")

from tests.helpers.risk import esg_filter_instance, reload_esg_filter


def test_esg_assets_persist_across_restarts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_name = "esg_persist.db"
    symbol = "eth-usd"

    with esg_filter_instance(tmp_path, monkeypatch, db_filename=db_name) as module_a:
        entry = module_a.esg_filter.update_asset(symbol, 72.5, "watch", "initial load")
        assets = module_a.esg_filter.list_assets()
        assert len(assets) == 1
        assert assets[0].symbol == entry.symbol
        assert assets[0].flag == entry.flag
        assert assets[0].reason == entry.reason

    with esg_filter_instance(tmp_path, monkeypatch, db_filename=db_name) as module_b:
        assets_after_restart = module_b.esg_filter.list_assets()
        assert len(assets_after_restart) == 1
        stored = assets_after_restart[0]
        assert stored.symbol == "ETH-USD"
        assert stored.flag == "watch"
        assert stored.reason == "initial load"


def test_esg_rejections_visible_to_new_instances(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_name = "esg_consistency.db"
    account_a = str(uuid4())
    account_b = str(uuid4())
    symbol = "btc-usd"

    with esg_filter_instance(tmp_path, monkeypatch, db_filename=db_name) as module_a:
        entry = module_a.esg_filter.update_asset(symbol, 5.0, "high_regulatory", "blocked")
        module_a.esg_filter.log_rejection(account_a, symbol, entry)
        with module_a.SessionLocal() as session:
            records = session.execute(select(module_a.ESGRejection)).scalars().all()
            assert len(records) == 1
            assert str(records[0].account_id) == account_a
            assert records[0].symbol == "BTC-USD"

    module_b = reload_esg_filter(tmp_path, monkeypatch, db_filename=db_name)
    try:
        allowed, entry_b = module_b.esg_filter.evaluate(symbol)
        assert allowed is False
        assert entry_b is not None
        module_b.esg_filter.log_rejection(account_b, symbol, entry_b)
        with module_b.SessionLocal() as session:
            records = session.execute(select(module_b.ESGRejection)).scalars().all()
            assert len(records) == 2
            assert {str(record.account_id) for record in records} == {account_a, account_b}
            assert {record.symbol for record in records} == {"BTC-USD"}
    finally:
        module_b.ENGINE.dispose()
        sys.modules.pop("esg_filter", None)
