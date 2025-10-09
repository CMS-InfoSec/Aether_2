from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from shared import trade_logging


def test_trade_logger_appends_csv(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("INFO")
    log_path = tmp_path / "trade_log.csv"
    logger = trade_logging.TradeLogger(path=log_path)

    entry = trade_logging.TradeLogEntry(
        timestamp=datetime(2024, 6, 15, 12, 30, tzinfo=timezone.utc),
        account_id="alpha",
        client_order_id="cid-123",
        exchange_order_id="ex-123",
        symbol="BTC/USD",
        side="buy",
        quantity=Decimal("1.25"),
        price=Decimal("19950.5"),
        pnl=Decimal("15.75"),
        pre_trade_mid=Decimal("19938.9"),
        transport="websocket",
        simulated=False,
    )

    logger.log(entry)
    logger.log(
        trade_logging.TradeLogEntry(
            timestamp=datetime(2024, 6, 15, 13, 0, tzinfo=timezone.utc),
            account_id="alpha",
            client_order_id="cid-124",
            exchange_order_id="ex-124",
            symbol="ETH/USD",
            side="sell",
            quantity=Decimal("2"),
            price=Decimal("3500"),
            pnl=Decimal("-12.3"),
            pre_trade_mid=Decimal("3504"),
            transport="rest",
            simulated=True,
        )
    )

    assert log_path.exists()
    rows = list(csv.DictReader(log_path.open()))
    assert len(rows) == 2
    assert rows[0]["account_id"] == "alpha"
    assert rows[0]["symbol"] == "BTC/USD"
    assert rows[0]["quantity"] == "1.25"
    assert rows[0]["price"] == "19950.5"
    assert rows[0]["pnl"] == "15.75"
    assert rows[1]["side"] == "sell"
    assert rows[1]["simulated"] == "true"

    messages = [record.msg for record in caplog.records if record.name == "trade.journal"]
    assert any("trade.executed" in message for message in messages)


def test_override_trade_logger_restores_previous(tmp_path: Path) -> None:
    default = trade_logging.get_trade_logger()

    class _RecordingLogger(trade_logging.TradeLogger):
        def __init__(self, path: Path) -> None:
            super().__init__(path=path)
            self.entries: list[trade_logging.TradeLogEntry] = []

        def log(self, entry: trade_logging.TradeLogEntry) -> None:  # type: ignore[override]
            self.entries.append(entry)

    custom = _RecordingLogger(tmp_path / "override.csv")

    with trade_logging.override_trade_logger(custom) as active:
        assert active is custom
        assert trade_logging.get_trade_logger() is custom
        active.log(
            trade_logging.TradeLogEntry(
                timestamp=datetime.now(timezone.utc),
                account_id="beta",
                client_order_id="cid-override",
                exchange_order_id="ex-override",
                symbol="BTC/USD",
                side="buy",
                quantity=Decimal("0.5"),
                price=Decimal("20000"),
                pnl=Decimal("5"),
            )
        )
        assert len(custom.entries) == 1

    assert trade_logging.get_trade_logger() is default


def test_trade_logger_exposes_journal_path(tmp_path: Path) -> None:
    log_path = tmp_path / "journal.csv"
    logger = trade_logging.TradeLogger(path=log_path)

    assert logger.path == log_path


def test_read_trade_log_filters_account_and_window(tmp_path: Path) -> None:
    log_path = tmp_path / "journal.csv"
    logger = trade_logging.TradeLogger(path=log_path)
    start = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    second = start + timedelta(hours=1)
    third = start + timedelta(hours=2)

    logger.log(
        trade_logging.TradeLogEntry(
            timestamp=start,
            account_id="alpha",
            client_order_id="cid-1",
            exchange_order_id="ex-1",
            symbol="BTC/USD",
            side="buy",
            quantity=Decimal("1"),
            price=Decimal("20000"),
            pnl=Decimal("10"),
        )
    )
    logger.log(
        trade_logging.TradeLogEntry(
            timestamp=second,
            account_id="beta",
            client_order_id="cid-2",
            exchange_order_id="ex-2",
            symbol="ETH/USD",
            side="sell",
            quantity=Decimal("2"),
            price=Decimal("3000"),
            pnl=Decimal("-5"),
        )
    )
    logger.log(
        trade_logging.TradeLogEntry(
            timestamp=third,
            account_id="alpha",
            client_order_id="cid-3",
            exchange_order_id="ex-3",
            symbol="ETH/USD",
            side="buy",
            quantity=Decimal("0.5"),
            price=Decimal("2500"),
            pnl=Decimal("4"),
        )
    )

    rows = trade_logging.read_trade_log(
        account_id="alpha",
        start=start + timedelta(minutes=30),
        end=third + timedelta(minutes=1),
        path=log_path,
    )

    assert [row["client_order_id"] for row in rows] == ["cid-3"]
