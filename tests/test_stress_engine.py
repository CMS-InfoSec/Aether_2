import json
from datetime import datetime, timezone

import pytest

import stress_engine


def _write_log(path, messages):
    with path.open("w", encoding="utf-8") as handle:
        for message in messages:
            handle.write(json.dumps(message) + "\n")


def test_replay_from_ws_log_without_pandas(tmp_path, monkeypatch):
    def fallback_timestamp(value: float):
        return datetime.fromtimestamp(value, tz=timezone.utc)

    monkeypatch.setattr(stress_engine, "_TIMESTAMP_FACTORY", fallback_timestamp)

    log_path = tmp_path / "ws.log"
    _write_log(
        log_path,
        [
            [
                0,
                [
                    ["100", "0.2", "1690000001", "buy", "market", ""],
                    ["101", "0.1", "1690000000", "sell", "limit", ""],
                ],
                "trade",
                "XBT/USD",
            ],
            [1, [], "trade", "XBT/USD"],
            "invalid",
        ],
    )

    records = stress_engine.replay_from_ws_log(log_path)
    assert [record["price"] for record in records] == [101.0, 100.0]
    assert records[0]["timestamp"] == fallback_timestamp(1690000000.0)
    assert records[1]["timestamp"] == fallback_timestamp(1690000001.0)
    assert records[0]["side"] == "sell"
    assert records[1]["side"] == "buy"


def test_replay_from_ws_log_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        stress_engine.replay_from_ws_log(tmp_path / "missing.log")
