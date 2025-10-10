from __future__ import annotations

import datetime as dt
import importlib
import json


def test_reglog_insecure_defaults_persists_chain(tmp_path, monkeypatch):
    monkeypatch.setenv("REGLOG_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("REGLOG_STATE_DIR", str(tmp_path))

    import reglog

    importlib.reload(reglog)

    monkeypatch.setattr(reglog, "psycopg", None)
    monkeypatch.setattr(reglog, "_PSYCOPG_IMPORT_ERROR", ModuleNotFoundError("psycopg"))

    ts1 = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
    ts2 = ts1 + dt.timedelta(minutes=1)

    hash_one = reglog.append_log("director-1", "config.update", {"key": "value"}, timestamp=ts1)
    hash_two = reglog.append_log("director-2", "config.update", {"key": "value2"}, timestamp=ts2)

    assert hash_one != hash_two

    log_path = tmp_path / "reglog_log.json"
    contents = json.loads(log_path.read_text())
    assert len(contents) == 2
    assert contents[0]["hash"] == hash_one

    assert reglog.verify_chain()
