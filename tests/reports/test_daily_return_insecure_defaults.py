from __future__ import annotations

import importlib
import json
from datetime import date

def test_daily_return_summary_fallback(tmp_path, monkeypatch):
    monkeypatch.setenv("REPORTS_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("AETHER_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("AETHER_ACCOUNT_ID", "company")
    for env_key in ("REPORT_DATABASE_URL", "TIMESCALE_DSN", "DATABASE_URL"):
        monkeypatch.delenv(env_key, raising=False)

    from services.reports import report_service

    importlib.reload(report_service)
    report_service.reset_daily_report_service_cache()

    monkeypatch.setattr(report_service, "sql", None)

    service = report_service.get_daily_report_service()

    target_date = date(2024, 5, 1)
    summary = service.get_daily_return_summary(account_id="company", nav_date=target_date)

    assert summary["account_id"] == "company"
    assert summary["date"] == target_date.isoformat()
    assert isinstance(summary["daily_return_pct"], float)
    assert summary["nav_open"] > 0

    nav_path = tmp_path / "reports" / "daily_nav.json"
    assert nav_path.exists()
    stored = json.loads(nav_path.read_text())
    assert "company" in stored
    assert target_date.isoformat() in stored["company"]

    repeat = service.get_daily_return_summary(account_id="company", nav_date=target_date)
    assert repeat == summary
