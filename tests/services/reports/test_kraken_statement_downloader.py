from __future__ import annotations

import importlib
import importlib.util
import pathlib
import sys

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
project_root_str = str(PROJECT_ROOT)
if project_root_str in sys.path:
    sys.path.remove(project_root_str)
sys.path.insert(0, project_root_str)
unique_path = []
seen_paths = set()
for entry in sys.path:
    if entry in seen_paths:
        continue
    unique_path.append(entry)
    seen_paths.add(entry)
sys.path[:] = unique_path
for module_name in (
    "services",
    "services.reports",
    "services.common",
    "services.common.config",
):
    sys.modules.pop(module_name, None)

import base64
from datetime import datetime, timezone
from typing import Any, Dict, List

import pytest

try:
    from services.reports.kraken_reconciliation import KrakenStatementDownloader
except ModuleNotFoundError:  # pragma: no cover - fallback for non-package execution
    module_path = PROJECT_ROOT / "services" / "reports" / "kraken_reconciliation.py"
    spec = importlib.util.spec_from_file_location(
        "services.reports.kraken_reconciliation", module_path
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    KrakenStatementDownloader = module.KrakenStatementDownloader


class _ResponseStub:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:  # pragma: no cover - interface parity
        return None


class _RecordingSession:
    def __init__(self) -> None:
        self.requests: List[Dict[str, Any]] = []

    def get(self, url: str, params: Dict[str, Any], headers: Dict[str, str]):
        self.requests.append({
            "url": url,
            "params": dict(params),
            "headers": dict(headers),
        })
        if params.get("format") == "csv":
            csv_payload = (
                "order_id,executed_at,quantity,price,fee\n"
                "abc,2023-01-01T00:00:00+00:00,1,100,0.1\n"
            )
            return _ResponseStub(csv_payload)
        json_payload = "{\"nav_snapshots\": [], \"fee_adjustments\": []}"
        return _ResponseStub(json_payload)


def _statement_downloader(**kwargs: Any) -> tuple[KrakenStatementDownloader, _RecordingSession]:
    session = _RecordingSession()
    downloader = KrakenStatementDownloader(
        base_url="https://api.test.kraken.com",
        session=session,
        **kwargs,
    )
    return downloader, session


def test_fetch_uses_signed_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = base64.b64encode(b"supersecret").decode()
    downloader, session = _statement_downloader(api_key="api_key_123", api_secret=secret)

    monkeypatch.setattr(
        "services.reports.kraken_reconciliation.time.time",
        lambda: 1700000000.0,
    )

    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end = datetime(2023, 1, 2, tzinfo=timezone.utc)
    downloader.fetch("account-1", start, end)

    assert len(session.requests) == 2
    for request in session.requests:
        headers = request["headers"]
        params = request["params"]
        assert "API-Secret" not in headers
        assert headers.get("API-Key") == "api_key_123"
        assert "API-Sign" in headers
        assert params.get("nonce") == "1700000000000"

    from services.secrets.signing import sign_kraken_request

    csv_request = session.requests[0]
    expected_csv_signature = sign_kraken_request(
        "/statements/trades",
        csv_request["params"],
        secret,
    )[1]
    json_request = session.requests[1]
    expected_json_signature = sign_kraken_request(
        "/statements/accounting",
        json_request["params"],
        secret,
    )[1]

    assert session.requests[0]["headers"]["API-Sign"] == expected_csv_signature
    assert session.requests[1]["headers"]["API-Sign"] == expected_json_signature


def test_fetch_without_credentials_does_not_include_secrets() -> None:
    downloader, session = _statement_downloader()

    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end = datetime(2023, 1, 2, tzinfo=timezone.utc)
    downloader.fetch("account-1", start, end)

    assert len(session.requests) == 2
    for request in session.requests:
        headers = request["headers"]
        params = request["params"]
        assert "API-Secret" not in headers
        assert "API-Sign" not in headers
        assert "Authorization" not in headers
        assert "nonce" not in params

