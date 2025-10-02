import json
import logging
from pathlib import Path
from decimal import Decimal

import pytest

pytest.importorskip("fastapi")

from services.oms import oms_service
from services.oms.oms_service import (
    AccountContext,
    CredentialLoadError,
    CredentialWatcher,
    OMSPlaceRequest,
)


@pytest.mark.asyncio
async def test_credential_watcher_missing_file_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(CredentialWatcher, "_base_path", tmp_path)
    CredentialWatcher._instances = {}
    watcher = CredentialWatcher("missing-account")

    with pytest.raises(CredentialLoadError):
        await watcher.start()

    assert watcher._task is None


@pytest.mark.asyncio
async def test_credential_watcher_invalid_payload_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(CredentialWatcher, "_base_path", tmp_path)
    CredentialWatcher._instances = {}

    secret_dir = tmp_path / "acct"
    secret_dir.mkdir(parents=True)
    secret_file = secret_dir / "credentials.json"
    secret_file.write_text("{ invalid json")

    watcher = CredentialWatcher("acct")

    with pytest.raises(CredentialLoadError):
        await watcher.start()

    assert watcher._task is None


@pytest.mark.asyncio
async def test_credential_watcher_missing_required_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(CredentialWatcher, "_base_path", tmp_path)
    CredentialWatcher._instances = {}

    secret_dir = tmp_path / "acct"
    secret_dir.mkdir(parents=True)
    secret_file = secret_dir / "credentials.json"
    secret_file.write_text(json.dumps({"api_key": "only-key"}))

    watcher = CredentialWatcher("acct")

    with pytest.raises(CredentialLoadError):
        await watcher.start()

    assert watcher._task is None


@pytest.mark.asyncio
async def test_account_context_start_reports_missing_credentials(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR, logger=oms_service.logger.name)

    class FailingCredentialWatcher:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id
            self.started = False

        @classmethod
        def instance(cls, account_id: str) -> "FailingCredentialWatcher":
            return cls(account_id)

        async def start(self) -> None:
            self.started = True
            raise CredentialLoadError("credentials missing")

        async def get_credentials(self) -> dict:
            return {}

        async def get_metadata(self) -> dict | None:
            return None

    metrics_calls = []

    def _record_metric(account: str, symbol: str, transport: str, *, service=None) -> None:
        metrics_calls.append((account, symbol, transport, service))

    monkeypatch.setattr(oms_service, "CredentialWatcher", FailingCredentialWatcher)
    monkeypatch.setattr(oms_service, "increment_oms_error_count", _record_metric)

    class DummyLatencyRouter:
        def __init__(self, account_id: str) -> None:
            self.account_id = account_id

        async def start(self, ws_client=None, rest_client=None) -> None:
            return None

        async def stop(self) -> None:
            return None

    monkeypatch.setattr(oms_service, "LatencyRouter", DummyLatencyRouter)

    account = AccountContext("MISSING")

    request = OMSPlaceRequest(
        account_id="MISSING",
        client_id="abc",
        symbol="BTC/USD",
        side="buy",
        order_type="market",
        qty=Decimal("1"),
    )

    with pytest.raises(CredentialLoadError):
        await account.place_order(request)

    assert account.ws_client is None
    assert account.rest_client is None
    assert metrics_calls == [("MISSING", "credentials", "startup", None)]
    assert any(
        "credential load error" in message.lower()
        for message in caplog.text.splitlines()
    )
