import importlib.util
import json
import logging
import sys
import time
from pathlib import Path
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pytest

pytest.importorskip("fastapi")

from fastapi import HTTPException, Request, status

import metrics as metrics_module

metrics_module.setup_metrics = lambda *_, **__: None  # type: ignore[assignment]
metrics_module.init_metrics = lambda *_, **__: None  # type: ignore[assignment]

MODULE_PATH = Path(__file__).resolve().parents[3] / "services" / "oms" / "oms_service.py"
MODULE_SPEC = importlib.util.spec_from_file_location("services.oms.oms_service", MODULE_PATH)
assert MODULE_SPEC and MODULE_SPEC.loader
oms_service = importlib.util.module_from_spec(MODULE_SPEC)


async def _require_authorized_account(request: Request) -> str:
    return request.headers.get("X-Account-ID", "test-account")


oms_service.require_authorized_account = _require_authorized_account  # type: ignore[attr-defined]
sys.modules.setdefault("services.oms.oms_service", oms_service)
MODULE_SPEC.loader.exec_module(oms_service)

from services.oms.oms_service import (  # type: ignore[no-redef]
    AccountContext,
    CredentialLoadError,
    CredentialWatcher,
    OMSPlaceRequest,
)
from services.oms.kraken_ws import OrderAck


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


class _StubCredentialWatcher:
    _instances: Dict[str, "_StubCredentialWatcher"] = {}

    def __init__(self, account_id: str) -> None:
        self.account_id = account_id

    @classmethod
    def instance(cls, account_id: str) -> "_StubCredentialWatcher":
        existing = cls._instances.get(account_id)
        if existing is None:
            existing = cls(account_id)
            cls._instances[account_id] = existing
        return existing

    async def start(self) -> None:
        return None

    async def get_credentials(self) -> Dict[str, str]:
        return {
            "api_key": "stub-key",
            "api_secret": "stub-secret",
            "ws_token": "stub-token",
        }

    async def get_metadata(self) -> Dict[str, Any]:
        return {
            "BTC/USD": {
                "lot_step": "0.1",
                "price_increment": "0.5",
                "ordermin": "0.1",
            }
        }


class _StubLatencyRouter:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self._preferred = "websocket"

    async def start(self, ws_client: Any = None, rest_client: Any = None) -> None:
        return None

    async def stop(self) -> None:
        return None

    @property
    def preferred_path(self) -> str:
        return self._preferred

    def update_probe_template(self, payload: Dict[str, Any]) -> None:
        return None

    def record_latency(self, path: str, latency_ms: float) -> None:
        return None

    def status(self) -> Dict[str, Any]:  # pragma: no cover - helper for completeness
        return {"ws_latency": None, "rest_latency": None, "preferred_path": self._preferred}


class _StubImpactStore:
    async def record_fill(self, **_: Any) -> None:
        return None

    async def impact_curve(self, **_: Any) -> List[Dict[str, Any]]:
        return []


class _StubWSClient:
    def __init__(
        self,
        *,
        credential_getter: Any,
        stream_update_cb: Any,
        heartbeat: Optional[float] = None,
    ) -> None:
        self._credential_getter = credential_getter
        self._stream_update_cb = stream_update_cb
        self._heartbeat_age = heartbeat
        self.add_calls: List[Dict[str, Any]] = []

    async def ensure_connected(self) -> None:
        return None

    async def subscribe_private(self, channels: List[str]) -> None:
        del channels
        return None

    async def stream_handler(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
        self.add_calls.append(dict(payload))
        return OrderAck(
            exchange_order_id="WS-1",
            status="accepted",
            filled_qty=None,
            avg_price=None,
            errors=None,
        )

    def heartbeat_age(self) -> Optional[float]:
        return self._heartbeat_age

    def set_heartbeat_age(self, value: Optional[float]) -> None:
        self._heartbeat_age = value


class _StubRESTClient:
    def __init__(self, *, credential_getter: Any) -> None:
        self._credential_getter = credential_getter
        self.submissions: List[Dict[str, Any]] = []

    async def add_order(self, payload: Dict[str, Any]) -> OrderAck:
        self.submissions.append(dict(payload))
        return OrderAck(
            exchange_order_id="REST-1",
            status="accepted",
            filled_qty=None,
            avg_price=None,
            errors=None,
        )

    async def close(self) -> None:
        return None

    async def cancel_order(self, payload: Dict[str, Any]) -> OrderAck:
        return OrderAck(
            exchange_order_id=str(payload.get("txid") or "REST-1"),
            status="cancelled",
            filled_qty=None,
            avg_price=None,
            errors=None,
        )


class _TestPlaceRequest(OMSPlaceRequest):
    shadow: bool = False


@pytest.mark.asyncio
async def test_place_order_rejects_when_public_book_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubCredentialWatcher._instances = {}
    monkeypatch.setattr(oms_service, "CredentialWatcher", _StubCredentialWatcher)
    monkeypatch.setattr(oms_service, "LatencyRouter", _StubLatencyRouter)
    monkeypatch.setattr(oms_service, "impact_store", _StubImpactStore())

    ws_instances: List[_StubWSClient] = []

    def _ws_factory(**kwargs: Any) -> _StubWSClient:
        client = _StubWSClient(**kwargs)
        client.set_heartbeat_age(0.1)
        ws_instances.append(client)
        return client

    monkeypatch.setattr(oms_service, "KrakenWSClient", _ws_factory)
    monkeypatch.setattr(oms_service, "KrakenRESTClient", lambda **kwargs: _StubRESTClient(**kwargs))
    monkeypatch.setattr(oms_service, "increment_oms_child_orders_total", lambda *_, **__: None)
    monkeypatch.setattr(oms_service, "record_oms_latency", lambda *_, **__: None)

    metrics_calls: List[tuple[str, str, str, str, Optional[str]]] = []

    def _record_metric(
        account: str,
        symbol: str,
        *,
        source: str,
        action: str,
        service: Optional[str] = None,
    ) -> None:
        metrics_calls.append((account, symbol, source, action, service))

    monkeypatch.setattr(oms_service, "increment_oms_stale_feed", _record_metric)

    class _StaleBook:
        async def is_stale(self, threshold: float) -> bool:
            assert threshold >= 0
            return True

        async def depth(self, side: str, levels: int = 10) -> Decimal:  # pragma: no cover - defensive
            raise AssertionError("depth should not be queried for stale books")

        async def last_update(self) -> float:
            return time.time() - 5.0

    class _BookStore:
        def __init__(self, book: Any) -> None:
            self.book = book

        async def ensure_book(self, symbol: str) -> Any:
            assert symbol == "BTC/USD"
            return self.book

    monkeypatch.setattr(oms_service, "order_book_store", _BookStore(_StaleBook()))

    account = AccountContext("ACC-STALE")
    request = _TestPlaceRequest(
        account_id="ACC-STALE",
        client_id="CID-STALE",
        symbol="BTC/USD",
        side="buy",
        type="limit",
        qty=Decimal("0.5"),
        limit_px=Decimal("30000"),
    )
    request.shadow = False  # type: ignore[attr-defined]

    with pytest.raises(HTTPException) as exc:
        await account.place_order(request)

    assert exc.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert metrics_calls == [("ACC-STALE", "BTC/USD", "public_order_book", "rejected", None)]
    assert ws_instances and not ws_instances[0].add_calls

    await account.close()


@pytest.mark.asyncio
async def test_place_order_reroutes_when_private_stream_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _StubCredentialWatcher._instances = {}
    monkeypatch.setattr(oms_service, "CredentialWatcher", _StubCredentialWatcher)
    monkeypatch.setattr(oms_service, "LatencyRouter", _StubLatencyRouter)
    monkeypatch.setattr(oms_service, "impact_store", _StubImpactStore())

    ws_instances: List[_StubWSClient] = []

    def _ws_factory(**kwargs: Any) -> _StubWSClient:
        client = _StubWSClient(**kwargs)
        client.set_heartbeat_age(5.0)
        ws_instances.append(client)
        return client

    rest_instances: List[_StubRESTClient] = []

    def _rest_factory(**kwargs: Any) -> _StubRESTClient:
        client = _StubRESTClient(**kwargs)
        rest_instances.append(client)
        return client

    monkeypatch.setattr(oms_service, "KrakenWSClient", _ws_factory)
    monkeypatch.setattr(oms_service, "KrakenRESTClient", _rest_factory)
    monkeypatch.setattr(oms_service, "increment_oms_child_orders_total", lambda *_, **__: None)
    monkeypatch.setattr(oms_service, "record_oms_latency", lambda *_, **__: None)

    metrics_calls: List[tuple[str, str, str, str, Optional[str]]] = []

    def _record_metric(
        account: str,
        symbol: str,
        *,
        source: str,
        action: str,
        service: Optional[str] = None,
    ) -> None:
        metrics_calls.append((account, symbol, source, action, service))

    monkeypatch.setattr(oms_service, "increment_oms_stale_feed", _record_metric)

    class _FreshBook:
        async def is_stale(self, threshold: float) -> bool:
            assert threshold >= 0
            return False

        async def depth(self, side: str, levels: int = 10) -> Decimal:
            assert side in {"buy", "sell"}
            return Decimal("1")

        async def last_update(self) -> float:
            return time.time()

    class _BookStore:
        def __init__(self, book: Any) -> None:
            self.book = book

        async def ensure_book(self, symbol: str) -> Any:
            assert symbol == "BTC/USD"
            return self.book

    monkeypatch.setattr(oms_service, "order_book_store", _BookStore(_FreshBook()))

    account = AccountContext("ACC-REST")
    request = _TestPlaceRequest(
        account_id="ACC-REST",
        client_id="CID-REST",
        symbol="BTC/USD",
        side="buy",
        type="limit",
        qty=Decimal("0.25"),
        limit_px=Decimal("30500"),
    )
    request.shadow = False  # type: ignore[attr-defined]

    response = await account.place_order(request)

    assert response.transport == "rest"
    assert rest_instances and rest_instances[0].submissions
    assert ws_instances and not ws_instances[0].add_calls
    assert metrics_calls == [("ACC-REST", "BTC/USD", "private_stream", "rerouted", None)]

    await account.close()
