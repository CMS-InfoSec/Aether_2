from __future__ import annotations

import asyncio
import sys
import threading
import types
from pathlib import Path
from typing import Any, Callable, Dict, List

import pytest

pytest.importorskip("fastapi")


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_adapters_stub = types.ModuleType("services.common.adapters")


class _StubKafkaAdapter:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id

    @staticmethod
    async def flush_events() -> Dict[str, int]:  # pragma: no cover - trivial stub
        return {}

    async def publish(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - trivial stub
        return None


_original_modules: Dict[str, types.ModuleType | None] = {}


def _install_stub(name: str, module: types.ModuleType) -> None:
    if name not in _original_modules:
        _original_modules[name] = sys.modules.get(name)
    sys.modules[name] = module


_adapters_stub.KafkaNATSAdapter = _StubKafkaAdapter  # type: ignore[attr-defined]
_install_stub("services.common.adapters", _adapters_stub)

aiohttp_stub = types.ModuleType("aiohttp")


class _ClientSession:  # pragma: no cover - stub implementation
    async def __aenter__(self) -> "_ClientSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def close(self) -> None:
        return None

    async def get(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("aiohttp stub used")

    async def post(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("aiohttp stub used")


class _ClientTimeout:  # pragma: no cover - stub implementation
    def __init__(self, total: float | None = None) -> None:
        self.total = total


aiohttp_stub.ClientSession = _ClientSession
aiohttp_stub.ClientTimeout = _ClientTimeout
aiohttp_stub.ClientError = Exception
_install_stub("aiohttp", aiohttp_stub)

_kraken_rest_stub = types.ModuleType("services.oms.kraken_rest")


class _StubRESTClient:
    def __init__(self, *, credential_getter, **_: Any) -> None:
        self.credential_getter = credential_getter

    async def close(self) -> None:  # pragma: no cover - stub implementation
        return None


class _StubRESTError(Exception):
    pass


_kraken_rest_stub.KrakenRESTClient = _StubRESTClient  # type: ignore[attr-defined]
_kraken_rest_stub.KrakenRESTError = _StubRESTError  # type: ignore[attr-defined]
_install_stub("services.oms.kraken_rest", _kraken_rest_stub)

_kraken_ws_stub = types.ModuleType("services.oms.kraken_ws")


class _StubWSClient:
    def __init__(self, *, credential_getter, **_: Any) -> None:
        self.credential_getter = credential_getter

    async def close(self) -> None:  # pragma: no cover - stub implementation
        return None


class _StubWSError(Exception):
    pass


class _StubWSTimeout(Exception):
    pass


class _StubOrderAck:
    exchange_order_id: str | None = None
    status: str | None = None
    filled_qty: float | None = None
    avg_price: float | None = None
    errors: List[str] | None = None


class _StubOrderState:
    pass


_kraken_ws_stub.KrakenWSClient = _StubWSClient  # type: ignore[attr-defined]
_kraken_ws_stub.KrakenWSError = _StubWSError  # type: ignore[attr-defined]
_kraken_ws_stub.KrakenWSTimeout = _StubWSTimeout  # type: ignore[attr-defined]
_kraken_ws_stub.OrderAck = _StubOrderAck  # type: ignore[attr-defined]
_kraken_ws_stub.OrderState = _StubOrderState  # type: ignore[attr-defined]
_install_stub("services.oms.kraken_ws", _kraken_ws_stub)

_rate_limit_stub = types.ModuleType("services.oms.rate_limit_guard")


class _StubRateLimitGuard:
    async def acquire(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - stub implementation
        return None

    async def release(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - stub implementation
        return None


_rate_limit_stub.RateLimitGuard = _StubRateLimitGuard  # type: ignore[attr-defined]
_rate_limit_stub.rate_limit_guard = _StubRateLimitGuard()  # type: ignore[attr-defined]
_install_stub("services.oms.rate_limit_guard", _rate_limit_stub)

_oms_service_helpers = types.ModuleType("services.oms.oms_service")


class _StubPrecisionValidator:
    @staticmethod
    def validate(symbol: str, qty: Any, limit_px: Any, metadata: Any) -> tuple[Any, Any]:
        return qty, limit_px

    @staticmethod
    def _step(*args: Any, **kwargs: Any) -> None:
        return None

    @staticmethod
    def _snap(value: Any, _step: Any) -> Any:
        return value


def _stub_normalize_symbol(symbol: str) -> str:
    return symbol


def _stub_resolve_pair_metadata(symbol: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    return metadata.get(symbol, {}) if isinstance(metadata, dict) else {}


_oms_service_helpers._PrecisionValidator = _StubPrecisionValidator  # type: ignore[attr-defined]
_oms_service_helpers._normalize_symbol = _stub_normalize_symbol  # type: ignore[attr-defined]
_oms_service_helpers._resolve_pair_metadata = _stub_resolve_pair_metadata  # type: ignore[attr-defined]
_install_stub("services.oms.oms_service", _oms_service_helpers)

_oms_kraken_stub = types.ModuleType("services.oms.oms_kraken")


class _DummyWatcher:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id

    @classmethod
    def instance(cls, account_id: str) -> "_DummyWatcher":
        return cls(account_id)

    def snapshot(self) -> tuple[Dict[str, Any], int]:
        return {}, 0

    def subscribe(self, callback: Callable[[Dict[str, Any], int], None]) -> Callable[[], None]:
        return lambda: None


_oms_kraken_stub.KrakenCredentialWatcher = _DummyWatcher  # type: ignore[attr-defined]
_install_stub("services.oms.oms_kraken", _oms_kraken_stub)

import oms_service

for name, original in _original_modules.items():
    if original is None:
        sys.modules.pop(name, None)
    else:
        sys.modules[name] = original


class _StubWatcher:
    _instances: Dict[str, "_StubWatcher"] = {}

    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self.version = 1
        self.payload: Dict[str, Any] = {
            "api_key": f"key-{account_id}",
            "api_secret": f"secret-{account_id}",
            "metadata": {"rotated_at": "2024-01-01T00:00:00+00:00"},
        }
        self._listeners: List[Callable[[Dict[str, Any], int], None]] = []

    @classmethod
    def reset(cls) -> None:
        cls._instances.clear()

    @classmethod
    def instance(cls, account_id: str) -> "_StubWatcher":
        existing = cls._instances.get(account_id)
        if existing is None:
            existing = cls(account_id)
            cls._instances[account_id] = existing
        return existing

    def snapshot(self) -> tuple[Dict[str, Any], int]:
        return dict(self.payload), self.version

    def subscribe(self, callback: Callable[[Dict[str, Any], int], None]) -> Callable[[], None]:
        self._listeners.append(callback)

        def _unsubscribe() -> None:
            try:
                self._listeners.remove(callback)
            except ValueError:
                pass

        return _unsubscribe

    def rotate(self, payload: Dict[str, Any]) -> None:
        self.payload = dict(payload)
        self.version += 1
        threads: List[threading.Thread] = []
        for listener in list(self._listeners):
            thread = threading.Thread(
                target=listener,
                args=(dict(self.payload), self.version),
            )
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()


@pytest.fixture(autouse=True)
def _reset_watcher() -> None:
    _StubWatcher.reset()


@pytest.fixture
def provider(monkeypatch: pytest.MonkeyPatch) -> oms_service.CredentialProvider:
    provider = oms_service.CredentialProvider(watcher_factory=_StubWatcher.instance)
    monkeypatch.setattr(oms_service.oms_service, "_credential_provider", provider)
    return provider


@pytest.mark.anyio
async def test_credential_provider_refreshes_on_rotation(provider: oms_service.CredentialProvider) -> None:
    credentials = await provider.get("acct")
    assert credentials["api_key"] == "key-acct"

    watcher = _StubWatcher.instance("acct")
    watcher.rotate(
        {
            "api_key": "rotated-key",
            "api_secret": "rotated-secret",
            "metadata": {"rotated_at": "2024-02-01T00:00:00+00:00"},
        }
    )

    await asyncio.sleep(0)

    refreshed = await provider.get("acct")
    assert refreshed["api_key"] == "rotated-key"
    assert refreshed["api_secret"] == "rotated-secret"


@pytest.mark.anyio
async def test_credential_provider_raises_when_material_missing(provider: oms_service.CredentialProvider) -> None:
    watcher = _StubWatcher.instance("missing")
    watcher.payload = {"metadata": {}}
    watcher.version += 1

    with pytest.raises(RuntimeError):
        await provider.get("missing")


@pytest.mark.anyio
async def test_startup_validation_requires_credentials(
    provider: oms_service.CredentialProvider, monkeypatch: pytest.MonkeyPatch
) -> None:
    watcher = _StubWatcher.instance("startup")
    watcher.payload = {"api_key": "", "api_secret": ""}
    monkeypatch.setenv("OMS_REQUIRED_ACCOUNTS", "startup")

    with pytest.raises(RuntimeError):
        await oms_service._validate_startup_credentials()


@pytest.mark.anyio
async def test_startup_validation_passes_with_valid_credentials(
    provider: oms_service.CredentialProvider, monkeypatch: pytest.MonkeyPatch
) -> None:
    _StubWatcher.instance("healthy")
    monkeypatch.setenv("OMS_REQUIRED_ACCOUNTS", "healthy")

    await oms_service._validate_startup_credentials()
