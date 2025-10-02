"""Exchange adapter abstractions for interacting with upstream OMS services."""

from __future__ import annotations

import asyncio
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional
from uuid import uuid4

import httpx

from metrics import get_request_id

logger = logging.getLogger(__name__)

_DEFAULT_PRIMARY_URL = os.getenv("OMS_SERVICE_URL", "http://oms-service")
_DEFAULT_PAPER_URL = os.getenv("PAPER_OMS_SERVICE_URL", "http://paper-oms-service")
_DEFAULT_TIMEOUT = float(os.getenv("OMS_REQUEST_TIMEOUT", "1.0"))


class ExchangeAdapter(ABC):
    """Base interface for OMS integrations."""

    def __init__(
        self,
        name: str,
        capabilities: Optional[Mapping[str, bool]] = None,
    ) -> None:
        self.name = name.lower()
        base_capabilities: Dict[str, bool] = {
            "place_order": False,
            "cancel_order": False,
            "get_balance": False,
            "get_trades": False,
        }
        if capabilities:
            base_capabilities.update({key: bool(value) for key, value in capabilities.items()})
        self._capabilities = base_capabilities

    @property
    def capabilities(self) -> Mapping[str, bool]:
        """Return the capability flags supported by this adapter."""

        return dict(self._capabilities)

    def supports(self, operation: str) -> bool:
        """Return ``True`` when the adapter implements ``operation``."""

        return bool(self._capabilities.get(operation, False))

    @abstractmethod
    async def place_order(
        self,
        account_id: str,
        payload: Mapping[str, Any],
        *,
        shadow: bool = False,
    ) -> Mapping[str, Any]:
        """Submit an order to the exchange."""

    @abstractmethod
    async def cancel_order(
        self,
        account_id: str,
        client_id: str,
        *,
        exchange_order_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        """Cancel a previously submitted order."""

    @abstractmethod
    async def get_balance(self, account_id: str) -> Mapping[str, Any]:
        """Return the latest account balance snapshot."""

    @abstractmethod
    async def get_trades(
        self,
        account_id: str,
        *,
        limit: int = 50,
    ) -> List[Mapping[str, Any]]:
        """Return the most recent trade executions for ``account_id``."""

    async def status(self) -> Mapping[str, Any]:  # pragma: no cover - thin wrapper
        """Return adapter health metadata used for discovery endpoints."""

        return {"available": True, "details": {}}


def _join_url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/")
    suffix = path if path.startswith("/") else f"/{path}"
    return f"{base}{suffix}" if base else suffix


def _account_headers(account_id: str) -> Dict[str, str]:
    request_id = get_request_id() or str(uuid4())
    return {"X-Account-ID": account_id, "X-Request-ID": request_id}


class KrakenAdapter(ExchangeAdapter):
    """Adapter implementation for the Kraken OMS service."""

    def __init__(
        self,
        *,
        primary_url: Optional[str] = None,
        paper_url: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        super().__init__(
            "kraken",
            capabilities={"place_order": True, "cancel_order": True},
        )
        self._primary_url = (primary_url or _DEFAULT_PRIMARY_URL or "").strip()
        self._paper_url = (paper_url or _DEFAULT_PAPER_URL or "").strip()
        self._timeout = timeout if timeout is not None else _DEFAULT_TIMEOUT

    async def place_order(
        self,
        account_id: str,
        payload: Mapping[str, Any],
        *,
        shadow: bool = False,
    ) -> Mapping[str, Any]:
        base_url = self._paper_url if shadow else self._primary_url
        if not base_url:
            logger.debug(
                "Skipping order submission for exchange %s (shadow=%s): no base URL configured",  # pragma: no cover - env configuration guard
                self.name,
                shadow,
            )
            return {}

        url = _join_url(base_url, "/oms/place")
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                url,
                json=dict(payload),
                headers=_account_headers(account_id),
            )
            response.raise_for_status()
            try:
                return response.json()
            except ValueError:  # pragma: no cover - defensive guard
                return {}

    async def cancel_order(
        self,
        account_id: str,
        client_id: str,
        *,
        exchange_order_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        if not self._primary_url:
            raise RuntimeError("Kraken OMS URL is not configured")
        url = _join_url(self._primary_url, "/oms/cancel")
        payload: Dict[str, Any] = {
            "account_id": account_id,
            "client_id": client_id,
        }
        if exchange_order_id:
            payload["exchange_order_id"] = exchange_order_id
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                url,
                json=payload,
                headers=_account_headers(account_id),
            )
            response.raise_for_status()
            try:
                return response.json()
            except ValueError:  # pragma: no cover - defensive guard
                return {}

    async def get_balance(self, account_id: str) -> Mapping[str, Any]:
        raise NotImplementedError("Balance retrieval is not implemented for KrakenAdapter")

    async def get_trades(
        self,
        account_id: str,
        *,
        limit: int = 50,
    ) -> List[Mapping[str, Any]]:
        raise NotImplementedError("Trade history retrieval is not implemented for KrakenAdapter")

    async def status(self) -> Mapping[str, Any]:
        async def _probe(url: str) -> Mapping[str, Any]:
            if not url:
                return {"available": False, "reason": "not_configured"}

            endpoints: Iterable[str] = ("/health", "/ready", "/")
            last_error: Optional[str] = None
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                for endpoint in endpoints:
                    target = _join_url(url, endpoint)
                    try:
                        response = await client.get(target)
                    except httpx.HTTPError as exc:  # pragma: no cover - network error guard
                        last_error = str(exc)
                        continue
                    if response.status_code < 400:
                        return {
                            "available": True,
                            "endpoint": endpoint,
                            "status_code": response.status_code,
                        }
                    last_error = f"HTTP {response.status_code}"
            return {"available": False, "error": last_error or "unreachable"}

        return {
            "exchange": self.name,
            "capabilities": self.capabilities,
            "transports": {
                "primary": await _probe(self._primary_url),
                "paper": await _probe(self._paper_url),
            },
        }


class BinanceAdapter(ExchangeAdapter):
    """Placeholder adapter for Binance spot trading."""

    def __init__(self) -> None:
        super().__init__("binance")

    async def place_order(
        self,
        account_id: str,
        payload: Mapping[str, Any],
        *,
        shadow: bool = False,
    ) -> Mapping[str, Any]:
        raise NotImplementedError("BinanceAdapter is not implemented yet")

    async def cancel_order(
        self,
        account_id: str,
        client_id: str,
        *,
        exchange_order_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        raise NotImplementedError("BinanceAdapter is not implemented yet")

    async def get_balance(self, account_id: str) -> Mapping[str, Any]:
        raise NotImplementedError("BinanceAdapter is not implemented yet")

    async def get_trades(
        self,
        account_id: str,
        *,
        limit: int = 50,
    ) -> List[Mapping[str, Any]]:
        raise NotImplementedError("BinanceAdapter is not implemented yet")

    async def status(self) -> Mapping[str, Any]:
        return {"exchange": self.name, "available": False, "reason": "not_implemented"}


class CoinbaseAdapter(ExchangeAdapter):
    """Placeholder adapter for Coinbase exchange integration."""

    def __init__(self) -> None:
        super().__init__("coinbase")

    async def place_order(
        self,
        account_id: str,
        payload: Mapping[str, Any],
        *,
        shadow: bool = False,
    ) -> Mapping[str, Any]:
        raise NotImplementedError("CoinbaseAdapter is not implemented yet")

    async def cancel_order(
        self,
        account_id: str,
        client_id: str,
        *,
        exchange_order_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        raise NotImplementedError("CoinbaseAdapter is not implemented yet")

    async def get_balance(self, account_id: str) -> Mapping[str, Any]:
        raise NotImplementedError("CoinbaseAdapter is not implemented yet")

    async def get_trades(
        self,
        account_id: str,
        *,
        limit: int = 50,
    ) -> List[Mapping[str, Any]]:
        raise NotImplementedError("CoinbaseAdapter is not implemented yet")

    async def status(self) -> Mapping[str, Any]:
        return {"exchange": self.name, "available": False, "reason": "not_implemented"}


_ADAPTER_FACTORIES: Dict[str, Callable[[], ExchangeAdapter]] = {
    "kraken": KrakenAdapter,
    "binance": BinanceAdapter,
    "coinbase": CoinbaseAdapter,
}
_ADAPTER_CACHE: MutableMapping[str, ExchangeAdapter] = {}


def get_exchange_adapter(name: Optional[str] = None) -> ExchangeAdapter:
    """Return the adapter registered for ``name`` (defaults to Kraken)."""

    adapter_name = (name or os.getenv("PRIMARY_EXCHANGE", "kraken")).lower()
    factory = _ADAPTER_FACTORIES.get(adapter_name)
    if factory is None:
        raise ValueError(f"Unknown exchange adapter '{adapter_name}'")
    if adapter_name not in _ADAPTER_CACHE:
        _ADAPTER_CACHE[adapter_name] = factory()
    return _ADAPTER_CACHE[adapter_name]


async def get_exchange_adapters_status() -> List[Mapping[str, Any]]:
    """Return status metadata for all registered exchange adapters."""

    adapters = [get_exchange_adapter(name) for name in _ADAPTER_FACTORIES]
    results: List[Mapping[str, Any]] = []
    statuses = await asyncio.gather(*(adapter.status() for adapter in adapters), return_exceptions=True)
    for adapter, status in zip(adapters, statuses):
        if isinstance(status, Exception):
            logger.debug(
                "Failed to compute status for adapter %s: %s", adapter.name, status
            )
            results.append(
                {
                    "exchange": adapter.name,
                    "capabilities": adapter.capabilities,
                    "status": {"available": False, "error": str(status)},
                }
            )
            continue
        results.append(
            {
                "exchange": adapter.name,
                "capabilities": adapter.capabilities,
                "status": status,
            }
        )
    return results


__all__ = [
    "ExchangeAdapter",
    "KrakenAdapter",
    "BinanceAdapter",
    "CoinbaseAdapter",
    "get_exchange_adapter",
    "get_exchange_adapters_status",
]
