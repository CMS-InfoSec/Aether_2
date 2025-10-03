"""Exchange adapter abstractions for interacting with upstream OMS services."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging
import os
from abc import ABC, abstractmethod


from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
)

from uuid import uuid4


import httpx

from auth.session_client import AdminSessionManager, get_default_session_manager

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


def _account_headers(account_id: str, *, token: Optional[str] = None) -> Dict[str, str]:
    request_id = get_request_id() or str(uuid4())
    headers = {"X-Account-ID": account_id, "X-Request-ID": request_id}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


class KrakenAdapter(ExchangeAdapter):
    """Adapter implementation for the Kraken OMS service."""

    def __init__(
        self,
        *,
        primary_url: Optional[str] = None,
        paper_url: Optional[str] = None,
        timeout: Optional[float] = None,
        session_manager: Optional[AdminSessionManager] = None,
    ) -> None:
        super().__init__(
            "kraken",
            capabilities={
                "place_order": True,
                "cancel_order": True,
                "get_balance": True,
                "get_trades": True,
            },
        )
        self._primary_url = (primary_url or _DEFAULT_PRIMARY_URL or "").strip()
        self._paper_url = (paper_url or _DEFAULT_PAPER_URL or "").strip()
        self._timeout = timeout if timeout is not None else _DEFAULT_TIMEOUT
        self._session_manager = session_manager

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
        headers = await self._request_headers(account_id)
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                url,
                json=dict(payload),
                headers=headers,
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
        headers = await self._request_headers(account_id)
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(
                url,
                json=payload,
                headers=headers,
            )
            response.raise_for_status()
            try:
                return response.json()
            except ValueError:  # pragma: no cover - defensive guard
                return {}

    async def get_balance(self, account_id: str) -> Mapping[str, Any]:
        payload = await self._fetch_oms_payload(
            path=f"/oms/accounts/{account_id}/balances",
            account_id=account_id,
        )
        return self._normalize_balances(account_id, payload)

    async def get_trades(
        self,
        account_id: str,
        *,
        limit: int = 50,
    ) -> List[Mapping[str, Any]]:
        payload = await self._fetch_oms_payload(
            path=f"/oms/accounts/{account_id}/trades",
            account_id=account_id,
            params={"limit": max(1, min(int(limit), 500))},
        )
        return self._normalize_trades(account_id, payload)

    async def _fetch_oms_payload(
        self,
        *,
        path: str,
        account_id: str,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        if not self._primary_url:
            raise RuntimeError("Kraken OMS URL is not configured")

        url = _join_url(self._primary_url, path)
        headers = await self._request_headers(account_id)
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(url, params=dict(params or {}), headers=headers)
            response.raise_for_status()
            try:
                payload = response.json()
            except ValueError:  # pragma: no cover - defensive guard
                return {}
            if isinstance(payload, Mapping):
                return payload
            if isinstance(payload, list):
                return {"result": payload}
            return {}

    async def _request_headers(self, account_id: str) -> Dict[str, str]:
        token = await self._resolve_session_token(account_id)
        return _account_headers(account_id, token=token)

    async def _resolve_session_token(self, account_id: str) -> str:
        manager = self._ensure_session_manager()
        return await manager.token_for_account(account_id)

    def _ensure_session_manager(self) -> AdminSessionManager:
        manager = self._session_manager
        if manager is None:
            manager = get_default_session_manager()
            self._session_manager = manager
        return manager

    @staticmethod
    def _normalize_balances(account_id: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        balances: Dict[str, float] = {}
        nav: Optional[float] = None
        timestamp: Optional[str] = None

        source: Mapping[str, Any] | Sequence[Any] = payload
        if isinstance(payload.get("result"), Mapping):
            source = payload["result"]
        elif isinstance(payload.get("result"), Sequence):
            source = payload["result"]

        if isinstance(source, Mapping):
            raw_balances = source.get("balances")
            if isinstance(raw_balances, Mapping):
                balances.update(KrakenAdapter._extract_balance_mapping(raw_balances))
            elif isinstance(raw_balances, Sequence):
                balances.update(KrakenAdapter._extract_balance_sequence(raw_balances))
            elif not raw_balances and isinstance(payload.get("result"), Mapping):
                balances.update(
                    KrakenAdapter._extract_balance_mapping(payload["result"])
                )

            nav = KrakenAdapter._to_float(
                source.get("net_asset_value")
                or source.get("nav")
                or source.get("total_value")
                or source.get("equity")
                or source.get("portfolio_value")
            )
            timestamp = KrakenAdapter._normalize_timestamp(source.get("timestamp") or source.get("as_of"))
        elif isinstance(source, Sequence):
            balances.update(KrakenAdapter._extract_balance_sequence(source))

        result: Dict[str, Any] = {"account_id": account_id, "balances": balances}
        if nav is not None:
            result["net_asset_value"] = nav
        if timestamp:
            result["timestamp"] = timestamp
        return result

    @staticmethod
    def _extract_balance_mapping(source: Mapping[str, Any]) -> Dict[str, float]:
        parsed: Dict[str, float] = {}
        for asset, value in source.items():
            amount = KrakenAdapter._to_float(value)
            if amount is None:
                continue
            parsed[str(asset).upper()] = amount
        return parsed

    @staticmethod
    def _extract_balance_sequence(source: Sequence[Any]) -> Dict[str, float]:
        parsed: Dict[str, float] = {}
        for entry in source:
            if not isinstance(entry, Mapping):
                continue
            asset = entry.get("asset") or entry.get("currency") or entry.get("symbol")
            if not asset:
                continue
            amount = (
                KrakenAdapter._to_float(entry.get("balance"))
                or KrakenAdapter._to_float(entry.get("amount"))
                or KrakenAdapter._to_float(entry.get("available"))
            )
            if amount is None:
                continue
            parsed[str(asset).upper()] = amount
        return parsed

    @staticmethod
    def _normalize_trades(account_id: str, payload: Mapping[str, Any]) -> List[Mapping[str, Any]]:
        records: Sequence[Any]
        if isinstance(payload.get("trades"), Sequence):
            records = payload["trades"]  # type: ignore[assignment]
        elif isinstance(payload.get("result"), Mapping) and isinstance(payload["result"].get("trades"), Sequence):
            records = payload["result"]["trades"]  # type: ignore[assignment]
        elif isinstance(payload.get("result"), Sequence):
            records = payload["result"]  # type: ignore[assignment]
        else:
            records = []

        normalized: List[Dict[str, Any]] = []
        for entry in records:
            if not isinstance(entry, Mapping):
                continue
            record: Dict[str, Any] = {"account_id": account_id}
            record["trade_id"] = entry.get("trade_id") or entry.get("id") or entry.get("txid") or entry.get("ordertxid")
            record["order_id"] = entry.get("order_id") or entry.get("client_id") or entry.get("ordertxid")
            symbol = entry.get("instrument") or entry.get("symbol") or entry.get("pair")
            if symbol is not None:
                record["instrument_id"] = str(symbol).replace("/", "-").upper()
            side = entry.get("side") or entry.get("type")
            if side is not None:
                record["side"] = str(side).lower()
            price = KrakenAdapter._to_float(entry.get("price") or entry.get("avg_price") or entry.get("cost") or entry.get("trade_price"))
            if price is not None:
                record["price"] = price
            quantity = KrakenAdapter._to_float(entry.get("quantity") or entry.get("volume") or entry.get("qty"))
            if quantity is not None:
                record["quantity"] = quantity
            fee = KrakenAdapter._to_float(entry.get("fee") or entry.get("fees") or entry.get("commission"))
            record["fee"] = fee if fee is not None else 0.0
            pnl = KrakenAdapter._to_float(entry.get("pnl") or entry.get("realized_pnl") or entry.get("realizedProfit"))
            record["pnl"] = pnl if pnl is not None else 0.0
            liquidity = entry.get("liquidity") or entry.get("role")
            if liquidity is not None:
                record["liquidity"] = str(liquidity).lower()
            timestamp = KrakenAdapter._normalize_timestamp(
                entry.get("timestamp")
                or entry.get("time")
                or entry.get("executed")
                or entry.get("execution_timestamp")
            )
            if timestamp:
                record["timestamp"] = timestamp
            record["raw"] = dict(entry)
            normalized.append(record)
        return normalized

    @staticmethod
    def _normalize_timestamp(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value)
            except ValueError:
                try:
                    parsed = datetime.fromtimestamp(float(value), tz=timezone.utc)
                except (TypeError, ValueError):
                    return value
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
        if isinstance(value, (int, float)):
            try:
                parsed = datetime.fromtimestamp(float(value), tz=timezone.utc)
            except (OSError, OverflowError, ValueError):  # pragma: no cover - defensive guard
                return None
            return parsed.isoformat()
        return None

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

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
