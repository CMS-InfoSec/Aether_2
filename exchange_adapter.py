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


from shared.common_bootstrap import ensure_httpx_ready

httpx = ensure_httpx_ready()

from auth.session_client import AdminSessionManager, get_default_session_manager

from metrics import get_request_id
from shared.spot import is_spot_symbol, normalize_spot_symbol

logger = logging.getLogger(__name__)

_DEFAULT_PRIMARY_URL = os.getenv("OMS_SERVICE_URL", "http://oms-service")
_DEFAULT_PAPER_URL = os.getenv("PAPER_OMS_SERVICE_URL", "http://paper-oms-service")
_DEFAULT_TIMEOUT = float(os.getenv("OMS_REQUEST_TIMEOUT", "1.0"))


class ExchangeError(RuntimeError):
    """Base class for recoverable exchange adapter failures."""


class ExchangeTimeoutError(ExchangeError):
    """Raised when a request to the OMS exceeds the configured timeout."""


class ExchangeRateLimitError(ExchangeError):
    """Raised when the OMS responds with a rate limit status."""


class ExchangeValidationError(ExchangeError):
    """Raised when the OMS rejects a request due to payload validation issues."""


class ExchangeAPIError(ExchangeError):
    """Raised when the OMS returns an unexpected HTTP error."""


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


def _normalise_error_detail(detail: object) -> Optional[str]:
    if detail is None:
        return None
    if isinstance(detail, str):
        return detail.strip() or None
    if isinstance(detail, Mapping):
        for key in ("error", "errors", "detail", "message"):
            if key not in detail:
                continue
            normalised = _normalise_error_detail(detail[key])  # type: ignore[index]
            if normalised:
                return normalised
        return ", ".join(
            f"{str(key)}={_normalise_error_detail(value) or value}"
            for key, value in detail.items()
        )
    if isinstance(detail, Sequence) and not isinstance(detail, (bytes, bytearray)):
        fragments = [
            fragment
            for fragment in (_normalise_error_detail(item) for item in detail)
            if fragment
        ]
        if fragments:
            return ", ".join(fragments)
        return None
    return str(detail)


def _extract_error_detail(response: httpx.Response) -> Optional[str]:
    try:
        payload = response.json()
    except Exception:
        payload = None
    detail = _normalise_error_detail(payload)
    if detail:
        return detail
    text = getattr(response, "text", "")
    if isinstance(text, str):
        text = text.strip()
        if text:
            return text
    return None


def _raise_exchange_error(response: httpx.Response, *, operation: str) -> None:
    status = getattr(response, "status_code", None)
    if status is None or status < 400:
        return
    detail = _extract_error_detail(response)
    suffix = f": {detail}" if detail else ""
    if status == 429:
        raise ExchangeRateLimitError(
            f"Kraken adapter rate limited {operation}{suffix}"
        )
    if status in {400, 422}:
        message_suffix = suffix or ": invalid request"
        raise ExchangeValidationError(
            f"Kraken adapter rejected {operation}{message_suffix}"
        )
    raise ExchangeAPIError(
        f"Kraken adapter request failed for {operation}: HTTP {status}{suffix}"
    )


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

        symbol_candidate = (
            payload.get("symbol")
            or payload.get("instrument")
            or payload.get("instrument_id")
            or payload.get("pair")
        )
        normalized_symbol = normalize_spot_symbol(symbol_candidate)
        if not normalized_symbol or not is_spot_symbol(normalized_symbol):
            raise ValueError("KrakenAdapter only supports spot market instruments")

        normalized_payload = dict(payload)
        normalized_payload["symbol"] = normalized_symbol

        response = await self._perform_request(
            "POST",
            base_url=base_url,
            path="/oms/place",
            account_id=account_id,
            json_payload=normalized_payload,
        )
        payload_obj = self._response_payload(response)
        if isinstance(payload_obj, Mapping):
            return payload_obj
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
        payload: Dict[str, Any] = {
            "account_id": account_id,
            "client_id": client_id,
        }
        if exchange_order_id:
            payload["exchange_order_id"] = exchange_order_id
        response = await self._perform_request(
            "POST",
            base_url=self._primary_url,
            path="/oms/cancel",
            account_id=account_id,
            json_payload=payload,
        )
        payload_obj = self._response_payload(response)
        if isinstance(payload_obj, Mapping):
            return payload_obj
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
        response = await self._perform_request(
            "GET",
            base_url=self._primary_url,
            path=path,
            account_id=account_id,
            params=params,
        )
        payload = self._response_payload(response)
        if isinstance(payload, Mapping):
            return payload
        if isinstance(payload, list):
            return {"result": payload}
        return {}

    async def _perform_request(
        self,
        method: str,
        *,
        base_url: str,
        path: str,
        account_id: str,
        json_payload: Optional[Mapping[str, Any]] = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> httpx.Response:
        if not base_url:
            raise RuntimeError("Kraken OMS URL is not configured")

        url = _join_url(base_url, path)
        headers = await self._request_headers(account_id)
        request_kwargs: Dict[str, Any] = {"headers": headers}
        if json_payload is not None:
            request_kwargs["json"] = json_payload
        if params is not None:
            request_kwargs["params"] = dict(params)
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                request = getattr(client, method.lower())
                response = await request(url, **request_kwargs)
        except httpx.TimeoutException as exc:
            raise ExchangeTimeoutError(
                f"Kraken adapter request timed out after {self._timeout:.2f}s: {method.upper()} {path}"
            ) from exc
        except httpx.HTTPError as exc:
            raise ExchangeAPIError(
                f"Kraken adapter request failed for {method.upper()} {path}: {exc}"
            ) from exc

        _raise_exchange_error(response, operation=f"{method.upper()} {path}")
        return response

    @staticmethod
    def _response_payload(response: httpx.Response) -> object:
        try:
            return response.json()
        except ValueError:  # pragma: no cover - defensive guard
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


def _resolve_multi_exchange_enabled() -> bool:
    flag = os.getenv("ENABLE_MULTI_EXCHANGE_ROUTING", "")
    return flag.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_url(candidate: Optional[str], env_var: str, default: str = "") -> str:
    if candidate:
        return str(candidate).strip()
    env_value = os.getenv(env_var, default)
    return env_value.strip()


class _SpotAdapter(ExchangeAdapter):
    """Common behaviour shared by spot adapters with stubbed trading flows."""

    def __init__(
        self,
        name: str,
        *,
        rest_url: Optional[str] = None,
        websocket_url: Optional[str] = None,
    ) -> None:
        super().__init__(
            name,
            capabilities={
                "place_order": True,
                "cancel_order": True,
            },
        )
        self._rest_url = rest_url or ""
        self._websocket_url = websocket_url or ""
        self._connected = False

    def supports(self, operation: str) -> bool:
        if operation in {"place_order", "cancel_order"} and not _resolve_multi_exchange_enabled():
            return False
        return super().supports(operation)

    async def connect(self) -> Mapping[str, Any]:
        self._connected = True
        return {
            "exchange": self.name,
            "status": "connected",
            "rest_url": self._rest_url or None,
            "websocket_url": self._websocket_url or None,
            "trading_enabled": _resolve_multi_exchange_enabled(),
        }

    async def fetch_orderbook(
        self,
        symbol: str,
        *,
        depth: int = 10,
    ) -> Mapping[str, Any]:
        normalized = self._require_spot_symbol(symbol)
        return {
            "exchange": self.name,
            "symbol": normalized,
            "depth": max(1, int(depth)),
            "bids": [],
            "asks": [],
            "as_of": datetime.now(timezone.utc).isoformat(),
        }

    async def submit_order(
        self,
        account_id: str,
        payload: Mapping[str, Any],
        *,
        shadow: bool = False,
    ) -> Mapping[str, Any]:
        normalized = self._require_spot_symbol(
            payload.get("symbol")
            or payload.get("instrument")
            or payload.get("instrument_id")
            or payload.get("pair")
        )
        client_id = str(
            payload.get("client_id")
            or payload.get("client_order_id")
            or uuid4()
        )
        ack: Dict[str, Any] = {
            "exchange": self.name,
            "account_id": account_id,
            "symbol": normalized,
            "client_order_id": client_id,
            "shadow": bool(shadow),
        }
        if _resolve_multi_exchange_enabled():
            ack.update({"status": "accepted", "note": "stubbed_execution"})
        else:
            ack.update({"status": "noop", "reason": "multi_exchange_routing_disabled"})
        return ack

    async def cancel_order(
        self,
        account_id: str,
        client_id: str,
        *,
        exchange_order_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        ack: Dict[str, Any] = {
            "exchange": self.name,
            "account_id": account_id,
            "client_order_id": client_id,
        }
        if exchange_order_id:
            ack["exchange_order_id"] = exchange_order_id
        if _resolve_multi_exchange_enabled():
            ack.update({"status": "accepted", "note": "stubbed_cancellation"})
        else:
            ack.update({"status": "noop", "reason": "multi_exchange_routing_disabled"})
        return ack

    async def place_order(
        self,
        account_id: str,
        payload: Mapping[str, Any],
        *,
        shadow: bool = False,
    ) -> Mapping[str, Any]:
        return await self.submit_order(account_id, payload, shadow=shadow)

    async def get_balance(self, account_id: str) -> Mapping[str, Any]:
        return {
            "exchange": self.name,
            "account_id": account_id,
            "balances": {},
        }

    async def get_trades(
        self,
        account_id: str,
        *,
        limit: int = 50,
    ) -> List[Mapping[str, Any]]:
        return []

    async def status(self) -> Mapping[str, Any]:
        return {
            "exchange": self.name,
            "available": True,
            "connected": self._connected,
            "rest_url": self._rest_url or None,
            "websocket_url": self._websocket_url or None,
            "trading_enabled": _resolve_multi_exchange_enabled(),
        }

    @staticmethod
    def _require_spot_symbol(symbol: object) -> str:
        normalized = normalize_spot_symbol(symbol)
        if not normalized or not is_spot_symbol(normalized):
            raise ValueError("Only USD spot market instruments are supported")
        return normalized


class BinanceAdapter(_SpotAdapter):
    """Binance spot adapter aligned with Kraken spot interface."""

    def __init__(
        self,
        *,
        rest_url: Optional[str] = None,
        websocket_url: Optional[str] = None,
    ) -> None:
        super().__init__(
            "binance",
            rest_url=_resolve_url(rest_url, "BINANCE_REST_URL", "https://api.binance.com"),
            websocket_url=_resolve_url(
                websocket_url,
                "BINANCE_WEBSOCKET_URL",
                "wss://stream.binance.com:9443/ws",
            ),
        )


class CoinbaseAdapter(_SpotAdapter):
    """Coinbase spot adapter aligned with Kraken spot interface."""

    def __init__(
        self,
        *,
        rest_url: Optional[str] = None,
        websocket_url: Optional[str] = None,
    ) -> None:
        super().__init__(
            "coinbase",
            rest_url=_resolve_url(
                rest_url,
                "COINBASE_REST_URL",
                "https://api.exchange.coinbase.com",
            ),
            websocket_url=_resolve_url(
                websocket_url,
                "COINBASE_WEBSOCKET_URL",
                "wss://ws-feed.exchange.coinbase.com",
            ),
        )


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
    "ExchangeError",
    "ExchangeTimeoutError",
    "ExchangeRateLimitError",
    "ExchangeValidationError",
    "ExchangeAPIError",
    "ExchangeAdapter",
    "KrakenAdapter",
    "BinanceAdapter",
    "CoinbaseAdapter",
    "get_exchange_adapter",
    "get_exchange_adapters_status",
]
