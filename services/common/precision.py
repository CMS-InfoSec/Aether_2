"""Precision metadata client shared by services that snap Kraken orders."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

import httpx
import inspect

# Module-level logger for metadata refresh diagnostics.
logger = logging.getLogger(__name__)


class PrecisionMetadataUnavailable(RuntimeError):
    """Raised when precision metadata for a symbol cannot be resolved."""


class PrecisionMetadataProvider:
    """Fetch and cache Kraken precision metadata for reuse across services."""

    def __init__(
        self,
        *,
        fetcher: Callable[[], Mapping[str, Any]]
        | Callable[[], Awaitable[Mapping[str, Any]]]
        | None = None,
        refresh_interval: float = 300.0,
        timeout: float = 2.5,
        time_source: Callable[[], float] = time.monotonic,
    ) -> None:
        self._fetcher = fetcher or self._default_fetcher
        self._refresh_interval = max(float(refresh_interval), 0.0)
        self._timeout = max(float(timeout), 0.0)
        self._time_source = time_source
        self._lock = threading.Lock()
        self._cache: Dict[str, Dict[str, float]] = {}
        self._last_refresh: float = 0.0
        self._refresh_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def get(self, symbol: str) -> Optional[Dict[str, float]]:
        """Return precision metadata for ``symbol`` if available."""

        normalized = _normalize_symbol(symbol)
        if not normalized:
            return None
        await self._maybe_refresh()
        with self._lock:
            entry = self._cache.get(normalized)
            return dict(entry) if entry else None

    async def require(self, symbol: str) -> Dict[str, float]:
        """Return precision metadata for ``symbol`` or raise."""

        metadata = await self.get(symbol)
        if metadata is None:
            raise PrecisionMetadataUnavailable(f"Precision metadata unavailable for {symbol}")
        return metadata

    async def refresh(self, *, force: bool = False) -> None:
        """Force a metadata refresh regardless of cache age if requested."""

        if not force and not self._needs_refresh():
            return
        async with self._refresh_lock:
            if not force and not self._needs_refresh():
                return
            try:
                payload = await self._call_fetcher()
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.warning("Failed to fetch Kraken precision metadata: %s", exc)
                return

            parsed = _parse_asset_pairs(payload)
            if not parsed:
                logger.warning("Received empty Kraken precision metadata payload")
                return

            with self._lock:
                self._cache = parsed
                self._last_refresh = self._time_source()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _needs_refresh(self) -> bool:
        if not self._cache:
            return True
        age = self._time_source() - self._last_refresh
        return age >= self._refresh_interval

    async def _maybe_refresh(self) -> None:
        if self._needs_refresh():
            await self.refresh(force=False)

    async def _call_fetcher(self) -> Mapping[str, Any]:
        fetcher = self._fetcher
        if inspect.iscoroutinefunction(fetcher):
            result = await fetcher()
        else:
            result = await asyncio.to_thread(fetcher)
        if isinstance(result, Mapping):
            return result
        return {}

    async def _default_fetcher(self) -> Mapping[str, Any]:
        url = "https://api.kraken.com/0/public/AssetPairs"
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.json()
        if isinstance(payload, Mapping):
            result = payload.get("result")
            if isinstance(result, Mapping):
                return result
        return {}


def _normalize_symbol(symbol: str) -> str:
    token = (symbol or "").replace("/", "-").strip().upper()
    return token


_BASE_ALIASES: Dict[str, str] = {
    "XBT": "BTC",
    "XXBT": "BTC",
    "XXBTZ": "BTC",
    "XETH": "ETH",
    "XETC": "ETC",
    "XXDG": "DOGE",
    "XDG": "DOGE",
}
_QUOTE_ALIASES: Dict[str, str] = {"ZUSD": "USD", "USD": "USD"}


def _normalize_asset(symbol: str, *, is_quote: bool) -> str:
    token = (symbol or "").strip().upper()
    if not token:
        return ""
    aliases = _QUOTE_ALIASES if is_quote else _BASE_ALIASES
    direct = aliases.get(token)
    if direct:
        return direct

    trimmed = token
    while len(trimmed) > 3 and trimmed.endswith(("X", "Z")):
        trimmed = trimmed[:-1]
    while len(trimmed) > 3 and trimmed.startswith(("X", "Z")):
        trimmed = trimmed[1:]

    return aliases.get(trimmed, trimmed)


def _normalize_instrument(entry: Mapping[str, Any]) -> str:
    wsname = entry.get("wsname")
    if isinstance(wsname, str) and "/" in wsname:
        base_part, quote_part = wsname.split("/", 1)
        base = _normalize_asset(base_part, is_quote=False)
        quote = _normalize_asset(quote_part, is_quote=True)
        if base and quote == "USD":
            return f"{base}-USD"

    altname = entry.get("altname")
    if isinstance(altname, str):
        cleaned = altname.replace("/", "").upper()
        if cleaned.endswith("USD") and len(cleaned) > 3:
            base = _normalize_asset(cleaned[:-3], is_quote=False)
            if base:
                return f"{base}-USD"

    base = _normalize_asset(str(entry.get("base") or ""), is_quote=False)
    quote = _normalize_asset(str(entry.get("quote") or ""), is_quote=True)
    if base and quote == "USD":
        return f"{base}-USD"
    return ""


def _step_from_metadata(
    metadata: Mapping[str, Any],
    step_keys: tuple[str, ...],
    decimal_keys: tuple[str, ...],
) -> Optional[Decimal]:
    for key in step_keys:
        value = metadata.get(key)
        if value is None:
            continue
        try:
            step = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            continue
        if step > 0:
            return step

    for key in decimal_keys:
        value = metadata.get(key)
        if value is None:
            continue
        try:
            decimals = int(value)
        except (TypeError, ValueError):
            continue
        if decimals < 0:
            continue
        return Decimal("1") / (Decimal("10") ** decimals)
    return None


def _parse_asset_pairs(payload: Mapping[str, Any]) -> Dict[str, Dict[str, float]]:
    if isinstance(payload.get("result"), Mapping):
        payload = payload["result"]  # type: ignore[assignment]

    parsed: Dict[str, Dict[str, float]] = {}
    for entry in payload.values():
        if not isinstance(entry, Mapping):
            continue
        instrument = _normalize_instrument(entry)
        if not instrument:
            continue
        tick = _step_from_metadata(
            entry,
            ("tick_size", "price_increment"),
            ("pair_decimals",),
        )
        lot = _step_from_metadata(
            entry,
            ("lot_step", "step_size"),
            ("lot_decimals",),
        )
        if tick is None or lot is None:
            continue
        parsed[instrument] = {"tick": float(tick), "lot": float(lot)}
    return parsed


precision_provider = PrecisionMetadataProvider()

__all__ = [
    "PrecisionMetadataProvider",
    "PrecisionMetadataUnavailable",
    "precision_provider",
]

