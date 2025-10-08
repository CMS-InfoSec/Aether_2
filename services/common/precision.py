"""Precision metadata client shared by services that snap Kraken orders."""

from __future__ import annotations

import asyncio

import logging
import threading
import time
from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal, InvalidOperation

from typing import Any, Dict, Optional, Tuple, cast


import httpx
import inspect

# Module-level logger for metadata refresh diagnostics.
logger = logging.getLogger(__name__)


def _coerce_mapping(value: object) -> Dict[str, Any]:
    """Return a mapping copy when the payload resembles the expected shape."""

    if isinstance(value, Mapping):
        # Ensure callers can safely mutate the result without affecting the source.
        return dict(value)

    logger.warning("Precision metadata fetcher returned non-mapping payload: %r", value)
    return {}


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
        raw_fetcher = fetcher or self._default_fetcher
        self._fetcher: Callable[[], Awaitable[Mapping[str, Any]]]
        if inspect.iscoroutinefunction(raw_fetcher):
            async_fetcher = cast(Callable[[], Awaitable[Mapping[str, Any]]], raw_fetcher)

            async def _async_fetcher() -> Mapping[str, Any]:
                result = await async_fetcher()
                return _coerce_mapping(result)

            self._fetcher = _async_fetcher
        else:
            sync_fetcher = cast(Callable[[], Mapping[str, Any]], raw_fetcher)

            async def _threaded_fetcher() -> Mapping[str, Any]:
                result = await asyncio.to_thread(sync_fetcher)
                return _coerce_mapping(result)

            self._fetcher = _threaded_fetcher
        self._refresh_interval = max(float(refresh_interval), 0.0)
        self._timeout = max(float(timeout), 0.0)
        self._time_source = time_source
        self._lock = threading.Lock()

        self._cache: Dict[str, Dict[str, Any]] = {}

        self._aliases: Dict[str, str] = {}
        self._last_refresh: float = 0.0
        self._refresh_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get(self, symbol: str) -> Optional[Dict[str, Decimal | str]]:
        """Return precision metadata for ``symbol`` if available."""

        normalized = _normalize_symbol(symbol)
        if not normalized:
            return None

        await self._maybe_refresh()

        with self._lock:
            key = self._aliases.get(normalized, normalized)
            entry = self._cache.get(key)

            return dict(entry) if entry else None


    async def require(self, symbol: str) -> Dict[str, Decimal | str]:

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


            parsed, aliases = _parse_asset_pairs(payload)
            if not parsed:

                logger.warning("Received empty Kraken precision metadata payload")
                return

            with self._lock:

                self._cache = parsed

                self._aliases = aliases
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
        result = await self._fetcher()
        return _coerce_mapping(result)

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
_QUOTE_ALIASES: Dict[str, str] = {
    "ZUSD": "USD",
    "USD": "USD",
    "ZUSDT": "USDT",
    "USDT": "USDT",
    "ZEUR": "EUR",
    "EUR": "EUR",
    "ZGBP": "GBP",
    "GBP": "GBP",
    "ZCAD": "CAD",
    "CAD": "CAD",
    "ZCHF": "CHF",
    "CHF": "CHF",
    "ZJPY": "JPY",
    "JPY": "JPY",
    "ZUSDC": "USDC",
    "USDC": "USDC",
}


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



def _sanitize_pair(value: str, entry: Mapping[str, Any]) -> str:
    token = (value or "").strip().upper()
    if not token:
        return ""
    if "/" in token:
        base_part, quote_part = token.split("/", 1)
        base = base_part.strip()
        quote = quote_part.strip()
        if base and quote:
            return f"{base}/{quote}"
        return ""

    base = str(entry.get("base") or "").strip().upper()
    quote = str(entry.get("quote") or "").strip().upper()
    if base and quote and token == f"{base}{quote}":
        return f"{base}/{quote}"

    quote_candidates = [quote]
    normalized_quote = _normalize_asset(quote, is_quote=True)
    if normalized_quote and normalized_quote != quote:
        quote_candidates.append(normalized_quote)

    for candidate in quote_candidates:
        if candidate and token.endswith(candidate) and len(token) > len(candidate):
            base_part = token[: -len(candidate)]
            if base_part:
                return f"{base_part}/{candidate}"
    return ""


def _normalize_instrument(entry: Mapping[str, Any]) -> str:
    wsname = entry.get("wsname")
    if isinstance(wsname, str):
        pair = _sanitize_pair(wsname, entry)
        if pair:
            return pair

    altname = entry.get("altname")
    if isinstance(altname, str):
        pair = _sanitize_pair(altname, entry)
        if pair:
            return pair

    base = str(entry.get("base") or "").strip().upper()
    quote = str(entry.get("quote") or "").strip().upper()
    if base and quote:
        return f"{base}/{quote}"
    return ""



def _alias_candidates(entry: Mapping[str, Any], native_pair: str) -> Tuple[str, ...]:
    base_native, quote_native = native_pair.split("/", 1)

    bases = {base_native}
    quotes = {quote_native}

    base_raw = str(entry.get("base") or "").strip().upper()
    if base_raw:
        bases.add(base_raw)
        normalized_base = _normalize_asset(base_raw, is_quote=False)
        if normalized_base:
            bases.add(normalized_base)

    quote_raw = str(entry.get("quote") or "").strip().upper()
    if quote_raw:
        quotes.add(quote_raw)
        normalized_quote = _normalize_asset(quote_raw, is_quote=True)
        if normalized_quote:
            quotes.add(normalized_quote)

    altname = entry.get("altname")
    if isinstance(altname, str):
        cleaned = altname.strip().upper()
        if "/" in cleaned:
            alt_base, alt_quote = cleaned.split("/", 1)
            if alt_base:
                bases.add(alt_base)
                normalized = _normalize_asset(alt_base, is_quote=False)
                if normalized:
                    bases.add(normalized)
            if alt_quote:
                quotes.add(alt_quote)
                normalized = _normalize_asset(alt_quote, is_quote=True)
                if normalized:
                    quotes.add(normalized)
        else:
            for candidate in list(quotes):
                if candidate and cleaned.endswith(candidate) and len(cleaned) > len(candidate):
                    alt_base = cleaned[: -len(candidate)]
                    if alt_base:
                        bases.add(alt_base)
                        normalized = _normalize_asset(alt_base, is_quote=False)
                        if normalized:
                            bases.add(normalized)

    aliases = set()
    for base in bases:
        for quote in quotes:
            if not base or not quote:
                continue
            aliases.add(f"{base}-{quote}")
            aliases.add(f"{base}/{quote}")
            aliases.add(f"{base}{quote}")

    return tuple(sorted({alias for alias in aliases if alias}))


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



def _parse_asset_pairs(payload: Mapping[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    source: Mapping[str, Any] = payload
    result_section = payload.get("result")
    if isinstance(result_section, Mapping):
        source = result_section

    parsed: Dict[str, Dict[str, Any]] = {}

    aliases: Dict[str, str] = {}
    for entry in source.values():
        if not isinstance(entry, Mapping):
            continue

        native_pair = _normalize_instrument(entry)
        if not native_pair:
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

        key = _normalize_symbol(native_pair)
        metadata = {
            "tick": tick,
            "lot": lot,
            "native_pair": native_pair,
        }
        parsed[key] = metadata
        aliases.setdefault(key, key)
        for alias in _alias_candidates(entry, native_pair):
            alias_key = _normalize_symbol(alias)
            if alias_key and alias_key not in aliases:
                aliases[alias_key] = key

    return parsed, aliases


precision_provider = PrecisionMetadataProvider()

__all__ = [
    "PrecisionMetadataProvider",
    "PrecisionMetadataUnavailable",
    "precision_provider",
]

