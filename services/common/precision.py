"""Precision metadata client shared by services that snap Kraken orders."""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import time
from decimal import Decimal, InvalidOperation

from typing import Any, Callable, Dict, Mapping, Optional, Tuple


import httpx

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
        raw_fetcher = fetcher or self._default_fetcher
        self._fetcher: Callable[[], Awaitable[Mapping[str, Any]]]
        if inspect.iscoroutinefunction(raw_fetcher):

            async def _async_fetcher() -> Mapping[str, Any]:
                result = raw_fetcher()
                if inspect.isawaitable(result):
                    return await result
                return result

            self._fetcher = _async_fetcher
        else:

            def _call_fetcher() -> Mapping[str, Any] | Awaitable[Mapping[str, Any]]:
                return raw_fetcher()

            async def _threaded_fetcher() -> Mapping[str, Any]:
                result = await asyncio.to_thread(_call_fetcher)
                if inspect.isawaitable(result):
                    return await result
                return result

            self._fetcher = _threaded_fetcher
        self._refresh_interval = max(float(refresh_interval), 0.0)
        self._timeout = max(float(timeout), 0.0)
        self._time_source = time_source
        self._lock = threading.Lock()

        self._cache: Dict[str, Dict[str, Any]] = {}

        self._aliases: Dict[str, str] = {}
        self._last_refresh: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Return precision metadata for ``symbol`` if available."""

        self._maybe_refresh()
        with self._lock:

            key = self._aliases.get(normalized, normalized)
            entry = self._cache.get(key)
            if entry is None and normalized in self._cache:
                entry = self._cache.get(normalized)

            return dict(entry) if entry else None

    def require(self, symbol: str) -> Dict[str, Any]:
        """Return precision metadata for ``symbol`` or raise."""

        metadata = self.get(symbol)
        if metadata is None:
            raise PrecisionMetadataUnavailable(f"Precision metadata unavailable for {symbol}")
        return metadata


    def resolve_native(self, symbol: str) -> Optional[str]:
        """Resolve a client-facing symbol to the cached native pair identifier."""

        self._maybe_refresh()
        with self._lock:
            return self._resolve_native_locked(symbol)

    def get_native(self, native_symbol: str) -> Optional[Dict[str, float]]:
        """Return precision metadata for a native Kraken pair identifier."""

        key = (native_symbol or "").strip().upper()
        if not key:
            return None
        self._maybe_refresh()
        with self._lock:
            entry = self._cache.get(key)
        return dict(entry) if entry else None

    def require_native(self, native_symbol: str) -> Dict[str, float]:
        """Return precision metadata for a native Kraken pair identifier or raise."""

        metadata = self.get_native(native_symbol)
        if metadata is None:
            raise PrecisionMetadataUnavailable(
                f"Precision metadata unavailable for native pair {native_symbol}"
            )
        return metadata

    def refresh(self, *, force: bool = False) -> None:

        """Force a metadata refresh regardless of cache age if requested."""

        if not force and not self._needs_refresh():
            return
        try:
            payload = await self._fetcher()
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

    def _maybe_refresh(self) -> None:
        if self._needs_refresh():
            self._refresh_sync(force=True)

    def _refresh_sync(self, *, force: bool) -> None:
        try:
            asyncio.run(self.refresh(force=force))
        except RuntimeError as exc:  # pragma: no cover - defensive guard
            if "asyncio.run()" in str(exc):
                raise RuntimeError(
                    "PrecisionMetadataProvider.refresh() cannot be called from an "
                    "active event loop; await refresh() or call within asyncio.to_thread()."
                ) from exc
            raise

    def refresh_sync(self, *, force: bool = False) -> None:
        """Synchronously refresh metadata for callers without an event loop."""

        self._refresh_sync(force=force)


    def _resolve_native_locked(self, symbol: str) -> Optional[str]:
        normalized = _normalize_symbol(symbol)
        if not normalized:
            return None
        native = self._aliases.get(normalized)
        if native:
            return native

        direct = (symbol or "").strip().upper()
        if direct and direct in self._cache:
            return direct

        fallback = normalized.replace("-", "/")
        if fallback in self._cache:
            return fallback
        return None

    def _default_fetcher(self) -> Mapping[str, Any]:

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
    if isinstance(payload.get("result"), Mapping):
        payload = payload["result"]  # type: ignore[assignment]

    parsed: Dict[str, Dict[str, Any]] = {}

    aliases: Dict[str, str] = {}
    for entry in payload.values():
        if not isinstance(entry, Mapping):
            continue

        native_pair = _normalize_instrument(entry)
        if not native_pair:

            continue
        native, base, quote = normalized
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
            "tick": float(tick),
            "lot": float(lot),
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

