"""Precision metadata client shared by services that snap Kraken orders."""

from __future__ import annotations

import logging
import threading
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, Mapping, Optional

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
        fetcher: Callable[[], Mapping[str, Any]] | None = None,
        refresh_interval: float = 300.0,
        timeout: float = 2.5,
        time_source: Callable[[], float] = time.monotonic,
    ) -> None:
        self._fetcher = fetcher or self._default_fetcher
        self._refresh_interval = max(float(refresh_interval), 0.0)
        self._timeout = max(float(timeout), 0.0)
        self._time_source = time_source
        self._lock = threading.Lock()
        # Cache keyed by Kraken native pair identifier (e.g. "ETH/USDT").
        self._cache: Dict[str, Dict[str, float]] = {}
        # Mapping of normalized client symbols to cached native identifiers.
        self._aliases: Dict[str, str] = {}
        self._last_refresh: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get(self, symbol: str) -> Optional[Dict[str, float]]:
        """Return precision metadata for ``symbol`` if available."""

        self._maybe_refresh()
        with self._lock:
            native = self._resolve_native_locked(symbol)
            if not native:
                return None
            entry = self._cache.get(native)
            return dict(entry) if entry else None

    def require(self, symbol: str) -> Dict[str, float]:
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
            payload = self._fetcher()
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
            self.refresh(force=True)

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
        with httpx.Client(timeout=self._timeout) as client:
            response = client.get(url)
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


def _normalize_instrument(entry: Mapping[str, Any]) -> Optional[tuple[str, str, str]]:
    base = _normalize_asset(str(entry.get("base") or ""), is_quote=False)
    quote = _normalize_asset(str(entry.get("quote") or ""), is_quote=True)

    native: Optional[str] = None

    wsname = entry.get("wsname")
    if isinstance(wsname, str):
        candidate = wsname.strip().upper()
        if candidate:
            native = candidate
            if "/" in candidate:
                base_part, quote_part = candidate.split("/", 1)
                parsed_base = _normalize_asset(base_part, is_quote=False)
                parsed_quote = _normalize_asset(quote_part, is_quote=True)
                base = parsed_base or base
                quote = parsed_quote or quote

    if native is None:
        altname = entry.get("altname")
        if isinstance(altname, str):
            candidate = altname.strip().upper()
            if candidate:
                native = candidate

    if native is None and base and quote:
        native = f"{base}/{quote}"

    if not native or not base or not quote:
        return None

    return native, base, quote


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


def _parse_asset_pairs(
    payload: Mapping[str, Any]
) -> tuple[Dict[str, Dict[str, float]], Dict[str, str]]:
    if isinstance(payload.get("result"), Mapping):
        payload = payload["result"]  # type: ignore[assignment]

    parsed: Dict[str, Dict[str, float]] = {}
    aliases: Dict[str, str] = {}
    for entry in payload.values():
        if not isinstance(entry, Mapping):
            continue
        normalized = _normalize_instrument(entry)
        if not normalized:
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
        native_key = native.strip().upper()
        parsed[native_key] = {"tick": float(tick), "lot": float(lot)}

        alias_candidates = {
            native_key,
            native_key.replace("/", "-"),
            f"{base}-{quote}",
            f"{base}/{quote}",
            f"{base}{quote}",
        }

        wsname = entry.get("wsname")
        if isinstance(wsname, str):
            alias_candidates.add(wsname.strip().upper())

        altname = entry.get("altname")
        if isinstance(altname, str):
            alias_candidates.add(altname.strip().upper())

        for candidate in alias_candidates:
            normalized_alias = _normalize_symbol(candidate)
            if normalized_alias:
                aliases.setdefault(normalized_alias, native_key)

    return parsed, aliases


precision_provider = PrecisionMetadataProvider()

__all__ = [
    "PrecisionMetadataProvider",
    "PrecisionMetadataUnavailable",
    "precision_provider",
]

