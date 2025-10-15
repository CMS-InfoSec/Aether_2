"""Helpers for retrieving account balance state for the portfolio service."""

from __future__ import annotations

import asyncio
import logging
import math
import os
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, MutableMapping, Optional

from shared.common_bootstrap import ensure_httpx_ready

httpx = ensure_httpx_ready()

from exchange_adapter import KrakenAdapter
from services.risk.stablecoin_monitor import StablecoinMonitor, StablecoinStatus, get_global_monitor

LOGGER = logging.getLogger(__name__)

_DEFAULT_CACHE_TTL_SECONDS = 3.0
_DEFAULT_SIM_TIMEOUT_SECONDS = 1.5


def _env_float(name: str, default: float) -> float:
    """Return ``name`` parsed as float when possible, otherwise ``default``."""

    try:
        raw = os.getenv(name)
        if raw is None or not raw.strip():
            return float(default)
        return float(raw)
    except (TypeError, ValueError):
        LOGGER.warning("Invalid value for %s=%r; falling back to %s", name, os.getenv(name), default)
        return float(default)


@dataclass(slots=True)
class BalanceSummary:
    """Aggregated balance metrics used by the portfolio API."""

    available_usd: float
    deployed_usd: float
    total_nav_usd: float
    stablecoin_depeg_bps: float
    source: str
    as_of: Optional[str]
    raw_balances: Mapping[str, float]

    def as_payload(self) -> Mapping[str, Any]:
        return {
            "available_usd": self.available_usd,
            "deployed_usd": self.deployed_usd,
            "total_nav_usd": self.total_nav_usd,
            "stablecoin_depeg_bps": self.stablecoin_depeg_bps,
        }


@dataclass(slots=True)
class _CacheEntry:
    value: BalanceSummary
    expires_at: float


class BalanceRetrievalError(Exception):
    """Raised when the balance reader cannot return a usable snapshot."""

    def __init__(self, message: str, status_code: int = 502) -> None:
        super().__init__(message)
        self.status_code = status_code


class SimStoreClient:
    """Client for the optional simulation store fallback."""

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        resolved_url = base_url or os.getenv("SIM_STORE_URL", "http://sim-store")
        self._base_url = resolved_url.rstrip("/") if resolved_url else ""
        self._timeout = timeout if timeout is not None else _DEFAULT_SIM_TIMEOUT_SECONDS

    async def is_active(self, account_id: Optional[str] = None) -> bool:
        """Return ``True`` when the simulation store is online and active."""

        if not self._base_url:
            return False
        url = f"{self._base_url}/sim/status"
        params = {"account_id": account_id} if account_id else None
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
        except httpx.HTTPError as exc:  # pragma: no cover - network guard
            LOGGER.debug("Failed to query sim status: %s", exc)
            return False
        except ValueError:  # pragma: no cover - payload not JSON
            LOGGER.debug("Simulation status endpoint returned non-JSON payload")
            return False

        if not isinstance(payload, Mapping):
            return False

        accounts = payload.get("accounts")
        if isinstance(accounts, list):
            for entry in accounts:
                if not isinstance(entry, Mapping):
                    continue
                if account_id and entry.get("account_id") != account_id:
                    continue
                if entry.get("active"):
                    return True
            return bool(payload.get("active"))

        return bool(payload.get("active"))

    async def fetch_balances(self, account_id: str) -> Mapping[str, Any]:
        """Return a Kraken-style balance snapshot from the simulation store."""

        if not self._base_url:
            raise BalanceRetrievalError("Simulation store URL not configured", status_code=503)

        params = {"account_id": account_id}
        url = f"{self._base_url}/sim/balances"
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
        except httpx.HTTPStatusError as exc:  # pragma: no cover - network guard
            raise BalanceRetrievalError(
                f"Simulation store returned HTTP {exc.response.status_code}",
                status_code=exc.response.status_code,
            ) from exc
        except httpx.HTTPError as exc:  # pragma: no cover - network guard
            raise BalanceRetrievalError("Simulation store unreachable") from exc
        except ValueError as exc:  # pragma: no cover - payload guard
            raise BalanceRetrievalError("Simulation store returned invalid JSON") from exc

        if not isinstance(payload, Mapping):
            raise BalanceRetrievalError("Simulation store returned unexpected payload")
        return payload


class BalanceReader:
    """Retrieve balances from Kraken while supporting a cached simulation fallback."""

    def __init__(
        self,
        *,
        adapter: Optional[KrakenAdapter] = None,
        sim_client: Optional[SimStoreClient] = None,
        monitor: Optional[StablecoinMonitor] = None,
        cache_ttl_seconds: Optional[float] = None,
    ) -> None:
        self._adapter = adapter or KrakenAdapter()
        self._sim_client = sim_client or SimStoreClient()
        self._monitor = monitor or get_global_monitor()
        ttl = cache_ttl_seconds if cache_ttl_seconds is not None else _env_float(
            "BALANCE_READER_CACHE_TTL", _DEFAULT_CACHE_TTL_SECONDS
        )
        self._cache_ttl = max(float(ttl), 0.0)
        self._cache: MutableMapping[str, _CacheEntry] = {}
        self._locks: MutableMapping[str, asyncio.Lock] = {}
        self._stablecoin_assets = self._discover_stablecoins(self._monitor)

    @staticmethod
    def _discover_stablecoins(monitor: StablecoinMonitor) -> set[str]:
        assets: set[str] = set()
        config = getattr(monitor, "config", None)
        if config is None:  # pragma: no cover - defensive guard
            symbols: Iterable[Any] = ()
        else:
            symbols = getattr(config, "monitored_symbols", ())
        for symbol in symbols:
            if not isinstance(symbol, str):
                continue
            if "-" not in symbol:
                continue
            asset = symbol.split("-", 1)[0].upper()
            assets.add(asset)
        return assets

    def invalidate(self, account_id: Optional[str] = None) -> None:
        """Invalidate cached balances either for ``account_id`` or globally."""

        if account_id is None:
            self._cache.clear()
            return
        self._cache.pop(account_id, None)

    async def get_account_balances(self, account_id: str) -> Mapping[str, Any]:
        """Return a cached balance summary for ``account_id``."""

        if not account_id:
            raise BalanceRetrievalError("account_id must be provided", status_code=422)

        loop = asyncio.get_running_loop()
        entry = self._cache.get(account_id)
        now = loop.time()
        if entry and entry.expires_at > now:
            return entry.value.as_payload()

        lock = self._locks.setdefault(account_id, asyncio.Lock())
        async with lock:
            # Double check inside the lock to avoid refetching.
            entry = self._cache.get(account_id)
            now = loop.time()
            if entry and entry.expires_at > now:
                return entry.value.as_payload()

            summary = await self._refresh(account_id)
            expiry = now + self._cache_ttl if self._cache_ttl else now
            self._cache[account_id] = _CacheEntry(summary, expiry)
            return summary.as_payload()

    async def _refresh(self, account_id: str) -> BalanceSummary:
        snapshot = await self._load_snapshot(account_id)
        balances = self._extract_balances(snapshot)
        nav = self._extract_nav(snapshot)
        timestamp = self._extract_timestamp(snapshot)

        cash_usd, stablecoin_usd, depeg_bps = self._compute_cash_equivalents(balances)
        available = cash_usd + stablecoin_usd
        total_nav = nav if nav > 0 else available
        deployed = max(total_nav - available, 0.0)

        return BalanceSummary(
            available_usd=self._sanitize_float(available),
            deployed_usd=self._sanitize_float(deployed),
            total_nav_usd=self._sanitize_float(total_nav),
            stablecoin_depeg_bps=depeg_bps,
            source=snapshot.get("source", "kraken"),
            as_of=timestamp,
            raw_balances=balances,
        )

    async def _load_snapshot(self, account_id: str) -> Mapping[str, Any]:
        use_sim = await self._sim_client.is_active(account_id)
        if use_sim:
            try:
                payload = await self._sim_client.fetch_balances(account_id)
                if isinstance(payload, Mapping):
                    result = dict(payload)
                    result.setdefault("source", "simulation")
                    return result
            except BalanceRetrievalError as exc:
                LOGGER.warning(
                    "Simulation store active but failed for %s: %s", account_id, exc
                )

        try:
            payload = await self._adapter.get_balance(account_id)
        except Exception as exc:  # pragma: no cover - adapter failure is environment specific
            raise BalanceRetrievalError("Failed to fetch balances from Kraken") from exc

        if not isinstance(payload, Mapping):
            raise BalanceRetrievalError("Kraken adapter returned invalid payload")
        result = dict(payload)
        result.setdefault("source", "kraken")
        return result

    @staticmethod
    def _extract_balances(snapshot: Mapping[str, Any]) -> Mapping[str, float]:
        raw = snapshot.get("balances")
        balances: dict[str, float] = {}
        if isinstance(raw, Mapping):
            for asset, value in raw.items():
                normalized = BalanceReader._normalize_asset(asset)
                amount = BalanceReader._coerce_float(value)
                if not normalized or amount is None:
                    continue
                balances[normalized] = balances.get(normalized, 0.0) + amount
        return balances

    @staticmethod
    def _extract_nav(snapshot: Mapping[str, Any]) -> float:
        nav_value = (
            snapshot.get("net_asset_value")
            or snapshot.get("nav")
            or snapshot.get("total_value")
            or snapshot.get("equity")
        )
        nav = BalanceReader._coerce_float(nav_value)
        if nav is None or nav <= 0 or math.isnan(nav):
            return 0.0
        return float(nav)

    @staticmethod
    def _extract_timestamp(snapshot: Mapping[str, Any]) -> Optional[str]:
        timestamp = snapshot.get("timestamp") or snapshot.get("as_of")
        if isinstance(timestamp, str):
            return timestamp
        return None

    @staticmethod
    def _normalize_asset(asset: Any) -> str:
        if not isinstance(asset, str):
            return ""
        candidate = asset.replace("/", "-").split(":", 1)[0]
        candidate = candidate.split(".", 1)[0]
        candidate = candidate.split("-", 1)[0] if candidate.count("-") > 1 else candidate
        return candidate.strip().upper()

    @staticmethod
    def _coerce_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _compute_cash_equivalents(
        self, balances: Mapping[str, float]
    ) -> tuple[float, float, float]:
        cash_usd = balances.get("USD", 0.0)
        stablecoin_usd = 0.0
        max_depeg = 0.0

        for asset, amount in balances.items():
            if asset == "USD":
                continue
            if asset not in self._stablecoin_assets:
                continue
            if amount == 0:
                continue
            status = self._stablecoin_status(asset)
            price = 1.0
            deviation_bps = 0.0
            if status is not None:
                price_value = self._coerce_float(status.price)
                if price_value is None or not math.isfinite(price_value):
                    price = 1.0
                else:
                    price = price_value
                deviation_value = self._coerce_float(status.deviation_bps)
                deviation_bps = abs(deviation_value) if deviation_value is not None else 0.0
                if status.stale:
                    price = 1.0
                max_depeg = max(max_depeg, deviation_bps)
            stablecoin_usd += amount * price
        return (cash_usd, stablecoin_usd, max_depeg)

    def _stablecoin_status(self, asset: str) -> Optional[StablecoinStatus]:
        symbol = f"{asset}-USD"
        try:
            return self._monitor.status(symbol)
        except Exception:  # pragma: no cover - monitor failures should not crash endpoint
            LOGGER.debug("Failed to query stablecoin status for %s", symbol, exc_info=True)
            return None

    @staticmethod
    def _sanitize_float(value: float) -> float:
        if not math.isfinite(value):
            return 0.0
        return float(value)
