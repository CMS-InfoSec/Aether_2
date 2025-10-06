from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Deque, DefaultDict, Dict, Iterable, List, Optional, Sequence

from fastapi import APIRouter, FastAPI, HTTPException

from services.common.adapters import RedisFeastAdapter, TimescaleAdapter
from shared.spot import is_spot_symbol, normalize_spot_symbol
from services.models.model_zoo import ModelZoo, get_model_zoo

LOGGER = logging.getLogger(__name__)

DEFAULT_ACCOUNT_IDS: Sequence[str] = ("company",)
DEFAULT_INSTRUMENTS: Sequence[str] = ("BTC-USD", "ETH-USD", "SOL-USD")


@dataclass
class CacheMetrics:
    """Simple counter used to compute cache hit ratios."""

    hits: int = 0
    misses: int = 0

    def record(self, *, hits: int = 0, misses: int = 0) -> None:
        if hits:
            self.hits += int(max(0, hits))
        if misses:
            self.misses += int(max(0, misses))

    @property
    def total(self) -> int:
        return self.hits + self.misses

    @property
    def hit_ratio(self) -> float:
        total = self.total
        if total <= 0:
            return 0.0
        return float(self.hits) / float(total)

    def snapshot(self) -> Dict[str, float]:
        return {
            "hits": float(self.hits),
            "misses": float(self.misses),
            "hit_ratio": self.hit_ratio,
        }


@dataclass
class ModuleWarmupResult:
    """Outcome of warming a specific cache-backed module."""

    module: str
    account_id: Optional[str]
    started_at: datetime
    completed_at: datetime
    duration_ms: float
    items_loaded: int
    hits: int
    misses: int
    hit_ratio_before: float
    hit_ratio_after: float
    hit_ratio_improvement: float
    error: Optional[str] = None

    def to_payload(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "module": self.module,
            "account_id": self.account_id,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "duration_ms": round(self.duration_ms, 3),
            "items_loaded": self.items_loaded,
            "hits": self.hits,
            "misses": self.misses,
            "hit_ratio_before": round(self.hit_ratio_before, 6),
            "hit_ratio_after": round(self.hit_ratio_after, 6),
            "hit_ratio_improvement": round(self.hit_ratio_improvement, 6),
            "status": "ok" if self.error is None else "error",
        }
        if self.error:
            payload["error"] = self.error
        return payload


@dataclass
class CacheWarmupReport:
    """Aggregate result for a cache warmup run."""

    started_at: datetime
    completed_at: datetime
    modules: Sequence[ModuleWarmupResult]
    account_ids: Sequence[str]

    @property
    def duration_ms(self) -> float:
        return round((self.completed_at - self.started_at).total_seconds() * 1000.0, 3)

    def to_payload(self) -> Dict[str, object]:
        return {
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "duration_ms": self.duration_ms,
            "account_ids": list(self.account_ids),
            "modules": [module.to_payload() for module in self.modules],
        }

    def to_summary(self) -> Dict[str, object]:
        return {
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "duration_ms": self.duration_ms,
        }


RedisFactory = Callable[[str], RedisFeastAdapter]
TimescaleFactory = Callable[[str], TimescaleAdapter]
ModelLoader = Callable[[], ModelZoo]


class CacheWarmer:
    """Prefetch frequently requested resources to smooth cold-start latency."""

    def __init__(
        self,
        *,
        account_ids: Sequence[str] | None = None,
        redis_factory: RedisFactory | None = None,
        timescale_factory: TimescaleFactory | None = None,
        model_loader: ModelLoader | None = None,
        most_used_instruments: Sequence[str] | None = None,
        max_instruments: int = 5,
        history_size: int = 5,
    ) -> None:
        self.account_ids: Sequence[str] = tuple(account_ids or DEFAULT_ACCOUNT_IDS)
        self._redis_factory = redis_factory or RedisFeastAdapter
        self._timescale_factory = timescale_factory or TimescaleAdapter
        self._model_loader = model_loader or get_model_zoo
        self._most_used_instruments: Sequence[str] = tuple(
            most_used_instruments or DEFAULT_INSTRUMENTS
        )
        self._max_instruments = max(1, int(max_instruments))
        self._metrics: DefaultDict[str, CacheMetrics] = defaultdict(CacheMetrics)
        self._history: Deque[CacheWarmupReport] = deque(maxlen=history_size)
        self._last_report: Optional[CacheWarmupReport] = None
        self._lock = asyncio.Lock()

    async def warmup(self, *, force: bool = False) -> CacheWarmupReport:
        """Execute the warmup flow, optionally forcing a rerun."""

        async with self._lock:
            if self._last_report is not None and not force:
                return self._last_report

            started_at = datetime.now(timezone.utc)
            modules: List[ModuleWarmupResult] = []

            for account_id in self.account_ids:
                modules.extend(self._warm_account(account_id))

            modules.append(self._warm_models())

            completed_at = datetime.now(timezone.utc)
            report = CacheWarmupReport(
                started_at=started_at,
                completed_at=completed_at,
                modules=modules,
                account_ids=self.account_ids,
            )
            self._last_report = report
            self._history.append(report)

            LOGGER.info(
                "Cache warmup completed in %.2f ms across %d modules",
                report.duration_ms,
                len(modules),
            )
            return report

    def status(self) -> Dict[str, object]:
        """Return the latest warmup status and historical metrics."""

        payload: Dict[str, object] = {
            "status": "cold" if self._last_report is None else "warm",
            "history": [entry.to_summary() for entry in self._history],
            "cache_metrics": {
                key: metrics.snapshot()
                for key, metrics in sorted(self._metrics.items())
            },
        }
        if self._last_report is not None:
            payload["last_run"] = self._last_report.to_payload()
        else:
            payload["last_run"] = None
        return payload

    # ------------------------------------------------------------------
    # Warmup helpers
    # ------------------------------------------------------------------
    def _warm_account(self, account_id: str) -> List[ModuleWarmupResult]:
        redis_adapter = self._redis_factory(account_id)
        instruments = self._select_instruments(redis_adapter)
        modules: List[ModuleWarmupResult] = []

        modules.append(
            self._execute_module(
                module="features",
                account_id=account_id,
                loader=lambda: self._warm_features(redis_adapter, instruments),
            )
        )
        modules.append(
            self._execute_module(
                module="fee_tiers",
                account_id=account_id,
                loader=lambda: self._warm_fee_tiers(redis_adapter, instruments),
            )
        )

        timescale_adapter = self._timescale_factory(account_id)
        modules.append(
            self._execute_module(
                module="ohlcv",
                account_id=account_id,
                loader=lambda: self._warm_ohlcv(timescale_adapter, instruments),
            )
        )

        return modules

    def _warm_models(self) -> ModuleWarmupResult:
        return self._execute_module(
            module="models",
            account_id=None,
            loader=self._prefetch_models,
        )

    def _execute_module(
        self,
        *,
        module: str,
        account_id: Optional[str],
        loader: Callable[[], tuple[int, int, int]],
    ) -> ModuleWarmupResult:
        metrics = self._metrics[self._metrics_key(module, account_id)]
        before = metrics.hit_ratio
        started_at = datetime.now(timezone.utc)
        start = time.perf_counter()
        items = hits = misses = 0
        error: Optional[str] = None

        try:
            items, hits, misses = loader()
            metrics.record(hits=hits, misses=misses)
        except Exception as exc:  # pragma: no cover - defensive guard
            error = str(exc)
            LOGGER.exception(
                "Cache warmup module '%s' failed for account %s", module, account_id
            )
        completed_at = datetime.now(timezone.utc)
        duration_ms = (time.perf_counter() - start) * 1000.0
        after = metrics.hit_ratio
        improvement = after - before

        LOGGER.debug(
            "Warmup module=%s account=%s duration=%.2fms hits=%d misses=%d ratio %.3f->%.3f",
            module,
            account_id,
            duration_ms,
            hits,
            misses,
            before,
            after,
        )

        return ModuleWarmupResult(
            module=module,
            account_id=account_id,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=duration_ms,
            items_loaded=items,
            hits=hits,
            misses=misses,
            hit_ratio_before=before,
            hit_ratio_after=after,
            hit_ratio_improvement=improvement,
            error=error,
        )

    def _select_instruments(self, redis: RedisFeastAdapter) -> List[str]:
        candidates: List[str] = []
        seen = set()

        def _append(symbol: str) -> None:
            normalized = normalize_spot_symbol(symbol)
            if not normalized:
                return

            if not is_spot_symbol(normalized):
                LOGGER.warning(
                    "Ignoring non-spot instrument '%s' in cache warmup", symbol
                )
                return

            if normalized in seen:
                return

            seen.add(normalized)
            candidates.append(normalized)

        for symbol in self._most_used_instruments:
            _append(symbol)

        try:
            for symbol in redis.approved_instruments():
                _append(symbol)
        except Exception:  # pragma: no cover - defensive guard
            LOGGER.exception("Failed to load approved instruments during warmup")

        return candidates[: self._max_instruments]

    def _warm_features(
        self, redis: RedisFeastAdapter, instruments: Iterable[str]
    ) -> tuple[int, int, int]:
        items = hits = misses = 0
        for instrument in instruments:
            payload = redis.fetch_online_features(instrument)
            items += 1
            if payload:
                hits += 1
            else:
                misses += 1
        return items, hits, misses

    def _warm_fee_tiers(
        self, redis: RedisFeastAdapter, instruments: Iterable[str]
    ) -> tuple[int, int, int]:
        items = hits = misses = 0
        for instrument in instruments:
            tiers = redis.fee_tiers(instrument)
            items += 1
            if tiers:
                hits += 1
            else:
                misses += 1
        return items, hits, misses

    def _warm_ohlcv(
        self, timescale: TimescaleAdapter, instruments: Iterable[str]
    ) -> tuple[int, int, int]:
        items = hits = misses = 0
        for instrument in instruments:
            rolling = timescale.rolling_volume(instrument)
            items += 1
            notional = float(rolling.get("notional", 0.0))
            if notional > 0:
                hits += 1
            else:
                misses += 1
        return items, hits, misses

    def _prefetch_models(self) -> tuple[int, int, int]:
        zoo = self._model_loader()
        inventory = list(zoo.inventory())
        active_models = list(zoo.active_models())
        items = len(inventory)
        hits = sum(1 for entry in inventory if getattr(entry, "versions", None))
        misses = max(0, items - hits)
        # Access payloads to prime any lazy properties.
        for entry in inventory:
            entry.to_payload()
        for entry in active_models:
            entry.to_payload()
        return items, hits, misses

    def _metrics_key(self, module: str, account_id: Optional[str]) -> str:
        if account_id is None:
            return module
        return f"{module}:{account_id}"


router = APIRouter(prefix="/ops/warmup", tags=["operations"])

_WARMER: Optional[CacheWarmer] = None


@router.get("/status")
async def warmup_status() -> Dict[str, object]:
    if _WARMER is None:
        raise HTTPException(status_code=503, detail="Cache warmer not configured")
    return _WARMER.status()


def register(app: FastAPI, warmer: Optional[CacheWarmer] = None) -> CacheWarmer:
    """Attach the cache warmer to the FastAPI application lifecycle."""

    global _WARMER
    if warmer is None:
        warmer = CacheWarmer()
    _WARMER = warmer

    if not any(route.path.startswith(router.prefix) for route in app.router.routes):
        app.include_router(router)

    @app.on_event("startup")
    async def _run_cache_warmup() -> None:  # pragma: no cover - FastAPI lifecycle
        assert _WARMER is not None
        try:
            await _WARMER.warmup()
        except Exception:  # pragma: no cover - defensive guard
            LOGGER.exception("Cache warmup failed during startup")

    app.state.cache_warmer = warmer
    return warmer


__all__ = [
    "CacheMetrics",
    "ModuleWarmupResult",
    "CacheWarmupReport",
    "CacheWarmer",
    "router",
    "register",
]
