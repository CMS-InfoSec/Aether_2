"""Automated canary rollout management and status reporting."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Dict, Iterable, Optional, Protocol

from fastapi import APIRouter


LOGGER = logging.getLogger(__name__)


class RolloutPhase(str, Enum):
    """Lifecycle states for a canary deployment."""

    IDLE = "idle"
    DEPLOYING = "deploying"
    MONITORING = "monitoring"
    PROMOTING = "promoting"
    COMPLETED = "completed"
    ROLLED_BACK = "rolled_back"


@dataclass(frozen=True)
class CanaryMetrics:
    """Snapshot of health metrics for a canary deployment."""

    latency_ms: float
    error_rate: float
    pnl_delta_pct: float


@dataclass(frozen=True)
class CanaryThresholds:
    """Allowable bounds for canary performance."""

    max_latency_ms: float = 120.0
    max_error_rate: float = 0.01
    min_pnl_delta_pct: float = -0.02

    def is_healthy(self, metrics: CanaryMetrics) -> bool:
        """Determine if *metrics* satisfy all thresholds."""

        if metrics.latency_ms > self.max_latency_ms:
            return False
        if metrics.error_rate > self.max_error_rate:
            return False
        if metrics.pnl_delta_pct < self.min_pnl_delta_pct:
            return False
        return True


@dataclass(frozen=True)
class RolloutStatus:
    """Current rollout state returned to API consumers."""

    version: Optional[str]
    phase: RolloutPhase
    started_at: Optional[datetime]
    last_updated: datetime
    healthy_since: Optional[datetime]
    traffic_percent: float
    metrics: Optional[CanaryMetrics]
    message: Optional[str]

    def to_payload(self) -> Dict[str, Any]:
        """Serialise the status into primitives for FastAPI responses."""

        def _serialise_timestamp(value: Optional[datetime]) -> Optional[str]:
            return value.isoformat() if value else None

        payload = {
            "version": self.version,
            "phase": self.phase.value,
            "started_at": _serialise_timestamp(self.started_at),
            "last_updated": _serialise_timestamp(self.last_updated),
            "healthy_since": _serialise_timestamp(self.healthy_since),
            "traffic_percent": self.traffic_percent,
            "message": self.message,
        }
        if self.metrics:
            payload["metrics"] = asdict(self.metrics)
        else:
            payload["metrics"] = None
        return payload


class DeploymentBackend(Protocol):
    """Interface for routing traffic to different model versions."""

    def deploy(self, version: str, traffic_percent: float) -> None:
        ...

    def promote(self, version: str) -> None:
        ...

    def rollback(self, version: str) -> None:
        ...


class MetricsProvider(Protocol):
    """Interface for collecting health metrics for a canary deployment."""

    def fetch(self, version: str) -> CanaryMetrics:
        ...


class CanaryLogStore:
    """Persist rollout transitions to a SQLite-backed audit log."""

    def __init__(self, database_path: Path | str = Path("data") / "canary_rollout.db") -> None:
        self._path = Path(database_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._initialise()

    def _initialise(self) -> None:
        with sqlite3.connect(self._path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS canary_log (
                    version TEXT NOT NULL,
                    status TEXT NOT NULL,
                    ts TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def record(self, version: str, status: RolloutPhase, timestamp: datetime) -> None:
        with sqlite3.connect(self._path) as conn:
            conn.execute(
                "INSERT INTO canary_log(version, status, ts) VALUES (?, ?, ?)",
                (version, status.value, timestamp.isoformat()),
            )
            conn.commit()

    def history(self, limit: int = 100) -> Iterable[Dict[str, str]]:
        with sqlite3.connect(self._path) as conn:
            cursor = conn.execute(
                "SELECT version, status, ts FROM canary_log ORDER BY ts DESC LIMIT ?",
                (limit,),
            )
            for row in cursor.fetchall():
                yield {"version": row[0], "status": row[1], "ts": row[2]}


class InMemoryDeploymentBackend:
    """Lightweight deployment backend for development and testing."""

    def __init__(self) -> None:
        self.current_version: Optional[str] = None
        self.traffic_percent: float = 0.0

    def deploy(self, version: str, traffic_percent: float) -> None:
        self.current_version = version
        self.traffic_percent = traffic_percent
        LOGGER.info(
            "Deployed canary version %s at %.2f%% traffic", version, traffic_percent
        )

    def promote(self, version: str) -> None:
        if self.current_version != version:
            LOGGER.warning("Promoting version %s that is not the active canary", version)
        self.current_version = version
        self.traffic_percent = 100.0
        LOGGER.info("Promoted version %s to 100%% traffic", version)

    def rollback(self, version: str) -> None:
        if self.current_version == version:
            LOGGER.info("Rolled back canary version %s", version)
        else:
            LOGGER.warning("Rollback requested for non-active version %s", version)
        self.current_version = None
        self.traffic_percent = 0.0


class StaticMetricsProvider:
    """Metrics provider returning fixed healthy metrics by default."""

    def __init__(self, metrics: Optional[CanaryMetrics] = None) -> None:
        self._metrics = metrics or CanaryMetrics(
            latency_ms=45.0, error_rate=0.0005, pnl_delta_pct=0.01
        )

    def fetch(self, version: str) -> CanaryMetrics:
        LOGGER.debug("Fetching static metrics for version %s", version)
        return self._metrics


class CanaryRolloutManager:
    """Coordinate canary rollouts, monitoring, and status reporting."""

    def __init__(
        self,
        *,
        deployment: DeploymentBackend,
        metrics: MetricsProvider,
        thresholds: CanaryThresholds | None = None,
        log_store: CanaryLogStore | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._deployment = deployment
        self._metrics = metrics
        self._thresholds = thresholds or CanaryThresholds()
        self._log_store = log_store or CanaryLogStore()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = Lock()
        now = self._clock()
        self._status = RolloutStatus(
            version=None,
            phase=RolloutPhase.IDLE,
            started_at=None,
            last_updated=now,
            healthy_since=None,
            traffic_percent=0.0,
            metrics=None,
            message=None,
        )
        self._monitor_task: Optional[asyncio.Task[None]] = None
        self._healthy_since: Optional[datetime] = None
        self._monitor_interval = 300

    @property
    def status(self) -> RolloutStatus:
        with self._lock:
            return self._status

    def start_rollout(self, version: str) -> RolloutStatus:
        """Deploy *version* to 1% of traffic and enter monitoring phase."""

        with self._lock:
            if self._status.phase in {RolloutPhase.MONITORING, RolloutPhase.PROMOTING}:
                raise RuntimeError("A canary rollout is already in progress")

            now = self._clock()
            deploying = replace(
                self._status,
                version=version,
                phase=RolloutPhase.DEPLOYING,
                started_at=now,
                last_updated=now,
                traffic_percent=0.0,
                healthy_since=None,
                message=f"Starting canary rollout for version {version}",
            )
            self._status = deploying
            self._log_store.record(version, RolloutPhase.DEPLOYING, now)

        self._deployment.deploy(version, 1.0)

        with self._lock:
            now = self._clock()
            monitoring = replace(
                self._status,
                phase=RolloutPhase.MONITORING,
                traffic_percent=1.0,
                last_updated=now,
                message="Canary deployed to 1% of traffic",
            )
            self._status = monitoring
            self._healthy_since = None
            self._log_store.record(version, RolloutPhase.MONITORING, now)
            return self._status

    def evaluate_once(self) -> RolloutStatus:
        """Fetch metrics, evaluate thresholds, and act on the current rollout."""

        with self._lock:
            status = self._status
        if status.version is None or status.phase != RolloutPhase.MONITORING:
            return status

        version = status.version
        metrics = self._metrics.fetch(version)
        now = self._clock()

        with self._lock:
            updated = replace(
                self._status,
                metrics=metrics,
                last_updated=now,
                message="Evaluated latest canary metrics",
            )
            self._status = updated

            if not self._thresholds.is_healthy(metrics):
                LOGGER.warning(
                    "Canary metrics breached thresholds for version %s: %s",
                    version,
                    metrics,
                )
                self._log_store.record(version, RolloutPhase.ROLLED_BACK, now)
                self._status = replace(
                    updated,
                    phase=RolloutPhase.ROLLED_BACK,
                    traffic_percent=0.0,
                    healthy_since=None,
                    message="Metrics breached thresholds; initiating rollback",
                )
                rollback_required = True
            else:
                if self._healthy_since is None:
                    self._healthy_since = now
                    healthy_since = now
                else:
                    healthy_since = self._healthy_since

                self._status = replace(
                    updated,
                    healthy_since=healthy_since,
                )
                rollback_required = False

                if now - healthy_since >= timedelta(hours=24):
                    LOGGER.info(
                        "Canary %s healthy for 24h; promoting to full traffic", version
                    )
                    self._log_store.record(version, RolloutPhase.PROMOTING, now)
                    self._status = replace(
                        self._status,
                        phase=RolloutPhase.PROMOTING,
                        message="Promoting canary to 100% traffic",
                    )
                    promote_required = True
                else:
                    promote_required = False

        if rollback_required:
            self._deployment.rollback(version)
            with self._lock:
                now = self._clock()
                self._status = replace(
                    self._status,
                    last_updated=now,
                )
            return self._status

        if promote_required:
            self._deployment.promote(version)
            completion_time = self._clock()
            with self._lock:
                self._log_store.record(version, RolloutPhase.COMPLETED, completion_time)
                self._status = replace(
                    self._status,
                    phase=RolloutPhase.COMPLETED,
                    traffic_percent=100.0,
                    last_updated=completion_time,
                    message="Canary promotion completed",
                )
            return self._status

        return self.status

    async def start_background_monitoring(self, interval_seconds: int = 300) -> None:
        """Launch background task to evaluate the canary periodically."""

        self._monitor_interval = interval_seconds
        if self._monitor_task and not self._monitor_task.done():
            return

        async def _monitor_loop() -> None:
            try:
                while True:
                    await asyncio.sleep(self._monitor_interval)
                    try:
                        await asyncio.to_thread(self.evaluate_once)
                    except Exception:  # pragma: no cover - defensive logging
                        LOGGER.exception("Error while evaluating canary metrics")
            except asyncio.CancelledError:  # pragma: no cover - task cancellation
                LOGGER.info("Canary monitoring loop cancelled")
                raise

        self._monitor_task = asyncio.create_task(_monitor_loop())

    async def stop_background_monitoring(self) -> None:
        """Cancel the background monitoring task if it is running."""

        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:  # pragma: no cover - expected on cancel
                pass
        self._monitor_task = None


_DEFAULT_MANAGER = CanaryRolloutManager(
    deployment=InMemoryDeploymentBackend(),
    metrics=StaticMetricsProvider(),
)


def get_manager() -> CanaryRolloutManager:
    """Expose the default rollout manager for dependency injection."""

    return _DEFAULT_MANAGER


router = APIRouter(prefix="/deploy/canary", tags=["canary"])


@router.on_event("startup")
async def _start_monitoring() -> None:  # pragma: no cover - FastAPI lifecycle
    await _DEFAULT_MANAGER.start_background_monitoring()


@router.on_event("shutdown")
async def _stop_monitoring() -> None:  # pragma: no cover - FastAPI lifecycle
    await _DEFAULT_MANAGER.stop_background_monitoring()


@router.get("/status")
async def get_rollout_status() -> Dict[str, Any]:
    """Return the current canary rollout state."""

    return get_manager().status.to_payload()


__all__ = [
    "CanaryLogStore",
    "CanaryMetrics",
    "CanaryRolloutManager",
    "CanaryThresholds",
    "RolloutPhase",
    "get_manager",
    "router",
]

