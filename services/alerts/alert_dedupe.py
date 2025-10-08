"""Alert deduplication service coordinating Alertmanager ingestion."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:  # pragma: no cover - fastapi is optional for unit tests
    from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
except ModuleNotFoundError:  # pragma: no cover - fallback stub for tests
    class HTTPException(Exception):
        def __init__(self, status_code: int, detail: str) -> None:
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

        class _RouterStub:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.routes: list[tuple[str, Any, Any]] = []

            def get(self, *args: Any, **kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
                def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
                    self.routes.append(("GET", args[0] if args else None, func))
                    return func

                return decorator

        def Depends(dependency: Callable[..., Any]) -> Callable[..., Any]:
            return dependency

        class FastAPI:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.state = type("State", (), {})()

            def on_event(self, *_: Any, **__: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
                def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
                    return func

                return decorator

            def include_router(self, *_: Any, **__: Any) -> None:
                return None

        APIRouter = _RouterStub

from prometheus_client import CollectorRegistry, Counter

from services.common.security import ensure_admin_access


FetchResult = Awaitable[Sequence[Mapping[str, Any]]] | Sequence[Mapping[str, Any]]


def _is_awaitable_fetch(result: FetchResult) -> TypeGuard[Awaitable[Sequence[Mapping[str, Any]]]]:
    return inspect.isawaitable(result)

_DEFAULT_ALERTMANAGER_URL = os.getenv("ALERTMANAGER_URL", "http://alertmanager:9093")
_ALERT_ENDPOINT = "/api/v2/alerts"

router = APIRouter(prefix="/alerts", tags=["alerts"])


# Typed decorator helpers -------------------------------------------------


def _router_get(*args: Any, **kwargs: Any) -> Callable[[RouteFn], RouteFn]:
    return cast(Callable[[RouteFn], RouteFn], router.get(*args, **kwargs))


def _app_on_event(application: FastAPI, event: str) -> Callable[[RouteFn], RouteFn]:
    return cast(Callable[[RouteFn], RouteFn], application.on_event(event))


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AlertPolicy:
    """Configuration for deduplication behaviour."""

    suppression_window: timedelta = timedelta(minutes=10)
    escalation_threshold: int = 3

    def as_dict(self) -> Dict[str, Any]:
        return {
            "suppression_window_seconds": int(self.suppression_window.total_seconds()),
            "escalation_threshold": self.escalation_threshold,
        }


@dataclass
class AlertGroupState:
    """Tracks the lifecycle of a grouped alert."""

    service: str
    alert_type: str
    symbol: str
    severity: str
    first_seen: datetime
    last_seen: datetime
    count: int = 1
    escalated: bool = False
    alert_ids: set[str] = field(default_factory=set)

    def to_payload(self) -> Dict[str, Any]:
        return {
            "service": self.service,
            "alert_type": self.alert_type,
            "symbol": self.symbol,
            "severity": "high" if self.escalated else self.severity,
            "first_seen": self.first_seen.isoformat(),
            "last_seen": self.last_seen.isoformat(),
            "count": self.count,
            "suppressed": max(self.count - 1, 0),
            "escalated": self.escalated,
        }


class AlertDedupeMetrics:
    """Prometheus counters exposed by the dedupe service."""

    def __init__(self, registry: Optional[CollectorRegistry] = None) -> None:
        metric_kwargs = {"registry": registry} if registry is not None else {}
        self.registry = registry
        self.alerts_suppressed_total = Counter(
            "aether_alerts_suppressed_total",
            "Total number of alerts suppressed within the suppression window.",
            ("service", "alert_type", "symbol"),
            **metric_kwargs,
        )
        self.alerts_escalated_total = Counter(
            "aether_alerts_escalated_total",
            "Total number of alert groups escalated due to repeated occurrences.",
            ("service", "alert_type", "symbol"),
            **metric_kwargs,
        )


FetchCallable = Callable[[], FetchResult]


class AlertDedupeService:
    """Ingests alerts from Alertmanager and applies deduplication policies."""

    def __init__(
        self,
        *,
        alertmanager_url: str = _DEFAULT_ALERTMANAGER_URL,
        policy: Optional[AlertPolicy] = None,
        metrics: Optional[AlertDedupeMetrics] = None,
        fetcher: Optional[FetchCallable] = None,
        http_timeout: float = 10.0,
    ) -> None:
        self.alertmanager_url = alertmanager_url.rstrip("/")
        self.policy = policy or AlertPolicy()
        self.metrics = metrics or AlertDedupeMetrics()
        self._fetcher: Optional[FetchCallable] = fetcher
        self._client: Optional[_HttpxClientProtocol] = None
        self._group_states: Dict[Tuple[str, str, str], AlertGroupState] = {}
        self._alert_index: Dict[str, Tuple[str, str, str]] = {}
        self._lock = asyncio.Lock()
        self._http_timeout = float(http_timeout)
        self._httpx_module: Optional[_HttpxModule] = None

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------
    def _ensure_httpx(self) -> _HttpxModule:
        if self._httpx_module is not None:
            return self._httpx_module

        try:
            import httpx as httpx_module
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("httpx is required to fetch alerts from Alertmanager") from exc

        module = cast(_HttpxModule, httpx_module)
        self._httpx_module = module
        return module

    async def _get_client(self) -> _HttpxClientProtocol:
        httpx = self._ensure_httpx()

        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._http_timeout)
        return self._client

    async def _default_fetch(self) -> Sequence[Mapping[str, Any]]:
        client = await self._get_client()
        url = f"{self.alertmanager_url}{_ALERT_ENDPOINT}"
        httpx = self._ensure_httpx()

        try:
            response = await client.get(url)
            response.raise_for_status()
        except httpx.TimeoutException as exc:
            raise HTTPException(
                status_code=504,
                detail=f"Alertmanager request timed out: {exc}",
            ) from exc
        except httpx.HTTPStatusError as exc:  # pragma: no cover - defensive
            raise HTTPException(status_code=502, detail=f"Failed to fetch alerts: {exc}") from exc
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"Alertmanager request failed: {exc}") from exc

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            raise HTTPException(status_code=502, detail="Invalid alert payload from Alertmanager") from exc

        if not isinstance(payload, list):
            raise HTTPException(status_code=502, detail="Unexpected alert payload structure")

        return cast(Sequence[Mapping[str, Any]], payload)

    async def _fetch_alerts(self) -> Sequence[Mapping[str, Any]]:
        if self._fetcher is None:
            return await self._default_fetch()

        result = self._fetcher()
        resolved: Sequence[Mapping[str, Any]]
        if _is_awaitable_fetch(result):
            resolved = await result
        else:
            resolved = cast(Sequence[Mapping[str, Any]], result)
        return list(resolved)

    # ------------------------------------------------------------------
    # Alert ingestion
    # ------------------------------------------------------------------
    async def refresh(self) -> List[Dict[str, Any]]:
        """Fetch latest alerts from Alertmanager and update state."""

        async with self._lock:
            payload = await self._fetch_alerts()
            self.ingest_alerts(payload)
            return self.active_alerts()

    def ingest_alerts(self, alerts: Iterable[Mapping[str, Any]]) -> None:
        """Update internal state with the provided active alerts."""

        new_active_ids: set[str] = set()
        for alert in alerts:
            alert_id = self._alert_identifier(alert)
            new_active_ids.add(alert_id)
            self._process_alert(alert_id, alert)

        expired = set(self._alert_index) - new_active_ids
        for alert_id in expired:
            key = self._alert_index.pop(alert_id)
            state = self._group_states.get(key)
            if state:
                state.alert_ids.discard(alert_id)
                if not state.alert_ids:
                    del self._group_states[key]

    def active_alerts(self) -> List[Dict[str, Any]]:
        """Return the currently active, deduplicated alerts."""

        return [state.to_payload() for state in self._group_states.values()]

    def policies(self) -> Dict[str, Any]:
        return self.policy.as_dict()

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _process_alert(self, alert_id: str, alert: Mapping[str, Any]) -> None:
        key = self._group_key(alert)
        timestamp = self._alert_timestamp(alert)
        severity = self._alert_severity(alert)
        state = self._group_states.get(key)

        if alert_id in self._alert_index:
            # Already accounted for; refresh timestamps
            if state:
                state.last_seen = max(state.last_seen, timestamp)
                state.alert_ids.add(alert_id)
            return

        if state is None:
            state = AlertGroupState(
                service=key[0],
                alert_type=key[1],
                symbol=key[2],
                severity=severity,
                first_seen=timestamp,
                last_seen=timestamp,
            )
            self._group_states[key] = state
        else:
            delta = timestamp - state.last_seen
            if delta <= self.policy.suppression_window:
                state.count += 1
                state.last_seen = max(state.last_seen, timestamp)
                self.metrics.alerts_suppressed_total.labels(
                    service=key[0], alert_type=key[1], symbol=key[2]
                ).inc()
            else:
                state.count = 1
                state.first_seen = timestamp
                state.last_seen = timestamp
                state.escalated = False
                state.severity = severity

        state.alert_ids.add(alert_id)
        self._alert_index[alert_id] = key

        if state.count >= self.policy.escalation_threshold and not state.escalated:
            state.escalated = True
            self.metrics.alerts_escalated_total.labels(
                service=key[0], alert_type=key[1], symbol=key[2]
            ).inc()

    def _group_key(self, alert: Mapping[str, Any]) -> Tuple[str, str, str]:
        labels = alert.get("labels", {})
        service = str(labels.get("service", "unknown"))
        alert_type = str(labels.get("alert_type", labels.get("alertname", "generic")))
        symbol = str(labels.get("symbol", "*"))
        return service, alert_type, symbol

    def _alert_identifier(self, alert: Mapping[str, Any]) -> str:
        fingerprint = alert.get("fingerprint")
        if fingerprint:
            return str(fingerprint)
        labels = alert.get("labels", {})
        if isinstance(labels, Mapping):
            ordered = sorted(labels.items())
            return json.dumps(ordered, sort_keys=True)
        return json.dumps(alert, sort_keys=True)

    def _alert_timestamp(self, alert: Mapping[str, Any]) -> datetime:
        starts_at = alert.get("startsAt") or alert.get("starts_at")
        if isinstance(starts_at, str):
            try:
                return datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
            except ValueError:  # pragma: no cover - defensive guard
                pass
        return datetime.now(timezone.utc)

    def _alert_severity(self, alert: Mapping[str, Any]) -> str:
        labels = alert.get("labels", {})
        severity = labels.get("severity")
        return str(severity) if severity else "warning"


# ---------------------------------------------------------------------------
# Dependency wiring for FastAPI
# ---------------------------------------------------------------------------

_service_lock = threading.Lock()
_service: Optional[AlertDedupeService] = None


def configure_alert_dedupe_service(service: Optional[AlertDedupeService]) -> None:
    global _service
    with _service_lock:
        _service = service


def get_alert_dedupe_service() -> AlertDedupeService:
    with _service_lock:
        if _service is None:
            configure_alert_dedupe_service(AlertDedupeService())
        assert _service is not None  # for type checkers
        return _service


@_router_get("/active")
async def get_active_alerts(
    request: Request,
    service: AlertDedupeService = Depends(get_alert_dedupe_service),
) -> List[Dict[str, Any]]:
    await ensure_admin_access(request, forbid_on_missing_token=True)
    return await service.refresh()


@router.get("/policies")
async def get_alert_policies(
    request: Request,
    service: AlertDedupeService = Depends(get_alert_dedupe_service),
) -> Dict[str, Any]:
    await ensure_admin_access(request, forbid_on_missing_token=True)
    return service.policies()


def setup_alert_dedupe(app: FastAPI, alertmanager_url: Optional[str] = None) -> None:
    """Bind lifecycle hooks for the dedupe service on a FastAPI app."""

    if not isinstance(app, FastAPI):  # pragma: no cover - defensive guard
        raise TypeError("setup_alert_dedupe expects a FastAPI application")

    @_app_on_event(app, "startup")
    async def _configure_dedupe() -> None:  # pragma: no cover - FastAPI lifecycle
        service = AlertDedupeService(alertmanager_url=alertmanager_url or _DEFAULT_ALERTMANAGER_URL)
        configure_alert_dedupe_service(service)
        app.state.alert_dedupe_service = service

    @_app_on_event(app, "shutdown")
    async def _shutdown_dedupe() -> None:  # pragma: no cover - FastAPI lifecycle
        service = get_alert_dedupe_service()
        await service.aclose()
        configure_alert_dedupe_service(None)
        if hasattr(app.state, "alert_dedupe_service"):
            delattr(app.state, "alert_dedupe_service")


__all__ = [
    "AlertDedupeMetrics",
    "AlertDedupeService",
    "AlertPolicy",
    "configure_alert_dedupe_service",
    "get_alert_dedupe_service",
    "router",
    "setup_alert_dedupe",
]
