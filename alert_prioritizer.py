"""Prometheus alert subscriber that prioritises alerts via ML classification."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Mapping, Optional

import httpx
try:  # pragma: no cover - prefer the real FastAPI implementation when available
    from fastapi import APIRouter, HTTPException, Request
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[assignment]
        APIRouter,
        HTTPException,
        Request,
    )
from shared.postgres import normalize_postgres_dsn

try:  # pragma: no cover - optional heavy dependency
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.feature_extraction import DictVectorizer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import LabelEncoder
except Exception as exc:  # pragma: no cover - allow graceful degradation when sklearn unavailable
    GradientBoostingClassifier = None  # type: ignore[assignment]
    DictVectorizer = None  # type: ignore[assignment]
    Pipeline = None  # type: ignore[assignment]
    LabelEncoder = None  # type: ignore[assignment]
    _SKLEARN_IMPORT_ERROR = exc
else:  # pragma: no cover - executed when sklearn available
    _SKLEARN_IMPORT_ERROR = None

try:  # pragma: no cover - optional dependency in lightweight tests
    from services.common.security import ensure_admin_access
except ModuleNotFoundError:  # pragma: no cover - fallback stub for isolated unit tests
    async def ensure_admin_access(*_: object, **__: object) -> str:  # type: ignore[override]
        return "test-account"


_DEFAULT_ALERTMANAGER_URL = os.getenv("ALERTMANAGER_URL", "http://alertmanager:9093")
_ALERT_ENDPOINT = "/api/v2/alerts"
_DATABASE_URL_ENV = "ALERT_PRIORITIZER_DATABASE_URL"


try:  # pragma: no cover - psycopg may be unavailable in lightweight test envs
    import psycopg
except Exception:  # pragma: no cover - allow dependency injection in tests
    psycopg = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_database_url(url: str) -> str:
    try:
        return normalize_postgres_dsn(
            url,
            allow_sqlite=False,
            label="Alert prioritizer database DSN",
        )
    except RuntimeError as exc:
        raise RuntimeError(str(exc))


class AlertPrioritizerService:
    """Consumes alerts from Alertmanager and classifies severity using ML."""

    def __init__(
        self,
        alertmanager_url: str = _DEFAULT_ALERTMANAGER_URL,
        timeout: float = 5.0,
        database_url: str | None = None,
        psycopg_module: Any | None = None,
        *,
        model: Any | None = None,
        label_encoder: Any | None = None,
    ) -> None:
        self.alertmanager_url = alertmanager_url.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout)
        if (model is None) != (label_encoder is None):
            raise ValueError("model and label_encoder must be provided together")
        if model is not None and label_encoder is not None:
            self._model = model
            self._label_encoder = label_encoder
        else:
            self._model, self._label_encoder = self._train_model()
        self._psycopg = psycopg_module or psycopg
        if self._psycopg is None:
            raise RuntimeError(
                "Timescale/PostgreSQL driver (psycopg) is not installed in this environment."
            )
        self._database_url = self._resolve_database_url(database_url)
        self._init_schema()
        self._connection_lock = asyncio.Lock()
        self._cursor_lock = asyncio.Lock()
        self._connection: Optional[Any] = None

    # ------------------------------------------------------------------
    # Model training utilities
    # ------------------------------------------------------------------
    def _train_model(self) -> tuple[Pipeline, LabelEncoder]:
        """Create a simple ML pipeline for alert severity classification."""

        if _SKLEARN_IMPORT_ERROR is not None:
            raise RuntimeError("scikit-learn is required to train the alert prioritizer model") from _SKLEARN_IMPORT_ERROR

        training_samples: List[Dict[str, Any]] = [
            {
                "service": "pricing",
                "alert_type": "LatencyBreach",
                "frequency": 12,
                "recent_pnl_impact": -12000.0,
                "anomaly_count": 5,
            },
            {
                "service": "pricing",
                "alert_type": "FeeSpike",
                "frequency": 2,
                "recent_pnl_impact": -3000.0,
                "anomaly_count": 2,
            },
            {
                "service": "execution",
                "alert_type": "NoTradeStall",
                "frequency": 8,
                "recent_pnl_impact": -6000.0,
                "anomaly_count": 4,
            },
            {
                "service": "execution",
                "alert_type": "ModelDrift",
                "frequency": 1,
                "recent_pnl_impact": -1000.0,
                "anomaly_count": 1,
            },
            {
                "service": "risk",
                "alert_type": "RiskEngineEvent",
                "frequency": 10,
                "recent_pnl_impact": -20000.0,
                "anomaly_count": 6,
            },
            {
                "service": "risk",
                "alert_type": "FeeSpike",
                "frequency": 4,
                "recent_pnl_impact": -400.0,
                "anomaly_count": 0,
            },
            {
                "service": "ops",
                "alert_type": "LatencyBreach",
                "frequency": 3,
                "recent_pnl_impact": -500.0,
                "anomaly_count": 1,
            },
            {
                "service": "ops",
                "alert_type": "UniverseShrink",
                "frequency": 1,
                "recent_pnl_impact": 0.0,
                "anomaly_count": 0,
            },
            {
                "service": "ops",
                "alert_type": "FeeSpike",
                "frequency": 6,
                "recent_pnl_impact": -1500.0,
                "anomaly_count": 3,
            },
        ]

        training_labels = [
            "high",
            "medium",
            "high",
            "medium",
            "high",
            "low",
            "medium",
            "low",
            "medium",
        ]

        label_encoder = LabelEncoder()
        label_encoder.fit(["low", "medium", "high"])
        encoded_training_labels = label_encoder.transform(training_labels)

        model = Pipeline(
            steps=[
                ("vectorizer", DictVectorizer()),
                ("classifier", GradientBoostingClassifier(random_state=7)),
            ]
        )

        model.fit(training_samples, encoded_training_labels)
        return model, label_encoder

    # ------------------------------------------------------------------
    # Alert ingestion
    # ------------------------------------------------------------------
    async def fetch_alerts(self) -> List[Mapping[str, Any]]:
        """Fetch all active alerts from Alertmanager."""

        url = f"{self.alertmanager_url}{_ALERT_ENDPOINT}"
        response = await self._client.get(url)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:  # pragma: no cover - defensive guard
            raise HTTPException(status_code=502, detail=f"Failed to fetch alerts: {exc}") from exc

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive guard
            raise HTTPException(status_code=502, detail="Invalid alert payload from Alertmanager") from exc

        if not isinstance(payload, list):
            raise HTTPException(status_code=502, detail="Unexpected alert payload structure")

        return payload

    def _alert_identifier(self, alert: Mapping[str, Any]) -> str:
        fingerprint = alert.get("fingerprint")
        if fingerprint:
            return str(fingerprint)

        labels = alert.get("labels", {})
        if isinstance(labels, Mapping):
            ordered = sorted(labels.items())
            return json.dumps(ordered, sort_keys=True)
        return json.dumps(alert, sort_keys=True)

    async def classify_alert(self, alert: Mapping[str, Any]) -> Dict[str, Any]:
        """Classify a single alert into severity buckets."""

        frequency = await self._lookup_frequency(alert)
        features = self._extract_features(alert, frequency)
        prediction = self._model.predict([features])
        severity = self._label_encoder.inverse_transform(prediction)[0]

        alert_id = self._alert_identifier(alert)
        await self._store_classification(alert_id, severity)

        return {
            "alert_id": alert_id,
            "severity": severity,
            "features": features,
            "labels": alert.get("labels", {}),
            "annotations": alert.get("annotations", {}),
        }

    def _extract_features(self, alert: Mapping[str, Any], frequency: float) -> Dict[str, Any]:
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})

        service = "unknown"
        alert_type = "unknown"
        if isinstance(labels, Mapping):
            service = str(labels.get("service") or labels.get("job") or "unknown").lower()
            alert_type = str(labels.get("alertname") or labels.get("alert_type") or "unknown")

        recent_pnl_impact = self._parse_float(annotations.get("recent_pnl_impact"))
        anomaly_count = self._parse_int(annotations.get("anomaly_count"))

        return {
            "service": service,
            "alert_type": alert_type,
            "frequency": frequency,
            "recent_pnl_impact": recent_pnl_impact,
            "anomaly_count": anomaly_count,
        }

    async def _lookup_frequency(self, alert: Mapping[str, Any]) -> float:
        alert_id = self._alert_identifier(alert)
        async with self._acquire_connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT COUNT(*) FROM alert_log WHERE alert_id = %s",
                    (alert_id,),
                )
                row = await cursor.fetchone()
        if not row:
            return 0.0
        value = row[0]
        return float(value) if value is not None else 0.0

    @staticmethod
    def _parse_float(value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _parse_int(value: Any, default: int = 0) -> int:
        try:
            if value is None:
                return default
            return int(value)
        except (ValueError, TypeError):
            return default

    async def _store_classification(self, alert_id: str, severity: str) -> None:
        async with self._acquire_connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "INSERT INTO alert_log(alert_id, severity, ts) VALUES(%s, %s, %s)",
                    (alert_id, severity, _now_utc()),
                )

    async def get_prioritized_alerts(self) -> List[Dict[str, Any]]:
        alerts = await self.fetch_alerts()

        classified: List[Dict[str, Any]] = []
        for alert in alerts:
            try:
                result = await self.classify_alert(alert)
            except HTTPException:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                raise HTTPException(status_code=500, detail=f"Failed to classify alert: {exc}") from exc
            classified.append(result)

        severity_order = {"high": 0, "medium": 1, "low": 2}
        classified.sort(key=lambda item: severity_order.get(item["severity"], 3))
        return classified

    async def close(self) -> None:
        await self._client.aclose()
        async with self._connection_lock:
            if self._connection is not None:
                try:
                    await self._connection.close()
                except Exception:  # pragma: no cover - defensive cleanup
                    logger.debug("Failed to close alert prioritizer database connection", exc_info=True)
                self._connection = None

    def _resolve_database_url(self, database_url: str | None) -> str:
        configured = database_url or os.getenv(_DATABASE_URL_ENV)
        if not configured:
            raise RuntimeError(
                f"{_DATABASE_URL_ENV} must be configured with the shared Postgres/Timescale DSN."
            )
        return _normalize_database_url(configured)

    def _init_schema(self) -> None:
        connection = self._psycopg.connect(self._database_url)
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("SET TIME ZONE 'UTC'")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS alert_log (
                        alert_id TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        ts TIMESTAMPTZ NOT NULL
                    )
                    """
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_alert_log_alert_id ON alert_log(alert_id)"
                )
        finally:
            connection.close()

    async def _ensure_connection(self) -> Any:
        async with self._connection_lock:
            if self._connection is not None and not getattr(self._connection, "closed", False):
                return self._connection
            async_connection_class = getattr(self._psycopg, "AsyncConnection", None)
            if async_connection_class is None:
                raise RuntimeError("Configured psycopg module does not provide AsyncConnection support")
            connection = await async_connection_class.connect(self._database_url)
            try:
                setattr(connection, "autocommit", True)
            except Exception:  # pragma: no cover - not all drivers expose autocommit attribute
                logger.debug("Unable to enable autocommit on alert prioritizer connection", exc_info=True)
            async with connection.cursor() as cursor:
                await cursor.execute("SET TIME ZONE 'UTC'")
            self._connection = connection
            return connection

    @asynccontextmanager
    async def _acquire_connection(self) -> Any:
        connection = await self._ensure_connection()
        async with self._cursor_lock:
            try:
                yield connection
            except Exception:
                # Reset the connection on failure to avoid leaking broken sessions
                try:
                    await connection.close()
                except Exception:  # pragma: no cover - defensive cleanup
                    logger.debug(
                        "Failed to close alert prioritizer connection after error", exc_info=True
                    )
                async with self._connection_lock:
                    self._connection = None
                raise


router = APIRouter()
_service_error: Exception | None = None
try:
    _service: Optional[AlertPrioritizerService] = AlertPrioritizerService()
except Exception as exc:  # pragma: no cover - handled during application startup
    logger.error("Failed to initialise alert prioritizer service", exc_info=True)
    _service = None
    _service_error = exc


@router.get("/alerts/prioritized")
async def prioritized_alerts(request: Request) -> List[Dict[str, Any]]:
    """Return alerts prioritised by the ML classifier."""

    await ensure_admin_access(request, forbid_on_missing_token=True)
    if _service is None:
        detail = "Alert prioritizer service is unavailable"
        if _service_error is not None:
            detail = f"Alert prioritizer unavailable: {_service_error}"
        raise HTTPException(status_code=503, detail=detail)
    return await _service.get_prioritized_alerts()


@asynccontextmanager
async def _lifespan(_: Any) -> AsyncIterator[None]:
    try:
        yield
    finally:
        if _service is not None:
            await _service.close()


router.lifespan_context = _lifespan  # type: ignore[attr-defined]


__all__ = ["router", "AlertPrioritizerService"]
