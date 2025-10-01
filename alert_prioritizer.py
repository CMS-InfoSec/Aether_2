"""Prometheus alert subscriber that prioritises alerts via ML classification."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping

import httpx
from fastapi import APIRouter, HTTPException
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.feature_extraction import DictVectorizer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder

_DEFAULT_ALERTMANAGER_URL = os.getenv("ALERTMANAGER_URL", "http://alertmanager:9093")
_ALERT_ENDPOINT = "/api/v2/alerts"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class AlertPrioritizerService:
    """Consumes alerts from Alertmanager and classifies severity using ML."""

    def __init__(
        self,
        alertmanager_url: str = _DEFAULT_ALERTMANAGER_URL,
        timeout: float = 5.0,
        db_path: str = "data/alert_prioritizer.db",
    ) -> None:
        self.alertmanager_url = alertmanager_url.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout)
        self._model, self._label_encoder = self._train_model()
        self._db = sqlite3.connect(db_path, check_same_thread=False)
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_log (
                alert_id TEXT NOT NULL,
                severity TEXT NOT NULL,
                ts TIMESTAMP NOT NULL
            )
            """
        )
        self._db.commit()
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Model training utilities
    # ------------------------------------------------------------------
    def _train_model(self) -> tuple[Pipeline, LabelEncoder]:
        """Create a simple ML pipeline for alert severity classification."""

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

        features = self._extract_features(alert)
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

    def _extract_features(self, alert: Mapping[str, Any]) -> Dict[str, Any]:
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})

        service = "unknown"
        alert_type = "unknown"
        if isinstance(labels, Mapping):
            service = str(labels.get("service") or labels.get("job") or "unknown").lower()
            alert_type = str(labels.get("alertname") or labels.get("alert_type") or "unknown")

        frequency = self._lookup_frequency(alert)
        recent_pnl_impact = self._parse_float(annotations.get("recent_pnl_impact"))
        anomaly_count = self._parse_int(annotations.get("anomaly_count"))

        return {
            "service": service,
            "alert_type": alert_type,
            "frequency": frequency,
            "recent_pnl_impact": recent_pnl_impact,
            "anomaly_count": anomaly_count,
        }

    def _lookup_frequency(self, alert: Mapping[str, Any]) -> float:
        alert_id = self._alert_identifier(alert)
        cursor = self._db.execute(
            "SELECT COUNT(*) FROM alert_log WHERE alert_id = ?",
            (alert_id,),
        )
        result = cursor.fetchone()
        return float(result[0]) if result and result[0] is not None else 0.0

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
        async with self._lock:
            self._db.execute(
                "INSERT INTO alert_log(alert_id, severity, ts) VALUES(?, ?, ?)",
                (alert_id, severity, _now_utc()),
            )
            self._db.commit()

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
        self._db.close()


router = APIRouter()
_service = AlertPrioritizerService()


@router.get("/alerts/prioritized")
async def prioritized_alerts() -> List[Dict[str, Any]]:
    """Return alerts prioritised by the ML classifier."""

    return await _service.get_prioritized_alerts()


@router.on_event("shutdown")
async def shutdown_prioritizer() -> None:
    await _service.close()


__all__ = ["router", "AlertPrioritizerService"]

