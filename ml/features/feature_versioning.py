"""Feature versioning and promotion service with semantic version control."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import Callable, Dict, Mapping, MutableMapping, Tuple

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field, validator

LOGGER = logging.getLogger(__name__)

SEMVER_PATTERN = re.compile(r"^v\d+\.\d+\.\d+$")


class FeatureDeploymentState(str, Enum):
    """Allowed lifecycle states for a feature definition version."""

    INACTIVE = "INACTIVE"
    ACTIVE_CANARY = "ACTIVE_CANARY"
    ACTIVE = "ACTIVE"


@dataclass
class HealthCheckResult:
    """Outcome of running post-promotion health checks."""

    passed: bool
    metrics: Mapping[str, float] = field(default_factory=dict)
    details: str | None = None


@dataclass
class FeatureVersionRecord:
    """Internal representation of a feature definition version."""

    version: str
    changelog: str
    status: FeatureDeploymentState
    created_at: datetime
    promoted_at: datetime | None = None
    shadow_metrics: MutableMapping[str, float] = field(default_factory=dict)
    health_passed: bool | None = None
    health_details: str | None = None
    model_version: str | None = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "version": self.version,
            "changelog": self.changelog,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "promoted_at": self.promoted_at.isoformat() if self.promoted_at else None,
            "shadow_metrics": dict(self.shadow_metrics),
            "health_passed": self.health_passed,
            "health_details": self.health_details,
            "model_version": self.model_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> "FeatureVersionRecord":
        created_at = _parse_datetime(data.get("created_at"))
        promoted_at = _parse_datetime(data.get("promoted_at"))
        status = FeatureDeploymentState(str(data.get("status", FeatureDeploymentState.INACTIVE.value)))
        return cls(
            version=str(data.get("version")),
            changelog=str(data.get("changelog", "")),
            status=status,
            created_at=created_at or datetime.now(timezone.utc),
            promoted_at=promoted_at,
            shadow_metrics=dict(data.get("shadow_metrics", {}) or {}),
            health_passed=data.get("health_passed"),
            health_details=data.get("health_details"),
            model_version=data.get("model_version"),
        )


@dataclass
class FeaturePromotionResult:
    """Snapshot describing the outcome of a promotion attempt."""

    record: FeatureVersionRecord
    promoted: bool
    rolled_back: bool
    previous_active: str | None


def _parse_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        LOGGER.warning("Failed to parse datetime value: %s", value)
        return None


class FeatureVersioningManager:
    """Coordinates promotion, health checking and persistence of feature versions."""

    def __init__(
        self,
        *,
        storage_path: Path,
        shadow_evaluator: Callable[[str], Mapping[str, float]] | None = None,
        health_checker: Callable[[str, Mapping[str, float]], HealthCheckResult] | None = None,
        model_version_resolver: Callable[[], str] | None = None,
    ) -> None:
        self._storage_path = storage_path
        self._shadow_evaluator = shadow_evaluator or _default_shadow_evaluator
        self._health_checker = health_checker or _default_health_checker
        self._model_version_resolver = model_version_resolver or _default_model_version_resolver
        self._lock = Lock()
        self._records: Dict[str, FeatureVersionRecord] = {}
        self._active_version: str | None = None
        self._canary_version: str | None = None
        self._model_map: Dict[str, str] = {}
        self._load()

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------
    def _load(self) -> None:
        if not self._storage_path.exists():
            self._storage_path.parent.mkdir(parents=True, exist_ok=True)
            self._persist()
            return

        try:
            with self._storage_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (json.JSONDecodeError, OSError) as exc:
            LOGGER.warning("Unable to load feature version state: %s", exc)
            return

        records = payload.get("records", [])
        for item in records:
            record = FeatureVersionRecord.from_dict(item)
            self._records[record.version] = record
            if record.status is FeatureDeploymentState.ACTIVE:
                self._active_version = record.version
            elif record.status is FeatureDeploymentState.ACTIVE_CANARY:
                self._canary_version = record.version
        self._model_map = dict(payload.get("model_map", {}) or {})
        self._active_version = payload.get("active_version", self._active_version)
        self._canary_version = payload.get("canary_version", self._canary_version)

    def _persist(self) -> None:
        payload = {
            "records": [record.to_dict() for record in self._records.values()],
            "active_version": self._active_version,
            "canary_version": self._canary_version,
            "model_map": dict(self._model_map),
        }
        try:
            self._storage_path.parent.mkdir(parents=True, exist_ok=True)
            with self._storage_path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
        except OSError as exc:
            LOGGER.error("Failed to persist feature version state: %s", exc)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def promote(self, version: str, changelog: str) -> FeaturePromotionResult:
        if not SEMVER_PATTERN.match(version):
            raise ValueError("Version must follow semantic format vMAJOR.MINOR.PATCH")
        metrics = dict(self._shadow_evaluator(version))
        previous_active = None

        with self._lock:
            now = datetime.now(timezone.utc)
            record = self._records.get(version)
            if record is None:
                record = FeatureVersionRecord(
                    version=version,
                    changelog=changelog,
                    status=FeatureDeploymentState.ACTIVE_CANARY,
                    created_at=now,
                    shadow_metrics=metrics,
                )
                self._records[version] = record
            else:
                record.changelog = changelog
                record.status = FeatureDeploymentState.ACTIVE_CANARY
                record.shadow_metrics = metrics
                record.health_passed = None
                record.health_details = None
                record.promoted_at = None

            previous_active = self._active_version if self._active_version != version else None
            if previous_active:
                prev_record = self._records.get(previous_active)
                if prev_record:
                    prev_record.status = FeatureDeploymentState.ACTIVE

            self._canary_version = version
            self._persist()

        try:
            result = self._health_checker(version, metrics)
        except Exception as exc:  # pragma: no cover - defensive fallback
            LOGGER.exception("Health checks failed for %s", version)
            result = HealthCheckResult(
                passed=False,
                metrics=metrics,
                details=f"Health check execution error: {exc}",
            )

        with self._lock:
            record = self._records[version]
            record.shadow_metrics = dict(result.metrics or metrics)
            record.health_passed = result.passed
            record.health_details = result.details
            if result.passed:
                record.status = FeatureDeploymentState.ACTIVE
                record.promoted_at = datetime.now(timezone.utc)
                self._active_version = version
                self._canary_version = None
                model_version = self._model_version_resolver()
                record.model_version = model_version
                if model_version:
                    self._model_map[version] = model_version
                if previous_active:
                    prev_record = self._records.get(previous_active)
                    if prev_record:
                        prev_record.status = FeatureDeploymentState.INACTIVE
            else:
                record.status = FeatureDeploymentState.INACTIVE
                if previous_active:
                    self._active_version = previous_active
                    prev_record = self._records.get(previous_active)
                    if prev_record:
                        prev_record.status = FeatureDeploymentState.ACTIVE
                else:
                    self._active_version = None
                self._canary_version = None
            self._persist()

        return FeaturePromotionResult(
            record=record,
            promoted=result.passed,
            rolled_back=not result.passed,
            previous_active=previous_active,
        )

    def status(self) -> Dict[str, object]:
        with self._lock:
            history = [record.to_dict() for record in sorted(self._records.values(), key=lambda r: r.created_at)]
            active = self._records.get(self._active_version) if self._active_version else None
            canary = self._records.get(self._canary_version) if self._canary_version else None
            return {
                "active": active.to_dict() if active else None,
                "canary": canary.to_dict() if canary else None,
                "history": history,
                "model_map": dict(self._model_map),
            }

    def current_versions(self) -> Tuple[str | None, str | None]:
        with self._lock:
            return self._active_version, self._canary_version


# ----------------------------------------------------------------------
# FastAPI wiring
# ----------------------------------------------------------------------


class FeaturePromotionRequest(BaseModel):
    """Payload required to promote a feature version."""

    version: str = Field(..., description="Semantic version to promote, e.g. v1.2.0")
    changelog: str = Field(..., description="Human-readable summary of the feature changes")

    @validator("version")
    def _validate_semver(cls, value: str) -> str:
        if not SEMVER_PATTERN.match(value):
            raise ValueError("Version must follow semantic format vMAJOR.MINOR.PATCH")
        return value


class FeaturePromotionResponse(BaseModel):
    """Result returned to clients after attempting a promotion."""

    version: str
    status: FeatureDeploymentState
    promoted: bool
    rolled_back: bool
    active_version: str | None
    canary_version: str | None
    health_passed: bool | None
    health_details: str | None
    shadow_metrics: Mapping[str, float]
    model_version: str | None
    previous_active: str | None


class FeatureStatusResponse(BaseModel):
    """Response model for ``GET /features/status``."""

    active: Mapping[str, object] | None
    canary: Mapping[str, object] | None
    history: list[Mapping[str, object]]
    model_map: Mapping[str, str]


def _default_shadow_evaluator(version: str) -> Mapping[str, float]:
    """Run a lightweight shadow evaluation returning synthetic metrics."""

    # In production this would execute offline feature pipelines against recorded
    # traffic.  The deterministic placeholder keeps unit tests stable while giving
    # operators enough signal for dashboards.
    return {
        "shadow_latency_ms": 35.0,
        "records_scored": 1000.0,
        "version_hash": float(abs(hash(version)) % 10_000),
    }


def _default_health_checker(version: str, metrics: Mapping[str, float]) -> HealthCheckResult:
    """Simple heuristic validating the shadow evaluation metrics."""

    error_rate = metrics.get("error_rate", 0.0)
    latency = metrics.get("shadow_latency_ms", 0.0)
    passed = error_rate <= 0.05 and latency <= 250.0
    details = "All health checks passed" if passed else "Shadow evaluation breached thresholds"
    return HealthCheckResult(passed=passed, metrics=metrics, details=details)


def _default_model_version_resolver() -> str:
    """Return the currently deployed model version for persistence mapping."""

    return os.getenv("CURRENT_MODEL_VERSION", "model-main")


def _get_storage_path() -> Path:
    configured = os.getenv("FEATURE_VERSION_STORE")
    if configured:
        return Path(configured)
    return Path("artifacts/feature_versions.json")


_MANAGER = FeatureVersioningManager(storage_path=_get_storage_path())

app = FastAPI(title="Feature Versioning Service", version="1.0.0")


@app.post("/features/promote", response_model=FeaturePromotionResponse)
def promote_feature(request: FeaturePromotionRequest) -> FeaturePromotionResponse:
    try:
        result = _MANAGER.promote(request.version, request.changelog)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    active_version, canary_version = _MANAGER.current_versions()
    return FeaturePromotionResponse(
        version=result.record.version,
        status=result.record.status,
        promoted=result.promoted,
        rolled_back=result.rolled_back,
        active_version=active_version,
        canary_version=canary_version,
        health_passed=result.record.health_passed,
        health_details=result.record.health_details,
        shadow_metrics=dict(result.record.shadow_metrics),
        model_version=result.record.model_version,
        previous_active=result.previous_active,
    )


@app.get("/features/status", response_model=FeatureStatusResponse)
def get_feature_status() -> FeatureStatusResponse:
    payload = _MANAGER.status()
    return FeatureStatusResponse(**payload)
