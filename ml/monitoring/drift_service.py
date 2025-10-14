"""Drift monitoring orchestration with automated canary deployments.

The trading platform previously offered a monolithic drift service at the
repository root.  The new implementation focuses on the ML package boundary to
make it embeddable inside batch jobs as well as FastAPI routers.  It wires
feature monitoring, retraining triggers and MLflow canary promotion logic so
that higher level services only need to feed fresh production data and trading
events.

The module purposefully keeps the external interface small:

``DriftMonitoringService``
    Core state machine that evaluates drift, requests retraining when the
    population stability index (PSI) or Kolmogorov-Smirnov (KS) statistic
    breach configured thresholds and exposes status snapshots for APIs.

``CanaryDeploymentManager``
    Thin wrapper around the MLflow model registry used to transition a newly
    trained model into the ``canary`` stage and promote or rollback based on
    subsequent trading stability.

The FastAPI router declared at the bottom exposes a single ``GET /drift/status``
endpoint returning the latest per-feature drift metrics, retraining status and
canary rollout metadata.  Additional write operations (feeding production data
or recording trades) happen via direct service invocation, keeping the HTTP
surface deliberately small.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import os
from types import ModuleType
from typing import Any, Callable, Dict, Mapping, MutableMapping, Sequence, Tuple, TYPE_CHECKING

try:  # pragma: no cover - pandas is optional in lightweight environments
    import pandas as pd
except Exception:  # pragma: no cover - executed when pandas is unavailable
    pd = None  # type: ignore[assignment]

try:  # pragma: no cover - prefer FastAPI when available
    from fastapi import APIRouter, Depends, HTTPException, status
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[misc]
        APIRouter,
        Depends,
        HTTPException,
        status,
    )
from pydantic import BaseModel, Field

from ml.monitoring.drift import DriftReport, MissingDependencyError, generate_drift_report

LOGGER = logging.getLogger(__name__)


if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    import pandas


try:  # pragma: no cover - mlflow is optional for tests/CI.
    import mlflow
    from mlflow.tracking import MlflowClient
    from shared.mlflow_safe import harden_mlflow

    harden_mlflow(mlflow)
except Exception:  # pragma: no cover - executed when mlflow is missing.
    mlflow = None  # type: ignore
    MlflowClient = None  # type: ignore


CANARY_STAGE = "canary"
PRODUCTION_STAGE = "Production"
ARCHIVED_STAGE = "Archived"


class FeatureDriftScore(BaseModel):
    """Container returned to API consumers for a single feature."""

    psi: float = Field(..., description="Population stability index score")
    psi_alert: bool = Field(..., description="Whether PSI breached its threshold")
    ks: float = Field(..., description="Kolmogorov-Smirnov statistic")
    ks_alert: bool = Field(..., description="Whether KS statistic breached its threshold")
    severity: float = Field(
        ..., description="Max-normalised drift severity across monitored statistics"
    )
    alert: bool = Field(..., description="Whether the feature breached thresholds")
    checked_at: datetime = Field(..., description="Timestamp of the last evaluation")


class CanaryStatus(BaseModel):
    """Snapshot of the latest canary deployment orchestration state."""

    active_version: str | None = Field(
        None, description="Model version currently in the canary stage"
    )
    previous_production: str | None = Field(
        None, description="Last known production model version"
    )
    trades_observed: int = Field(0, description="Trades observed since canary deploy")
    promote_after: int = Field(
        ..., description="Number of trades required for promotion"
    )
    last_promotion_at: datetime | None = Field(
        None, description="Timestamp when the canary was last promoted"
    )
    rollbacks: int = Field(0, description="Count of rollback operations performed")
    mlflow_enabled: bool = Field(
        ..., description="Whether MLflow integration is active for deployments"
    )


class DriftStatusResponse(BaseModel):
    """Response payload exposed via ``GET /drift/status``."""

    features: Dict[str, FeatureDriftScore] = Field(
        default_factory=dict, description="Per-feature drift metrics"
    )
    last_checked: datetime | None = Field(
        None, description="Timestamp of the last completed drift evaluation"
    )
    retrain_requested: bool = Field(
        False, description="Whether a retraining job is currently requested"
    )
    last_retrain_at: datetime | None = Field(
        None, description="Timestamp when a retrain was last triggered"
    )
    canary: CanaryStatus | None = Field(
        None, description="Current canary deployment status if enabled"
    )


def _ensure_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


@dataclass
class CanaryDeploymentManager:
    """State machine coordinating MLflow stage transitions for canary rollouts."""

    model_name: str
    promote_after_trades: int
    tracking_uri: str | None = None
    registry_uri: str | None = None
    _active_version: str | None = field(default=None, init=False)
    _previous_production: str | None = field(default=None, init=False)
    _trades_seen: int = field(default=0, init=False)
    _rollbacks: int = field(default=0, init=False)
    _last_promotion: datetime | None = field(default=None, init=False)

    def _client(self) -> MlflowClient | None:
        if MlflowClient is None or mlflow is None:  # pragma: no cover - depends on mlflow.
            LOGGER.warning("MLflow client unavailable; skipping registry operations.")
            return None

        if self.tracking_uri:
            mlflow.set_tracking_uri(self.tracking_uri)
        if self.registry_uri:
            mlflow.set_registry_uri(self.registry_uri)
        return MlflowClient()

    def deploy(self, model_version: str) -> None:
        """Move the supplied model version into the canary stage."""

        client = self._client()
        if client is not None:  # pragma: no branch - simple branch.
            try:
                latest_prod = client.get_latest_versions(self.model_name, stages=[PRODUCTION_STAGE])
                if latest_prod:
                    self._previous_production = latest_prod[0].version
            except Exception as exc:  # pragma: no cover - requires mlflow backend.
                LOGGER.warning("Unable to resolve latest production version: %s", exc)
            try:
                client.transition_model_version_stage(
                    name=self.model_name,
                    version=model_version,
                    stage=CANARY_STAGE,
                    archive_existing_versions=False,
                )
            except Exception as exc:  # pragma: no cover - requires mlflow backend.
                LOGGER.error("Failed to transition model %s to canary: %s", model_version, exc)
        else:
            LOGGER.info(
                "MLflow client not available. Recording canary version %s without registry transition.",
                model_version,
            )

        self._active_version = model_version
        self._trades_seen = 0

    def observe_trade(
        self,
        *,
        drift_alerts: Sequence[str],
        degraded_metrics: Mapping[str, float] | None,
        has_metrics: bool,
        trade_metrics: Mapping[str, float] | None = None,
    ) -> None:
        """Record an executed trade for stability checks."""

        if self._active_version is None:
            return

        if drift_alerts:
            LOGGER.info(
                "Drift detected while canary version %s active. Triggering rollback.",
                self._active_version,
            )
            self._rollback()
            return

        if degraded_metrics:
            LOGGER.info(
                "Performance degradation observed for canary %s: %s. Triggering rollback.",
                self._active_version,
                ", ".join(f"{metric}={value:.4f}" for metric, value in degraded_metrics.items()),
            )
            self._rollback()
            return

        self._trades_seen += 1
        LOGGER.debug(
            "Observed trade %d/%d for canary %s", self._trades_seen, self.promote_after_trades, self._active_version
        )
        if self._trades_seen >= self.promote_after_trades:
            if not has_metrics:
                LOGGER.debug(
                    "Skipping canary promotion for %s because no drift metrics are available yet.",
                    self._active_version,
                )
                return
            self._promote()

    def _promote(self) -> None:
        if self._active_version is None:
            return

        client = self._client()
        if client is not None:  # pragma: no branch - simple branch.
            try:
                client.transition_model_version_stage(
                    name=self.model_name,
                    version=self._active_version,
                    stage=PRODUCTION_STAGE,
                    archive_existing_versions=False,
                )
                if self._previous_production and self._previous_production != self._active_version:
                    client.transition_model_version_stage(
                        name=self.model_name,
                        version=self._previous_production,
                        stage=ARCHIVED_STAGE,
                        archive_existing_versions=False,
                    )
                    LOGGER.info(
                        "Archived previous production model version %s after promoting %s.",
                        self._previous_production,
                        self._active_version,
                    )
                self._previous_production = self._active_version
            except Exception as exc:  # pragma: no cover - requires mlflow backend.
                LOGGER.error("Failed to promote canary %s to production: %s", self._active_version, exc)
                return

        self._last_promotion = datetime.now(timezone.utc)
        LOGGER.info(
            "Promoted canary model version %s to production after %d trades.",
            self._active_version,
            self._trades_seen,
        )
        # Reset canary tracking – the promoted version becomes production.
        self._active_version = None
        self._trades_seen = 0

    def _rollback(self) -> None:
        if self._active_version is None:
            return

        client = self._client()
        if client is not None:  # pragma: no branch - simple branch.
            try:
                if self._previous_production is not None:
                    client.transition_model_version_stage(
                        name=self.model_name,
                        version=self._previous_production,
                        stage=PRODUCTION_STAGE,
                        archive_existing_versions=False,
                    )
                client.transition_model_version_stage(
                    name=self.model_name,
                    version=self._active_version,
                    stage=ARCHIVED_STAGE,
                    archive_existing_versions=False,
                )
            except Exception as exc:  # pragma: no cover - requires mlflow backend.
                LOGGER.error("Failed to rollback canary %s: %s", self._active_version, exc)

        LOGGER.info(
            "Rolled back canary model version %s to previous production %s.",
            self._active_version,
            self._previous_production,
        )
        self._trades_seen = 0
        self._active_version = None
        self._rollbacks += 1

    def status(self) -> CanaryStatus:
        return CanaryStatus(
            active_version=self._active_version,
            previous_production=self._previous_production,
            trades_observed=self._trades_seen,
            promote_after=self.promote_after_trades,
            last_promotion_at=_ensure_datetime(self._last_promotion),
            rollbacks=self._rollbacks,
            mlflow_enabled=MlflowClient is not None and mlflow is not None,
        )


def _require_pandas() -> ModuleType:
    if pd is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "The pandas library is required for drift monitoring. Install pandas to evaluate "
                "drift metrics or disable the drift monitoring service."
            ),
        )
    return pd


def _coerce_dataframe(value: Any) -> Any:
    pandas = _require_pandas()
    if value is None:
        return pandas.DataFrame()
    if isinstance(value, pandas.DataFrame):
        return value
    return pandas.DataFrame(value)


class DriftMonitoringService:
    """Evaluate feature drift and manage automated retraining workflows."""

    def __init__(
        self,
        *,
        baseline: Any | None = None,
        psi_threshold: float = 0.2,
        ks_threshold: float = 0.1,
        retrain_callback: Callable[[Sequence[DriftReport]], str | None] | None = None,
        canary_model_name: str | None = None,
        canary_promote_after_trades: int = 50,
        tracking_uri: str | None = None,
        registry_uri: str | None = None,
        performance_thresholds: Mapping[str, Tuple[float | None, float | None]] | None = None,
    ) -> None:
        self.psi_threshold = psi_threshold
        self.ks_threshold = ks_threshold
        if baseline is not None and pd is not None:
            self._baseline: Any | None = pd.DataFrame(baseline)
        elif pd is not None:
            self._baseline = pd.DataFrame()
        else:
            self._baseline = None
        self._retrain_callback = retrain_callback
        self._metrics: MutableMapping[str, FeatureDriftScore] = {}
        self._last_checked: datetime | None = None
        self._retrain_requested = False
        self._last_retrain_at: datetime | None = None
        self._performance_thresholds: Dict[str, Tuple[float | None, float | None]] = (
            dict(performance_thresholds) if performance_thresholds else {}
        )
        self._canary_manager = (
            CanaryDeploymentManager(
                model_name=canary_model_name,
                promote_after_trades=canary_promote_after_trades,
                tracking_uri=tracking_uri,
                registry_uri=registry_uri,
            )
            if canary_model_name
            else None
        )

    @property
    def baseline(self) -> Any:
        if self._baseline is None:
            self._baseline = _coerce_dataframe(None)
        return self._baseline

    def set_baseline(self, baseline: Any) -> None:
        frame = _coerce_dataframe(baseline)
        if frame.empty:
            raise ValueError("Baseline dataframe must contain at least one feature.")
        self._baseline = frame

    def evaluate(self, production: Any) -> Sequence[DriftReport]:
        baseline_df = self.baseline
        if baseline_df.empty:
            raise HTTPException(
                status_code=status.HTTP_412_PRECONDITION_FAILED,
                detail="Baseline dataframe has not been initialised",
            )

        production_df = _coerce_dataframe(production)

        try:
            reports = generate_drift_report(
                baseline_df=baseline_df,
                production_df=production_df,
                psi_threshold=self.psi_threshold,
                ks_threshold=self.ks_threshold,
            )
        except MissingDependencyError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=str(exc),
            ) from exc
        evaluated_at = datetime.now(timezone.utc)
        self._last_checked = evaluated_at
        self._metrics = {
            report.feature: FeatureDriftScore(
                psi=report.population_stability_index,
                psi_alert=report.psi_alert,
                ks=report.kolmogorov_smirnov,
                ks_alert=report.ks_alert,
                severity=report.severity,
                alert=report.alert,
                checked_at=evaluated_at,
            )
            for report in reports
        }

        alerts = [report for report in reports if report.alert]
        if alerts:
            self._trigger_retrain(alerts)

        return reports

    def _trigger_retrain(self, alerts: Sequence[DriftReport]) -> None:
        max_severity = max((report.severity for report in alerts), default=0.0)
        LOGGER.warning(
            "Feature drift detected for %s (max severity %.2f). Initiating retrain pipeline.",
            ", ".join(report.feature for report in alerts),
            max_severity,
        )
        self._retrain_requested = True
        self._last_retrain_at = datetime.now(timezone.utc)

        if self._retrain_callback is None:
            LOGGER.debug("No retrain callback configured; skipping downstream trigger.")
            return

        try:
            model_version = self._retrain_callback(alerts)
        except Exception as exc:  # pragma: no cover - depends on callback implementation.
            LOGGER.exception("Retrain callback raised an exception: %s", exc)
            return

        if model_version:
            self.notify_model_ready(model_version)

    def notify_model_ready(self, model_version: str) -> None:
        LOGGER.info("Retrain completed with model version %s", model_version)
        self._retrain_requested = False
        # Reset drift metrics captured for the previous production model so the
        # canary evaluation starts from a clean slate. Stale alerts would
        # otherwise cause an immediate rollback before the new model can
        # observe any trades.
        self._metrics = {}
        self._last_checked = None
        if self._canary_manager is not None:
            self._canary_manager.deploy(model_version)

    def record_trade(self, trade_metrics: Mapping[str, float] | None = None) -> None:
        if self._canary_manager is None:
            return

        alerts = [feature for feature, metric in self._metrics.items() if metric.alert]
        has_metrics = bool(self._metrics)
        degraded_metrics = self._detect_performance_degradation(trade_metrics)
        self._canary_manager.observe_trade(
            drift_alerts=alerts,
            degraded_metrics=degraded_metrics if degraded_metrics else None,
            has_metrics=has_metrics,
            trade_metrics=trade_metrics,
        )

    def status(self) -> DriftStatusResponse:
        return DriftStatusResponse(
            features=dict(self._metrics),
            last_checked=_ensure_datetime(self._last_checked),
            retrain_requested=self._retrain_requested,
            last_retrain_at=_ensure_datetime(self._last_retrain_at),
            canary=self._canary_manager.status() if self._canary_manager else None,
        )

    def _detect_performance_degradation(
        self, trade_metrics: Mapping[str, float] | None
    ) -> Dict[str, float]:
        if not trade_metrics or not self._performance_thresholds:
            return {}

        degraded: Dict[str, float] = {}
        for metric, value in trade_metrics.items():
            bounds = self._performance_thresholds.get(metric)
            if bounds is None:
                continue
            lower, upper = bounds
            if lower is not None and value < lower:
                degraded[metric] = value
                continue
            if upper is not None and value > upper:
                degraded[metric] = value
        return degraded


# ---------------------------------------------------------------------------
# FastAPI router wiring
# ---------------------------------------------------------------------------


router = APIRouter(prefix="/drift", tags=["drift"])


def _default_service() -> DriftMonitoringService:
    """Instantiate a module-level service lazily for dependency injection."""

    global _SERVICE_INSTANCE  # type: ignore  # lazy singleton for FastAPI dependency.
    try:
        instance = _SERVICE_INSTANCE  # type: ignore[name-defined]
    except NameError:
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
        registry_uri = os.getenv("MLFLOW_REGISTRY_URI")
        model_name = os.getenv("MLFLOW_MODEL_NAME")
        promote_after = int(os.getenv("CANARY_PROMOTE_AFTER_TRADES", "50"))
        instance = DriftMonitoringService(
            canary_model_name=model_name,
            canary_promote_after_trades=promote_after,
            tracking_uri=tracking_uri,
            registry_uri=registry_uri,
        )
        _SERVICE_INSTANCE = instance  # type: ignore[name-defined]
    return instance


def get_drift_monitoring_service() -> DriftMonitoringService:
    return _default_service()


@router.get("/status", response_model=DriftStatusResponse)
def get_drift_status(service: DriftMonitoringService = Depends(get_drift_monitoring_service)) -> DriftStatusResponse:
    """Return the most recent drift evaluation metrics."""

    return service.status()


__all__ = [
    "CanaryDeploymentManager",
    "DriftMonitoringService",
    "DriftStatusResponse",
    "FeatureDriftScore",
    "CanaryStatus",
    "router",
    "get_drift_monitoring_service",
]
