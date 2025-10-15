"""Model registry management endpoints backed by MLflow or in-memory stubs.

This module exposes a small "model zoo" service responsible for:

* Tracking the available model versions per trading symbol/strategy pair.
* Promoting a specific model version to an active stage (e.g. production).
* Returning the current active model selections for the organisation.
* Recording every promotion in a persistent audit log structure.

In the real deployment these operations would talk to MLflow.  For unit tests
and local development MLflow is optional – a deterministic in-memory registry
is provided so the API can still be exercised.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

try:  # pragma: no cover - prefer the real FastAPI implementation when available
    from fastapi import APIRouter, Depends, HTTPException, Query, status
except Exception:  # pragma: no cover - exercised when FastAPI is unavailable
    from services.common.fastapi_stub import (  # type: ignore[assignment]
        APIRouter,
        Depends,
        HTTPException,
        Query,
        status,
    )
from pydantic import BaseModel, Field

from services.common.security import require_admin_account

logger = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency
    from mlflow.entities.model_registry import ModelVersion
    from mlflow.exceptions import MlflowException
    from mlflow.tracking import MlflowClient
    from shared.mlflow_safe import harden_mlflow

    harden_mlflow()
except Exception:  # pragma: no cover - defensive guard for environments without mlflow
    ModelVersion = object  # type: ignore[assignment]
    MlflowException = Exception  # type: ignore
    MlflowClient = None  # type: ignore[assignment]


_ALLOWED_STAGE_ALIASES = {
    "staging": "Staging",
    "canary": "Canary",
    "prod": "Production",
    "production": "Production",
    "production_ready": "Production",
    "archived": "Archived",
}


@dataclass
class ModelVersionMetadata:
    """Internal representation of a registered model version."""

    name: str
    version: int
    stage: str
    run_id: Optional[str] = None
    source: Optional[str] = None
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    tags: MutableMapping[str, str] = field(default_factory=dict)
    metrics: MutableMapping[str, float] = field(default_factory=dict)

    @property
    def model_id(self) -> str:
        return f"{self.name}:{self.version}"

    @classmethod
    def from_mlflow(cls, version: ModelVersion) -> "ModelVersionMetadata":  # pragma: no cover - mlflow optional
        created_at = getattr(version, "creation_timestamp", None)
        if isinstance(created_at, (int, float)):
            created_at = datetime.fromtimestamp(created_at / 1000.0, tz=timezone.utc)
        elif created_at is None:
            created_at = None

        last_updated = getattr(version, "last_updated_timestamp", None)
        if isinstance(last_updated, (int, float)):
            updated_at = datetime.fromtimestamp(last_updated / 1000.0, tz=timezone.utc)
        else:
            updated_at = created_at

        description = getattr(version, "description", None)
        source = getattr(version, "source", None)
        run_id = getattr(version, "run_id", None)
        stage = getattr(version, "current_stage", "")
        tags = dict(getattr(version, "tags", {}) or {})

        return cls(
            name=getattr(version, "name", "unknown"),
            version=int(getattr(version, "version", 0) or 0),
            stage=stage,
            run_id=run_id,
            source=source,
            description=description,
            created_at=created_at,
            updated_at=updated_at,
            tags=tags,
            metrics={},
        )

    def to_payload(self) -> "ModelVersionPayload":
        return ModelVersionPayload(
            model_id=self.model_id,
            name=self.name,
            version=self.version,
            stage=self.stage,
            run_id=self.run_id,
            source=self.source,
            description=self.description,
            created_at=self.created_at,
            updated_at=self.updated_at,
            tags=dict(self.tags),
            metrics=dict(self.metrics),
        )


@dataclass
class ModelSwitchLogEntry:
    """Record representing a model promotion event."""

    symbol: str
    strategy: str
    model_id: str
    stage: str
    actor: str
    ts: datetime

    def to_payload(self) -> "ActiveModelEntry":
        return ActiveModelEntry(
            symbol=self.symbol,
            strategy=self.strategy,
            model_id=self.model_id,
            stage=self.stage,
            actor=self.actor,
            ts=self.ts,
        )


@dataclass
class SymbolInventory:
    symbol: str
    strategy: str
    versions: Sequence[ModelVersionMetadata]
    active: Optional[ModelSwitchLogEntry]

    def to_payload(self) -> "ModelInventoryEntry":
        return ModelInventoryEntry(
            symbol=self.symbol,
            strategy=self.strategy,
            versions=[version.to_payload() for version in self.versions],
            active_model=self.active.to_payload() if self.active else None,
        )


class ModelVersionPayload(BaseModel):
    """Public view of a registered model version."""

    model_id: str
    name: str
    version: int
    stage: str
    run_id: Optional[str] = None
    source: Optional[str] = None
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    tags: Mapping[str, str] = Field(default_factory=dict)
    metrics: Mapping[str, float] = Field(default_factory=dict)


class ActiveModelEntry(BaseModel):
    symbol: str
    strategy: str
    model_id: str
    stage: str
    actor: str
    ts: datetime

    @classmethod
    def from_entry(cls, entry: ModelSwitchLogEntry) -> "ActiveModelEntry":
        return entry.to_payload()


class ModelInventoryEntry(BaseModel):
    symbol: str
    strategy: str
    versions: List[ModelVersionPayload]
    active_model: Optional[ActiveModelEntry] = None


class ModelInventoryResponse(BaseModel):
    models: List[ModelInventoryEntry]


class ActiveModelsResponse(BaseModel):
    active: List[ActiveModelEntry]


class SwitchModelRequest(BaseModel):
    symbol: str
    model_id: str
    stage: str
    strategy: str | None = Field(default="default", description="Logical strategy variant")


class SwitchModelResponse(BaseModel):
    status: str
    active_model: ActiveModelEntry


def _normalize_stage(stage: str) -> str:
    normalized = _ALLOWED_STAGE_ALIASES.get(stage.strip().lower())
    if normalized is None:
        allowed = ", ".join(sorted(_ALLOWED_STAGE_ALIASES))
        raise ValueError(f"Unsupported stage '{stage}'. Expected one of: {allowed}.")
    return normalized


def _normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError("symbol must not be empty")
    return normalized


def _normalize_strategy(strategy: Optional[str]) -> str:
    if strategy is None:
        return "default"
    normalized = strategy.strip().lower()
    if not normalized:
        return "default"
    return normalized


class ModelZoo:
    """In-memory facade mirroring the behaviour of the production model registry."""

    def __init__(self) -> None:
        self._models: Dict[Tuple[str, str], List[ModelVersionMetadata]] = {}
        self._active: Dict[Tuple[str, str], ModelSwitchLogEntry] = {}
        self._switch_log: List[ModelSwitchLogEntry] = []
        self._registry_names: Dict[Tuple[str, str], str] = {}
        self._client = self._build_client()
        self._seed_dummy_models()

    @staticmethod
    def _build_client() -> Optional[MlflowClient]:
        if MlflowClient is None:  # mlflow not installed
            return None
        try:  # pragma: no cover - network dependency
            return MlflowClient()
        except Exception as exc:  # pragma: no cover - safety guard
            logger.warning("Failed to initialise MLflow client: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def inventory(
        self, *, symbol: Optional[str] = None, strategy: Optional[str] = None
    ) -> List[SymbolInventory]:
        keys = self._matching_keys(symbol=symbol, strategy=strategy)
        inventory: List[SymbolInventory] = []
        for key in keys:
            versions = list(self._load_versions_for_key(key))
            active = self._active.get(key)
            inventory.append(
                SymbolInventory(
                    symbol=key[0],
                    strategy=key[1],
                    versions=versions,
                    active=active,
                )
            )
        return inventory

    def list_switch_log(
        self, *, symbol: Optional[str] = None, strategy: Optional[str] = None, limit: Optional[int] = None
    ) -> List[ModelSwitchLogEntry]:
        symbol_key = _normalize_symbol(symbol) if symbol else None
        strategy_key = _normalize_strategy(strategy) if strategy else None
        entries = [
            entry
            for entry in self._switch_log
            if (symbol_key is None or entry.symbol == symbol_key)
            and (strategy_key is None or entry.strategy == strategy_key)
        ]
        entries.sort(key=lambda record: record.ts, reverse=True)
        if limit is not None:
            entries = entries[: int(limit)]
        return list(entries)

    def active_models(
        self, *, symbol: Optional[str] = None, strategy: Optional[str] = None
    ) -> List[ModelSwitchLogEntry]:
        symbol_key = _normalize_symbol(symbol) if symbol else None
        strategy_key = _normalize_strategy(strategy) if strategy else None
        entries: List[ModelSwitchLogEntry] = []
        for key, entry in self._active.items():
            if symbol_key is not None and key[0] != symbol_key:
                continue
            if strategy_key is not None and key[1] != strategy_key:
                continue
            entries.append(entry)
        entries.sort(key=lambda record: (record.symbol, record.strategy))
        return list(entries)

    def switch_model(
        self,
        *,
        symbol: str,
        model_id: str,
        stage: str,
        actor: str,
        strategy: Optional[str] = None,
    ) -> ModelSwitchLogEntry:
        normalized_symbol = _normalize_symbol(symbol)
        normalized_strategy = _normalize_strategy(strategy)
        stage_label = _normalize_stage(stage)

        key = (normalized_symbol, normalized_strategy)
        versions = list(self._load_versions_for_key(key))
        if not versions:
            raise ValueError(
                f"No models registered for symbol '{normalized_symbol}' and strategy '{normalized_strategy}'."
            )

        name, version_number, canonical_model_id = self._resolve_model_identifier(key, model_id)
        metadata = self._find_version(versions, canonical_model_id)
        if metadata is None:
            raise ValueError(
                f"Model '{canonical_model_id}' is not registered for symbol '{normalized_symbol}' (strategy '{normalized_strategy}')."
            )

        self._transition_stage(name=name, version=version_number, stage=stage_label)

        now = datetime.now(timezone.utc)
        for candidate in versions:
            if candidate.model_id == canonical_model_id:
                candidate.stage = stage_label
                candidate.updated_at = now
            elif stage_label == "Production" and candidate.stage == "Production":
                candidate.stage = "Archived"
        versions.sort(key=lambda item: item.version, reverse=True)
        self._models[key] = versions

        entry = ModelSwitchLogEntry(
            symbol=normalized_symbol,
            strategy=normalized_strategy,
            model_id=canonical_model_id,
            stage=stage_label,
            actor=actor,
            ts=now,
        )
        self._active[key] = entry
        self._switch_log.append(entry)
        return entry

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _matching_keys(
        self, *, symbol: Optional[str], strategy: Optional[str]
    ) -> List[Tuple[str, str]]:
        symbol_key = _normalize_symbol(symbol) if symbol else None
        strategy_key = _normalize_strategy(strategy) if strategy else None
        known_keys = set(self._models.keys()) | set(self._active.keys())
        keys: List[Tuple[str, str]] = []
        for key in known_keys:
            if symbol_key is not None and key[0] != symbol_key:
                continue
            if strategy_key is not None and key[1] != strategy_key:
                continue
            keys.append(key)
        keys.sort()
        return keys

    def _load_versions_for_key(self, key: Tuple[str, str]) -> Sequence[ModelVersionMetadata]:
        registry_name = self._registry_names.get(key)
        if registry_name and self._client is not None:  # pragma: no cover - requires mlflow
            try:
                versions = self._client.search_model_versions(f"name='{registry_name}'")
            except MlflowException as exc:
                logger.warning(
                    "Failed to fetch model versions for '%s': %s", registry_name, exc
                )
            else:
                converted = [ModelVersionMetadata.from_mlflow(version) for version in versions]
                converted.sort(key=lambda item: item.version, reverse=True)
                self._models[key] = converted
                return converted
        return list(self._models.get(key, []))

    def _resolve_model_identifier(
        self, key: Tuple[str, str], model_id: str
    ) -> Tuple[str, int, str]:
        if ":" in model_id:
            name, version_str = model_id.split(":", 1)
        else:
            name = self._registry_names.get(key)
            version_str = model_id
        if not name:
            raise ValueError(
                f"Registry name is unknown for symbol '{key[0]}' (strategy '{key[1]}')."
            )
        try:
            version_number = int(version_str)
        except ValueError as exc:
            raise ValueError(f"Invalid model identifier '{model_id}'.") from exc
        canonical_id = f"{name}:{version_number}"
        self._registry_names[key] = name
        return name, version_number, canonical_id

    def _find_version(
        self, versions: Sequence[ModelVersionMetadata], model_id: str
    ) -> Optional[ModelVersionMetadata]:
        for metadata in versions:
            if metadata.model_id == model_id:
                return metadata
        return None

    def _transition_stage(self, *, name: str, version: int, stage: str) -> None:
        if self._client is None:
            logger.debug(
                "MLflow client unavailable. Updating local registry for %s:%s to %s.",
                name,
                version,
                stage,
            )
            return
        try:  # pragma: no cover - requires mlflow
            self._client.transition_model_version_stage(
                name=name,
                version=str(version),
                stage=stage,
                archive_existing_versions=False,
            )
        except MlflowException as exc:  # pragma: no cover - defensive guard
            raise RuntimeError(
                f"Failed to promote model '{name}:{version}' to stage '{stage}': {exc}"
            ) from exc

    def _seed_dummy_models(self) -> None:
        now = datetime.now(timezone.utc)
        seeds = [
            {
                "symbol": "BTC-USD",
                "strategy": "policy",
                "name": "policy-intent::btc-usd::policy",
                "version": 3,
                "stage": "Production",
                "run_id": "run-btc-003",
                "metrics": {"precision": 0.72, "recall": 0.69},
            },
            {
                "symbol": "BTC-USD",
                "strategy": "policy",
                "name": "policy-intent::btc-usd::policy",
                "version": 2,
                "stage": "Archived",
                "run_id": "run-btc-002",
                "metrics": {"precision": 0.70, "recall": 0.65},
            },
            {
                "symbol": "ETH-USD",
                "strategy": "policy",
                "name": "policy-intent::eth-usd::policy",
                "version": 1,
                "stage": "Staging",
                "run_id": "run-eth-001",
                "metrics": {"precision": 0.68, "recall": 0.66},
            },
        ]
        for seed in seeds:
            self._register_seed(**seed, now=now)

        for key, versions in self._models.items():
            active = next((version for version in versions if version.stage == "Production"), None)
            if active is None:
                continue
            ts = active.updated_at or active.created_at or now
            entry = ModelSwitchLogEntry(
                symbol=key[0],
                strategy=key[1],
                model_id=active.model_id,
                stage=active.stage,
                actor="system",
                ts=ts,
            )
            self._active[key] = entry
            self._switch_log.append(entry)

    def _register_seed(
        self,
        *,
        symbol: str,
        strategy: str,
        name: str,
        version: int,
        stage: str,
        run_id: str,
        metrics: Mapping[str, float],
        now: datetime,
    ) -> None:
        normalized_symbol = _normalize_symbol(symbol)
        normalized_strategy = _normalize_strategy(strategy)
        key = (normalized_symbol, normalized_strategy)
        created_at = now - timedelta(days=max(0, version))
        metadata = ModelVersionMetadata(
            name=name,
            version=version,
            stage=stage,
            run_id=run_id,
            source=f"mlruns/{name}/{run_id}",
            description=f"Dummy model for {normalized_symbol}::{normalized_strategy}",
            created_at=created_at,
            updated_at=created_at + timedelta(hours=version),
            tags={"symbol": normalized_symbol, "strategy": normalized_strategy},
            metrics=dict(metrics),
        )
        entries = self._models.setdefault(key, [])
        entries = [entry for entry in entries if entry.version != version]
        entries.append(metadata)
        entries.sort(key=lambda item: item.version, reverse=True)
        self._models[key] = entries
        self._registry_names[key] = name


_ROUTER = APIRouter(prefix="/models", tags=["models"])
_ZOO: ModelZoo | None = None


def get_model_zoo() -> ModelZoo:
    global _ZOO
    if _ZOO is None:
        _ZOO = ModelZoo()
    return _ZOO


@_ROUTER.get("/list", response_model=ModelInventoryResponse)
def list_models(
    symbol: Optional[str] = Query(default=None),
    strategy: Optional[str] = Query(default=None),
    _: str = Depends(require_admin_account),
) -> ModelInventoryResponse:
    zoo = get_model_zoo()
    inventory = zoo.inventory(symbol=symbol, strategy=strategy)
    payload = [entry.to_payload() for entry in inventory]
    return ModelInventoryResponse(models=payload)


@_ROUTER.get("/active", response_model=ActiveModelsResponse)
def active_models(
    symbol: Optional[str] = Query(default=None),
    strategy: Optional[str] = Query(default=None),
    _: str = Depends(require_admin_account),
) -> ActiveModelsResponse:
    zoo = get_model_zoo()
    entries = [ActiveModelEntry.from_entry(entry) for entry in zoo.active_models(symbol=symbol, strategy=strategy)]
    return ActiveModelsResponse(active=entries)


@_ROUTER.post("/switch", response_model=SwitchModelResponse)
def switch_model(
    payload: SwitchModelRequest,
    actor: str = Depends(require_admin_account),
) -> SwitchModelResponse:
    zoo = get_model_zoo()
    try:
        entry = zoo.switch_model(
            symbol=payload.symbol,
            strategy=payload.strategy,
            model_id=payload.model_id,
            stage=payload.stage,
            actor=actor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except RuntimeError as exc:  # transition failure
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    active = ActiveModelEntry.from_entry(entry)
    return SwitchModelResponse(status="ok", active_model=active)


router = _ROUTER

__all__ = [
    "ModelZoo",
    "ModelVersionMetadata",
    "ModelSwitchLogEntry",
    "get_model_zoo",
    "router",
]
