from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from typing import Dict, List


# ---------------------------------------------------------------------------
# Lightweight stubs for optional dependencies
# ---------------------------------------------------------------------------

schemas_stub = types.ModuleType("services.common.schemas")


@dataclass
class ActionTemplate:
    name: str
    venue_type: str
    edge_bps: float
    fee_bps: float
    confidence: float


@dataclass
class ConfidenceMetrics:
    model_confidence: float
    state_confidence: float
    execution_confidence: float
    overall_confidence: float | None = None

    def __post_init__(self) -> None:
        if self.overall_confidence is None:
            composite = (
                self.model_confidence + self.state_confidence + self.execution_confidence
            ) / 3.0
            self.overall_confidence = round(composite, 4)


@dataclass
class BookSnapshot:
    mid_price: float
    spread_bps: float
    imbalance: float


schemas_stub.ActionTemplate = ActionTemplate
schemas_stub.ConfidenceMetrics = ConfidenceMetrics
schemas_stub.BookSnapshot = BookSnapshot

sys.modules.setdefault("services.common.schemas", schemas_stub)

from services.policy import model_server as policy_model_server


_STAGE_ALIASES = {
    "canary": "Canary",
    "staging": "Staging",
    "prod": "Production",
    "production": "Production",
    "archived": "Archived",
}


@dataclass
class _ModelEntry:
    version: int
    stage: str
    model: policy_model_server.DummyPolicyModel


class CanaryAwareMlflowStub:
    """Test double emulating MLflow stage transitions for policy models."""

    def __init__(self) -> None:
        self._registry: Dict[str, List[_ModelEntry]] = {}
        self._active_versions: Dict[str, int] = {}
        self._version_counters: Dict[str, int] = {}
        self._last_loaded: Dict[str, str] = {}

    def _normalize_stage(self, stage: str) -> str:
        normalized = _STAGE_ALIASES.get(stage.lower())
        if normalized is None:
            allowed = ", ".join(sorted(_STAGE_ALIASES))
            raise ValueError(f"Unsupported stage '{stage}'. Expected one of: {allowed}.")
        return normalized

    def load_latest_model(self, name: str) -> policy_model_server.DummyPolicyModel:
        if name not in self._active_versions:
            baseline = policy_model_server.DummyPolicyModel(f"{name}::baseline")
            entry = _ModelEntry(version=0, stage="Production", model=baseline)
            self._registry.setdefault(name, []).append(entry)
            self._active_versions[name] = 0
            self._version_counters.setdefault(name, 0)
        active_version = self._active_versions[name]
        for entry in self._registry.get(name, []):
            if entry.version == active_version:
                self._last_loaded[name] = entry.model.name
                return entry.model
        raise LookupError(f"Active version {active_version} for {name} is missing")

    def log_model(self, name: str, stage: str) -> int:
        stage_label = self._normalize_stage(stage)
        version = self._version_counters.get(name, 0) + 1
        self._version_counters[name] = version
        model = policy_model_server.DummyPolicyModel(f"{name}::v{version}")
        entry = _ModelEntry(version=version, stage=stage_label, model=model)
        self._registry.setdefault(name, []).append(entry)
        if stage_label == "Production":
            self._active_versions[name] = version
        return version

    def promote(self, name: str, version: int, stage: str = "Production") -> None:
        stage_label = self._normalize_stage(stage)
        entries = self._registry.get(name, [])
        if not entries:
            raise ValueError(f"No models registered for {name}")
        target = next((entry for entry in entries if entry.version == version), None)
        if target is None:
            raise ValueError(f"Version {version} is not registered for {name}")
        target.stage = stage_label
        if stage_label == "Production":
            for entry in entries:
                if entry is not target and entry.stage == "Production":
                    entry.stage = "Archived"
            self._active_versions[name] = version

    def rollback(self, name: str) -> None:
        entries = self._registry.get(name, [])
        if not entries:
            raise ValueError(f"No models registered for {name}")
        archived = [entry for entry in entries if entry.stage == "Archived"]
        if not archived:
            raise RuntimeError(f"No archived versions available to rollback for {name}")
        target = max(archived, key=lambda entry: entry.version)
        current_version = self._active_versions.get(name)
        for entry in entries:
            if entry is target:
                entry.stage = "Production"
            elif entry.stage == "Production" and entry.version != target.version:
                entry.stage = "Archived"
        if current_version is not None:
            prev = next((entry for entry in entries if entry.version == current_version), None)
            if prev is not None and prev is not target:
                prev.stage = "Archived"
        self._active_versions[name] = target.version

    # ------------------------------------------------------------------
    # Introspection helpers for assertions
    # ------------------------------------------------------------------
    def current_version(self, name: str) -> int | None:
        return self._active_versions.get(name)

    def get_stage(self, name: str, version: int) -> str | None:
        for entry in self._registry.get(name, []):
            if entry.version == version:
                return entry.stage
        return None

    def last_loaded_name(self, name: str) -> str | None:
        return self._last_loaded.get(name)


def test_model_promotion_and_rollback(monkeypatch) -> None:
    client_stub = CanaryAwareMlflowStub()
    monkeypatch.setattr(policy_model_server, "_client", client_stub)

    account_id = "acct-primary"
    symbol = "BTC-USD"
    variant = "trend_model"
    model_key = policy_model_server._model_name(account_id, symbol, variant)

    features = [0.12, -0.08, 0.45, 0.33]
    book_snapshot = {"mid_price": 28150.0, "spread_bps": 6.0, "imbalance": 0.1}

    def infer():
        return policy_model_server.predict_intent(
            account_id=account_id,
            symbol=symbol,
            features=features,
            book_snapshot=book_snapshot,
            model_variant=variant,
        )

    infer()
    baseline_model_name = client_stub.last_loaded_name(model_key)
    baseline_version = client_stub.current_version(model_key)

    assert baseline_model_name is not None
    assert baseline_version == 0
    assert client_stub.get_stage(model_key, baseline_version) == "Production"

    canary_version = client_stub.log_model(model_key, stage="canary")
    assert client_stub.get_stage(model_key, canary_version) == "Canary"

    _ = infer()
    assert client_stub.current_version(model_key) == baseline_version
    assert client_stub.last_loaded_name(model_key) == baseline_model_name

    client_stub.promote(model_key, canary_version, stage="prod")
    assert client_stub.current_version(model_key) == canary_version
    assert client_stub.get_stage(model_key, canary_version) == "Production"
    assert client_stub.get_stage(model_key, baseline_version) == "Archived"

    promoted_intent = infer()
    promoted_model_name = client_stub.last_loaded_name(model_key)

    assert promoted_model_name is not None
    assert promoted_model_name != baseline_model_name
    assert isinstance(promoted_intent.action_templates, list)
    assert promoted_intent.action_templates, "Expected promoted intent to provide action templates"

    client_stub.rollback(model_key)
    assert client_stub.current_version(model_key) == baseline_version
    assert client_stub.get_stage(model_key, baseline_version) == "Production"
    assert client_stub.get_stage(model_key, canary_version) == "Archived"

    rolled_back_intent = infer()
    rolled_back_model_name = client_stub.last_loaded_name(model_key)

    assert rolled_back_model_name == baseline_model_name
    assert isinstance(rolled_back_intent.action_templates, list)
