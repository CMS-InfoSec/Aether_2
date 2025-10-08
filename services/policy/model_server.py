"""MLflow-backed model server abstraction for the policy service."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from importlib import import_module
from types import ModuleType
from typing import Any, Dict, Iterable, List, Mapping, Protocol, Sequence, Tuple, TYPE_CHECKING, cast

from services.common.schemas import ActionTemplate, BookSnapshot, ConfidenceMetrics
from shared.spot import require_spot_symbol
class _PyFuncModel(Protocol):
    metadata: Any

    def predict(self, data: Any, params: Mapping[str, Any]) -> Any: ...


class _PyFuncModule(Protocol):
    def load_model(self, model_uri: str) -> _PyFuncModel: ...


class _ModelSignatureLike(Protocol):
    inputs: Any


class _MlflowClientProtocol(Protocol):
    def get_model_version_by_alias(self, name: str, alias: str) -> Any: ...

    def get_latest_versions(self, name: str, stages: Sequence[str]) -> Sequence[Any]: ...


PyFuncModel = _PyFuncModel
PyFuncModule = _PyFuncModule
ModelSignatureLike = _ModelSignatureLike
MlflowClientProtocol = _MlflowClientProtocol


if TYPE_CHECKING:  # pragma: no cover - imported for typing only.
    import pandas as pd
    from mlflow import pyfunc as mlflow_pyfunc
    from mlflow.models.signature import ModelSignature as MlflowModelSignature
    from mlflow.tracking import MlflowClient as MlflowClientType
else:  # pragma: no cover - executed at runtime.
    pd = cast(ModuleType | None, None)
    try:
        pd = import_module("pandas")
    except Exception:
        pd = None

    mlflow_pyfunc = None
    MlflowModelSignature = cast(type[Any] | None, None)
    MlflowClientType = cast(type[Any] | None, None)
    try:
        mlflow_pyfunc = import_module("mlflow.pyfunc")
        MlflowModelSignature = cast(
            type[Any], getattr(import_module("mlflow.models.signature"), "ModelSignature")
        )
        MlflowClientType = cast(
            type[Any], getattr(import_module("mlflow.tracking"), "MlflowClient")
        )
    except Exception:
        mlflow_pyfunc = None

MlflowException: type[Exception]
try:  # pragma: no cover - exercised when mlflow is unavailable locally.
    from mlflow.exceptions import MlflowException as _MlflowException
except Exception:  # pragma: no cover - gracefully degrade if mlflow missing.
    MlflowException = Exception
else:
    MlflowException = cast(type[Exception], _MlflowException)

pyfunc: PyFuncModule | None = cast(PyFuncModule | None, mlflow_pyfunc)
MlflowClient: type[MlflowClientProtocol] | None = cast(
    type[MlflowClientProtocol] | None, MlflowClientType
)
ModelSignature: type[ModelSignatureLike] | None = cast(
    type[ModelSignatureLike] | None, MlflowModelSignature
)

logger = logging.getLogger(__name__)

PRODUCTION_ALIAS = "production"


class ModelResolutionError(RuntimeError):
    """Raised when the production model cannot be located."""


class FeatureValidationError(ValueError):
    """Raised when the provided feature vector does not match the signature."""


class ModelInferenceError(RuntimeError):
    """Raised when the underlying model returns an invalid response."""


@dataclass
class Intent:
    """Represents a policy intent returned by the model server."""

    edge_bps: float
    confidence: ConfidenceMetrics
    take_profit_bps: float
    stop_loss_bps: float
    selected_action: str
    action_templates: List[ActionTemplate]
    approved: bool
    reason: str | None = None
    metadata: Dict[str, Any] | None = None

    @classmethod
    def null(
        cls,
        reason: str = "model_unavailable",
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> "Intent":
        """Return a do-nothing intent used when inference fails."""

        confidence = ConfidenceMetrics(
            model_confidence=0.0,
            state_confidence=0.0,
            execution_confidence=0.0,
            overall_confidence=0.0,
        )
        template = ActionTemplate(
            name="abstain",
            venue_type="none",
            edge_bps=0.0,
            fee_bps=0.0,
            confidence=0.0,
        )
        metadata_dict = dict(metadata or {}) or None
        return cls(
            edge_bps=0.0,
            confidence=confidence,
            take_profit_bps=0.0,
            stop_loss_bps=0.0,
            selected_action="abstain",
            action_templates=[template],
            approved=False,
            reason=reason,
            metadata=metadata_dict,
        )

    @property
    def is_null(self) -> bool:
        return not self.approved and self.edge_bps == 0.0 and self.selected_action == "abstain"


@dataclass
class CachedPolicyModel:
    """Container for a cached MLflow model and its signature metadata."""

    name: str
    version: str
    signature: ModelSignatureLike
    feature_names: Tuple[str, ...]
    feature_types: Tuple[str, ...]
    signature_hash: str
    model: PyFuncModel

    @classmethod
    def from_pyfunc(
        cls,
        name: str,
        version: str,
        model: PyFuncModel,
        signature: ModelSignatureLike,
    ) -> "CachedPolicyModel":
        if not hasattr(signature, "inputs"):
            raise ModelResolutionError("Model signature object does not expose inputs")

        inputs = getattr(signature, "inputs")
        if inputs is None:
            raise ModelResolutionError("Model signature is missing input schema")

        feature_names: List[str] = []
        feature_types: List[str] = []
        for idx, field in enumerate(inputs):
            name = getattr(field, "name", None) or f"feature_{idx}"
            feature_names.append(str(name))
            dtype = getattr(field, "type", None)
            feature_types.append(_describe_dtype(dtype))

        signature_hash = _signature_digest(inputs)

        return cls(
            name=name,
            version=version,
            signature=signature,
            feature_names=tuple(feature_names),
            feature_types=tuple(feature_types),
            signature_hash=signature_hash,
            model=model,
        )

    def explain(self, feature_values: Mapping[str, float] | Sequence[float]) -> Dict[str, float]:
        """Provide deterministic feature attributions mirroring legacy behaviour."""

        if isinstance(feature_values, Mapping):
            items: Iterable[Tuple[str, float]] = (
                (str(key), float(value)) for key, value in feature_values.items()
            )
        else:
            items = (
                (name, float(value))
                for name, value in zip(self.feature_names, feature_values)
            )

        values = list(items)
        if not values:
            return {}

        normaliser = float(len(values))
        if normaliser == 0:  # pragma: no cover - defensive guard
            return {}

        return {name: contribution / normaliser for name, contribution in values}


class PolicyModelRegistry:
    """MLflow-backed loader that caches production policy models."""

    def __init__(self, client: MlflowClientProtocol | None) -> None:
        self._client = client
        self._cache: Dict[str, CachedPolicyModel] = {}

    def load(self, name: str) -> CachedPolicyModel:
        client = self._client
        loader = pyfunc
        if client is None or loader is None:
            raise ModelResolutionError("MLflow client is not available")

        version_info = self._resolve_production_version(name)
        version = version_info.version

        cached = self._cache.get(name)
        if cached and cached.version == version:
            return cached

        model_uri = self._build_model_uri(name, version_info)
        logger.info("Loading policy model", extra={"model_uri": model_uri, "version": version})
        model = loader.load_model(model_uri)

        signature = getattr(model.metadata, "signature", None)
        if signature is None:
            info = getattr(model.metadata, "get_model_info", lambda: None)()
            signature = getattr(info, "signature", None)
        if signature is None:
            raise ModelResolutionError("Loaded model does not expose a signature")

        cached = CachedPolicyModel.from_pyfunc(
            name,
            version,
            model,
            cast(ModelSignatureLike, signature),
        )
        self._cache[name] = cached
        return cached

    def _resolve_production_version(self, name: str) -> Any:
        client = self._client
        if client is None:
            raise ModelResolutionError("MLflow client is not available")

        try:
            return client.get_model_version_by_alias(name, PRODUCTION_ALIAS)
        except MlflowException:
            logger.debug("Production alias not found for %s", name)

        try:
            versions = client.get_latest_versions(name, stages=["Production"])
        except MlflowException as exc:  # pragma: no cover - depends on mlflow backend
            raise ModelResolutionError(f"Unable to query model registry for {name}") from exc

        if not versions:
            raise ModelResolutionError(f"No production model registered for {name}")

        return versions[0]

    @staticmethod
    def _build_model_uri(name: str, version_info: Any) -> str:
        aliases = getattr(version_info, "aliases", None) or []
        if isinstance(aliases, (list, tuple)) and PRODUCTION_ALIAS in aliases:
            return f"models:/{name}@{PRODUCTION_ALIAS}"
        version = getattr(version_info, "version", None)
        if version is None:
            raise ModelResolutionError(f"Model version for {name} is undefined")
        return f"models:/{name}/{version}"


def _describe_dtype(dtype: Any) -> str:
    if dtype is None:
        return "unknown"

    to_string = getattr(dtype, "to_string", None)
    if callable(to_string):
        try:
            return str(to_string())
        except Exception:  # pragma: no cover - defensive formatting guard
            return str(dtype)

    return str(dtype)


def _signature_digest(schema: Any) -> str:
    if schema is None:
        raise ModelResolutionError("Model signature is missing input schema")
    payload = schema.to_dict() if hasattr(schema, "to_dict") else schema
    serialised = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha1(serialised.encode("utf-8")).hexdigest()


def _coerce_features(
    features: Sequence[float] | Mapping[str, float],
    model: CachedPolicyModel,
) -> List[float]:
    expected = len(model.feature_names)
    if isinstance(features, Mapping):
        ordered: List[float] = []
        for name in model.feature_names:
            if name not in features:
                raise FeatureValidationError(
                    f"Feature mapping is missing required field '{name}'"
                )
            try:
                ordered.append(float(features[name]))
            except (TypeError, ValueError) as exc:
                raise FeatureValidationError(
                    f"Feature '{name}' could not be coerced to float"
                ) from exc
    else:
        if len(features) != expected:
            raise FeatureValidationError(
                f"Expected {expected} features, received {len(features)}"
            )
        ordered = []
        for idx, value in enumerate(features):
            try:
                ordered.append(float(value))
            except (TypeError, ValueError) as exc:
                name = model.feature_names[idx]
                raise FeatureValidationError(
                    f"Feature '{name}' at position {idx} could not be coerced to float"
                ) from exc

    for idx, dtype in enumerate(model.feature_types):
        if not _is_numeric_dtype(dtype):
            name = model.feature_names[idx]
            raise FeatureValidationError(
                f"Feature '{name}' is declared as non-numeric type '{dtype}'"
            )

    return ordered


def _is_numeric_dtype(dtype: str) -> bool:
    value = dtype.lower()
    numeric_prefixes = (
        "double",
        "float",
        "int",
        "long",
        "short",
        "byte",
    )
    return value.startswith(numeric_prefixes)


def _build_feature_frame(
    features: Sequence[float],
    model: CachedPolicyModel,
) -> Any:
    data = {
        name: [float(value)]
        for name, value in zip(model.feature_names, features)
    }
    if pd is None:
        return [{name: values[0] for name, values in data.items()}]
    return pd.DataFrame(data)


def _normalise_model_output(result: Any) -> Mapping[str, Any]:
    if isinstance(result, Mapping):
        return cast(Mapping[str, Any], result)

    if pd is not None and hasattr(pd, "DataFrame") and isinstance(result, pd.DataFrame):
        if result.empty:
            raise ModelInferenceError("Model returned an empty DataFrame")
        row_mapping = result.iloc[0].to_dict()
        return cast(Mapping[str, Any], row_mapping)

    if hasattr(result, "to_dict") and not isinstance(result, (list, tuple)):
        coerced = result.to_dict()
        if isinstance(coerced, Mapping):
            return cast(Mapping[str, Any], coerced)

    if isinstance(result, Sequence) and not isinstance(result, (str, bytes, bytearray)):
        if not result:
            raise ModelInferenceError("Model returned an empty sequence")
        first = result[0]
        if isinstance(first, Mapping):
            return cast(Mapping[str, Any], first)

    raise ModelInferenceError(f"Unsupported model output type: {type(result).__name__}")


def _intent_from_payload(payload: Mapping[str, Any], metadata: Mapping[str, Any] | None) -> Intent:
    required_fields = (
        "edge_bps",
        "take_profit_bps",
        "stop_loss_bps",
        "selected_action",
        "action_templates",
        "confidence",
        "approved",
    )
    for field in required_fields:
        if field not in payload:
            raise ModelInferenceError(f"Model response is missing '{field}' field")

    confidence_raw = payload["confidence"]
    if isinstance(confidence_raw, ConfidenceMetrics):
        confidence = confidence_raw
    elif isinstance(confidence_raw, Mapping):
        confidence = ConfidenceMetrics(**confidence_raw)
    else:
        raise ModelInferenceError("Model confidence must be a mapping or ConfidenceMetrics")

    templates_raw = payload["action_templates"]
    if not isinstance(templates_raw, Sequence):
        raise ModelInferenceError("Model response action_templates must be a sequence")

    action_templates: List[ActionTemplate] = []
    for template in templates_raw:
        if isinstance(template, ActionTemplate):
            action_templates.append(template)
        elif isinstance(template, Mapping):
            action_templates.append(ActionTemplate(**template))
        else:
            raise ModelInferenceError("Unsupported action template payload returned by model")

    response_metadata: Dict[str, Any] = dict(metadata or {})
    extra_metadata = payload.get("metadata")
    if isinstance(extra_metadata, Mapping):
        response_metadata.update({str(key): extra_metadata[key] for key in extra_metadata})

    reason = payload.get("reason")
    if reason is not None and not isinstance(reason, str):
        reason = str(reason)

    return Intent(
        edge_bps=float(payload["edge_bps"]),
        confidence=confidence,
        take_profit_bps=float(payload["take_profit_bps"]),
        stop_loss_bps=float(payload["stop_loss_bps"]),
        selected_action=str(payload["selected_action"]),
        action_templates=action_templates,
        approved=bool(payload["approved"]),
        reason=reason,
        metadata=response_metadata or None,
    )


def _augment_metadata(
    metadata: Mapping[str, Any] | None,
    **updates: Any,
) -> Dict[str, Any] | None:
    if metadata is None and not updates:
        return None
    payload: Dict[str, Any] = dict(metadata or {})
    for key, value in updates.items():
        payload[str(key)] = value
    return payload


def _require_spot_symbol(symbol: object) -> str:
    try:
        return cast(str, require_spot_symbol(symbol))
    except ValueError as exc:
        logger.warning("Rejected non-spot symbol in policy model server", extra={"symbol": symbol})
        raise ValueError(str(exc)) from exc


def _model_name(account_id: str, symbol: str, variant: str | None = None) -> str:
    canonical_symbol = _require_spot_symbol(symbol)
    suffix = f"::{variant}" if variant else ""
    return f"policy-intent::{account_id}::{canonical_symbol}{suffix}".lower()


def _initialise_registry() -> PolicyModelRegistry:
    client: MlflowClientProtocol | None = None
    factory = MlflowClient
    if factory is not None:
        try:
            client = factory()
        except Exception as exc:  # pragma: no cover - depends on mlflow runtime
            logger.warning("Unable to initialise MlflowClient: %s", exc)
    else:  # pragma: no cover - executed when mlflow is missing
        logger.warning("MlflowClient is not available; model loading disabled")
    return PolicyModelRegistry(client)


_MODEL_REGISTRY = _initialise_registry()


def predict_intent(
    account_id: str,
    symbol: str,
    features: Sequence[float] | Mapping[str, float],
    book_snapshot: BookSnapshot | Dict[str, float],
    model_variant: str | None = None,
    *,
    horizon: int | None = None,
) -> Intent:
    """Run inference against the latest MLflow model and return an intent."""

    metadata: Dict[str, Any] | None = None
    canonical_symbol = _require_spot_symbol(symbol)

    try:
        snapshot = (
            book_snapshot
            if isinstance(book_snapshot, BookSnapshot)
            else BookSnapshot(**book_snapshot)
        )

        model_key = _model_name(account_id, canonical_symbol, model_variant)
        cached_model = _MODEL_REGISTRY.load(model_key)

        metadata = {
            "model_key": model_key,
            "model_name": cached_model.name,
            "model_version": cached_model.version,
            "feature_signature": cached_model.signature_hash,
            "feature_columns": list(cached_model.feature_names),
        }
        if model_variant:
            metadata["model_variant"] = model_variant

        validated_features = _coerce_features(features, cached_model)
        feature_frame = _build_feature_frame(validated_features, cached_model)
        params = {
            "book_snapshot": snapshot.model_dump(),
            "horizon": horizon,
        }

        raw_response = cached_model.model.predict(feature_frame, params=params)
        payload = _normalise_model_output(raw_response)

        return _intent_from_payload(payload, metadata)

    except FeatureValidationError as exc:
        logger.warning(
            "Feature validation failed for account=%s symbol=%s: %s",
            account_id,
            canonical_symbol,
            exc,
        )
        failure_metadata = _augment_metadata(metadata, error=str(exc))
        return Intent.null("feature_validation_error", metadata=failure_metadata)
    except ModelResolutionError as exc:
        logger.error(
            "Unable to load policy model for account=%s symbol=%s: %s",
            account_id,
            canonical_symbol,
            exc,
        )
        failure_metadata = _augment_metadata(metadata, error=str(exc))
        return Intent.null("model_unavailable", metadata=failure_metadata)
    except ModelInferenceError as exc:
        logger.error(
            "Model inference failed for account=%s symbol=%s: %s",
            account_id,
            canonical_symbol,
            exc,
        )
        failure_metadata = _augment_metadata(metadata, error=str(exc))
        return Intent.null("model_inference_error", metadata=failure_metadata)
    except Exception as exc:  # pragma: no cover - defensive safety net
        logger.exception(
            "Unexpected failure during intent prediction for account=%s symbol=%s",
            account_id,
            canonical_symbol,
        )
        failure_metadata = _augment_metadata(metadata, error=str(exc))
        return Intent.null(metadata=failure_metadata)


def get_active_model(account_id: str, symbol: str) -> CachedPolicyModel:
    """Return the cached production model for ``account_id`` and ``symbol``."""

    model_key = _model_name(account_id, symbol)
    return _MODEL_REGISTRY.load(model_key)
