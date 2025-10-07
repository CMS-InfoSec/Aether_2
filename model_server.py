from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from services.common.security import require_admin_account
from shared.spot import require_spot_symbol

try:  # pragma: no cover - mlflow is optional for local tests.
    import mlflow
    from mlflow import pyfunc
    from mlflow.exceptions import MlflowException
    from mlflow.tracking import MlflowClient
except Exception:  # pragma: no cover - graceful degradation when mlflow missing.
    mlflow = None  # type: ignore
    pyfunc = None  # type: ignore
    MlflowClient = None  # type: ignore
    MlflowException = Exception  # type: ignore


LOGGER = logging.getLogger("model_server")
logging.basicConfig(level=os.getenv("MODEL_SERVER_LOG_LEVEL", "INFO"))

MLFLOW_STAGE = os.getenv("MLFLOW_MODEL_STAGE", "prod")
if MLFLOW_STAGE.lower() == "prod":
    # Translate to canonical MLflow stage naming.
    MLFLOW_STAGE = "Production"
MODEL_NAME_TEMPLATE = os.getenv("MLFLOW_MODEL_TEMPLATE", "{symbol}_policy")
MODEL_CACHE_TTL = float(os.getenv("MODEL_CACHE_TTL", "60"))

if mlflow is not None:
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    registry_uri = os.getenv("MLFLOW_REGISTRY_URI")
    if registry_uri:
        mlflow.set_registry_uri(registry_uri)


class PredictRequest(BaseModel):
    account_id: str = Field(..., description="Trading account identifier")
    symbol: str = Field(..., description="Instrument symbol")
    features: Dict[str, float] = Field(default_factory=dict)
    book_snapshot: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def _validate_symbol(cls, value: str) -> str:
        return require_spot_symbol(value)


class IntentResponse(BaseModel):
    action: str
    side: str
    qty: float
    type: str
    limit_px: Optional[float] = None
    tp: Optional[float] = None
    sl: Optional[float] = None
    trailing: Optional[float] = None
    expected_edge_bps: float
    confidence: float


class ModelInfo(BaseModel):
    symbol: str
    model_id: str
    stage: str
    source: str


@dataclass
class CachedModel:
    symbol: str
    model_id: str
    stage: str
    source: str
    model: Any
    loaded_at: float


class ModelCache:
    """Thread-safe in-memory model cache with TTL."""

    def __init__(self, ttl: float = 60.0) -> None:
        self._ttl = ttl
        self._lock = threading.Lock()
        self._models: Dict[str, CachedModel] = {}

    def get(self, symbol: str) -> Optional[CachedModel]:
        now = time.time()
        with self._lock:
            entry = self._models.get(symbol)
            if entry is None:
                return None
            if now - entry.loaded_at > self._ttl:
                LOGGER.debug("Model cache expired for %s", symbol)
                self._models.pop(symbol, None)
                return None
            return entry

    def set(self, symbol: str, entry: CachedModel) -> None:
        with self._lock:
            self._models[symbol] = entry

    def clear(self) -> None:
        with self._lock:
            self._models.clear()


def inference_log(
    symbol: str,
    model_id: str,
    confidence: float,
    ts: float,
    actor_account: str | None = None,
) -> None:
    """Lightweight structured log for inference events."""
    extra = {"symbol": symbol, "model_id": model_id, "confidence": confidence, "ts": ts}
    if actor_account:
        extra["actor_account"] = actor_account
    LOGGER.info("inference_log", extra=extra)


model_cache = ModelCache(ttl=MODEL_CACHE_TTL)


def _resolve_model_name(symbol: str) -> str:
    return MODEL_NAME_TEMPLATE.format(symbol=symbol)


def _load_mlflow_model(symbol: str) -> Optional[CachedModel]:
    if mlflow is None or pyfunc is None or MlflowClient is None:
        LOGGER.warning("MLflow not available; using heuristic for symbol %s", symbol)
        return None

    client = MlflowClient()
    model_name = _resolve_model_name(symbol)
    try:
        latest_versions = client.get_latest_versions(model_name, stages=[MLFLOW_STAGE])
    except MlflowException as exc:  # pragma: no cover - depends on mlflow env
        LOGGER.error("Failed to query MLflow for %s: %s", symbol, exc)
        return None

    if not latest_versions:
        LOGGER.warning("No MLflow versions found for %s in stage %s", model_name, MLFLOW_STAGE)
        return None

    version = latest_versions[0]
    model_uri = f"models:/{model_name}/{version.version}"
    try:
        loaded_model = pyfunc.load_model(model_uri)
    except Exception as exc:  # pragma: no cover - depends on mlflow runtime
        LOGGER.error("Failed to load MLflow model %s: %s", model_uri, exc)
        return None

    LOGGER.info(
        "Loaded MLflow model", extra={"symbol": symbol, "model": model_name, "version": version.version}
    )
    return CachedModel(
        symbol=symbol,
        model_id=f"{model_name}:{version.version}",
        stage=version.current_stage or MLFLOW_STAGE,
        source="mlflow",
        model=loaded_model,
        loaded_at=time.time(),
    )


def get_model(symbol: str) -> CachedModel:
    canonical_symbol = require_spot_symbol(symbol)
    cached = model_cache.get(canonical_symbol)
    if cached is not None:
        return cached

    loaded = _load_mlflow_model(canonical_symbol)
    if loaded is None:
        # Fabricate baseline entry to satisfy downstream callers.
        return CachedModel(
            symbol=canonical_symbol,
            model_id="baseline",
            stage="baseline",
            source="baseline",
            model=baseline_policy,
            loaded_at=time.time(),
        )

    model_cache.set(canonical_symbol, loaded)
    return loaded


def baseline_policy(
    account_id: str,
    symbol: str,
    features: Dict[str, Any],
    book_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    """Simple heuristic policy used when a trained model is unavailable."""
    momentum = float(features.get("momentum", 0.0))
    volatility = float(features.get("volatility", 1.0)) or 1.0
    spread = float(book_snapshot.get("spread", 0.0))

    signal_strength = max(min(momentum / max(volatility, 1e-6), 3.0), -3.0)
    side = "buy" if signal_strength >= 0 else "sell"
    action = "enter" if abs(signal_strength) > 0.5 else "hold"
    qty = max(1.0, float(features.get("base_qty", 100.0)) * min(abs(signal_strength), 1.5))
    expected_edge_bps = float(features.get("expected_edge_bps", 5.0))
    confidence = min(0.6 + 0.1 * abs(signal_strength), 0.95)

    intent: Dict[str, Any] = {
        "action": action,
        "side": side if action != "hold" else "flat",
        "qty": 0.0 if action == "hold" else qty,
        "type": "limit" if spread > 0 else "market",
        "expected_edge_bps": expected_edge_bps,
        "confidence": confidence,
    }

    if intent["type"] == "limit":
        mid_px = float(book_snapshot.get("mid", book_snapshot.get("mid_price", 0.0)))
        limit_adj = spread * (0.25 if side == "buy" else -0.25)
        intent["limit_px"] = mid_px + limit_adj

    if features.get("take_profit"):
        intent["tp"] = float(features["take_profit"])
    if features.get("stop_loss"):
        intent["sl"] = float(features["stop_loss"])

    return intent


def make_intent_response(payload: Any) -> IntentResponse:
    if isinstance(payload, IntentResponse):
        return payload

    if hasattr(payload, "to_dict"):
        payload = payload.to_dict()  # type: ignore[assignment]

    if isinstance(payload, list) and payload:
        payload = payload[0]

    if not isinstance(payload, dict):
        LOGGER.warning("Unexpected model output type %s; falling back to defaults", type(payload))
        payload = {}

    defaults = {
        "action": "hold",
        "side": "flat",
        "qty": 0.0,
        "type": "market",
        "expected_edge_bps": 0.0,
        "confidence": 0.0,
    }
    merged = {**defaults, **payload}
    return IntentResponse(**merged)


async def get_cached_model(symbol: str = Query(..., description="Instrument symbol")) -> CachedModel:
    return get_model(symbol)


app = FastAPI(title="Policy Model Server", version="1.0.0")


@app.post("/models/predict", response_model=IntentResponse)
async def predict(request: PredictRequest, caller: str = Depends(require_admin_account)) -> IntentResponse:
    model_entry = get_model(request.symbol)
    ts = time.time()

    if callable(getattr(model_entry.model, "predict", None)):
        try:
            raw_result = model_entry.model.predict(
                {
                    "account_id": request.account_id,
                    "symbol": request.symbol,
                    "features": request.features,
                    "book_snapshot": request.book_snapshot,
                }
            )
        except Exception as exc:  # pragma: no cover - depends on model implementation
            LOGGER.error("Model predict failed for %s: %s", request.symbol, exc)
            raw_result = baseline_policy(request.account_id, request.symbol, request.features, request.book_snapshot)
    else:
        raw_result = model_entry.model(
            request.account_id, request.symbol, request.features, request.book_snapshot
        )

    intent = make_intent_response(raw_result)
    inference_log(request.symbol, model_entry.model_id, intent.confidence, ts, caller)
    return intent


@app.get("/models/active", response_model=ModelInfo)
async def active_model(
    info: CachedModel = Depends(get_cached_model), caller: str = Depends(require_admin_account)
) -> ModelInfo:
    LOGGER.info(
        "active_model_lookup",
        extra={
            "symbol": info.symbol,
            "model_id": info.model_id,
            "stage": info.stage,
            "source": info.source,
            "actor_account": caller,
        },
    )
    return ModelInfo(symbol=info.symbol, model_id=info.model_id, stage=info.stage, source=info.source)


@app.get("/health")
async def healthcheck() -> JSONResponse:
    return JSONResponse({"status": "ok"})
