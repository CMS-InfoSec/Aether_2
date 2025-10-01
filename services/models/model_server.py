"""Model server abstraction for retrieving policy intent models from MLflow.

This module provides a small caching layer around MLflow's model registry to
fetch the latest model artifact for a given name and stage.  It exposes a
``predict_intent`` entry point that merges model output with a deterministic
fallback heuristic so the policy service can continue operating even when the
registry or model artifact is unavailable.

The intent payload returned by :func:`predict_intent` follows the common Policy
Intent structure used throughout the code base::

    {
        "action": "trade" | "no_trade" | "scale_trade",
        "side": "buy" | "sell",
        "qty": float,
        "type": "market" | "limit",
        "limit_px": Optional[float],
        "tif": Optional[str],
        "tp": Optional[float],
        "sl": Optional[float],
        "trailing": Optional[bool],
        "expected_edge_bps": float,
        "expected_cost_bps": float,
        "confidence": float,
    }

The implementation intentionally keeps the MLflow dependency optional; if the
library cannot be imported the code gracefully falls back to the heuristic.
"""

from __future__ import annotations

import logging
import math
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Mapping, MutableMapping, Optional, Tuple

try:  # pragma: no cover - MLflow is optional in the execution environment.
    import mlflow
    from mlflow.pyfunc import PyFuncModel
except Exception:  # pragma: no cover - gracefully degrade when MLflow missing.
    mlflow = None  # type: ignore
    PyFuncModel = Any  # type: ignore[assignment]

try:  # pragma: no cover - pandas is an optional convenience dependency.
    import pandas as pd
except Exception:  # pragma: no cover - keep runtime lean when pandas missing.
    pd = None  # type: ignore

LOGGER = logging.getLogger(__name__)

# Provide deterministic stub configuration so local environments can execute
# without a fully fledged MLflow installation.
os.environ.setdefault("MLFLOW_TRACKING_URI", "http://localhost:5000")
os.environ.setdefault("MLFLOW_REGISTRY_URI", os.environ["MLFLOW_TRACKING_URI"])

_STAGE_ALIAS = {
    "prod": "Production",
    "production": "Production",
    "staging": "Staging",
    "canary": "Canary",
}

_DEFAULT_REFRESH_SECONDS = 5 * 60  # refresh cached models every five minutes.
# Confidence thresholds used to gate trading decisions.  Values can be tuned via
# environment variables to adjust behaviour without code changes.
_CONF_THRESHOLD = float(os.getenv("POLICY_MODEL_CONF_THRESHOLD", "0.6"))
_MIN_CONF_TO_TRADE = float(os.getenv("POLICY_MODEL_MIN_CONF", "0.35"))


@dataclass
class _CacheEntry:
    """Structure holding metadata about a cached model artifact."""

    model: PyFuncModel | Any
    loaded_at: float
    version: Optional[str] = None


class _ModelServer:
    """Encapsulates MLflow retrieval and inference with caching support."""

    def __init__(
        self,
        *,
        stage: str = "prod",
        refresh_seconds: int = _DEFAULT_REFRESH_SECONDS,
    ) -> None:
        if stage.lower() not in _STAGE_ALIAS:
            allowed = ", ".join(sorted(_STAGE_ALIAS))
            raise ValueError(f"Unsupported model stage '{stage}'. Expected one of: {allowed}.")
        self.stage = stage.lower()
        self.refresh_seconds = max(60, int(refresh_seconds))
        self._cache: MutableMapping[Tuple[str, str], _CacheEntry] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Model loading utilities
    # ------------------------------------------------------------------
    def _load_model_from_mlflow(self, name: str) -> PyFuncModel:
        """Retrieve the latest model from MLflow for ``name`` and ``stage``.

        Raises ``RuntimeError`` when the lookup fails so callers can decide
        whether to fall back to the heuristic or propagate the error.
        """

        if mlflow is None:  # pragma: no cover - depends on optional dependency.
            raise RuntimeError("MLflow is not available in this environment")

        target_stage = _STAGE_ALIAS[self.stage]
        model_uri = f"models:/{name}/{target_stage}"
        LOGGER.debug("Loading model '%s' for stage '%s'", name, target_stage)

        try:
            model = mlflow.pyfunc.load_model(model_uri)
        except Exception as exc:  # pragma: no cover - network/registry failure.
            raise RuntimeError(f"Failed to load model '{name}' at stage '{target_stage}': {exc}") from exc

        return model

    def _get_cached_model(self, name: str) -> Optional[PyFuncModel | Any]:
        cache_key = (name, self.stage)
        now = time.monotonic()
        entry = self._cache.get(cache_key)
        if entry and now - entry.loaded_at < self.refresh_seconds:
            return entry.model
        return None

    def _store_model(self, name: str, model: PyFuncModel | Any, version: Optional[str] = None) -> None:
        cache_key = (name, self.stage)
        self._cache[cache_key] = _CacheEntry(model=model, loaded_at=time.monotonic(), version=version)

    def _get_or_load_model(self, name: str) -> Optional[PyFuncModel | Any]:
        cache_key = (name, self.stage)
        now = time.monotonic()

        with self._lock:
            entry = self._cache.get(cache_key)
            if entry and now - entry.loaded_at < self.refresh_seconds:
                return entry.model

        # Attempt refresh outside the lock to avoid blocking other threads.
        model: Optional[PyFuncModel | Any]
        try:
            model = self._load_model_from_mlflow(name)
        except Exception as exc:  # pragma: no cover - failure handled gracefully.
            LOGGER.warning("Unable to refresh model '%s' for stage '%s': %s", name, self.stage, exc)
            with self._lock:
                entry = self._cache.get(cache_key)
                if entry is not None:
                    LOGGER.info("Using stale cached model for '%s'", name)
                    entry.loaded_at = now  # prevent immediate re-refresh attempts.
                    return entry.model
            return None

        version: Optional[str] = None
        metadata = getattr(model, "metadata", None)
        if isinstance(metadata, Mapping):
            try:
                version = metadata.get("version")  # type: ignore[assignment]
            except AttributeError:
                version = None

        with self._lock:
            self._store_model(name, model, version)
        return model

    # ------------------------------------------------------------------
    # Inference helpers
    # ------------------------------------------------------------------
    def predict_intent(
        self,
        account_id: str,
        symbol: str,
        features: Mapping[str, Any],
        book_snapshot: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """Generate a trading intent for the requested account/symbol pair."""

        model_name = self._model_name(account_id, symbol)
        baseline = self._baseline_intent(account_id, symbol, features, book_snapshot)

        model = self._get_or_load_model(model_name)
        if model is None:
            LOGGER.debug("Falling back to heuristic intent for %s/%s", account_id, symbol)
            return self._finalise_intent(baseline)

        try:
            raw_output = self._run_model(model, features, book_snapshot)
            merged = self._merge_with_baseline(baseline, raw_output)
            return self._finalise_intent(merged)
        except Exception:  # pragma: no cover - defensive guard around model exec.
            LOGGER.exception(
                "Model inference failed for account=%s symbol=%s. Reverting to baseline.",
                account_id,
                symbol,
            )
            return self._finalise_intent(baseline)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _model_name(account_id: str, symbol: str) -> str:
        return f"policy-intent::{account_id}::{symbol}".lower()

    def _run_model(
        self,
        model: PyFuncModel | Any,
        features: Mapping[str, Any],
        book_snapshot: Mapping[str, Any],
    ) -> Any:
        """Execute the MLflow model while handling different model signatures."""

        payload: Dict[str, Any] = {**features}
        for key, value in book_snapshot.items():
            payload[f"book_{key}"] = value

        if hasattr(model, "predict"):
            if pd is not None:
                try:
                    dataframe = pd.DataFrame([payload])
                    return model.predict(dataframe)
                except Exception as exc:
                    LOGGER.debug("DataFrame inference failed, retrying with dict payload: %s", exc)
            return model.predict(payload)

        if callable(model):
            return model(payload)

        raise RuntimeError("Loaded MLflow artifact does not expose a callable predict method")

    def _merge_with_baseline(self, baseline: Dict[str, Any], raw_output: Any) -> Dict[str, Any]:
        """Overlay model output on top of the heuristic baseline."""

        merged = dict(baseline)
        updates: Dict[str, Any]

        if raw_output is None:
            return merged
        if isinstance(raw_output, Mapping):
            updates = dict(raw_output)
        elif pd is not None and "DataFrame" in raw_output.__class__.__name__:
            try:
                updates = dict(raw_output.iloc[0])  # type: ignore[attr-defined]
            except Exception:  # pragma: no cover - fallback for unexpected shapes.
                updates = {}
        elif isinstance(raw_output, (list, tuple)) and raw_output:
            first = raw_output[0]
            updates = first if isinstance(first, Mapping) else {"expected_edge_bps": first}
        else:
            updates = {"expected_edge_bps": raw_output}

        for key, value in updates.items():
            if value is not None:
                merged[key] = value

        return merged

    def _baseline_intent(
        self,
        account_id: str,
        symbol: str,
        features: Mapping[str, Any],
        book_snapshot: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """Construct a deterministic heuristic when the ML model is unavailable."""

        spread_bps = float(self._safe_get(features, book_snapshot, "spread_bps", default=12.0))
        imbalance = float(self._safe_get(features, book_snapshot, "imbalance", default=0.0))
        liquidity = float(self._safe_get(features, book_snapshot, "liquidity_score", default=0.5))
        signal = float(self._safe_get(features, book_snapshot, "signal", default=imbalance))
        volatility = float(self._safe_get(features, book_snapshot, "volatility", default=35.0))
        base_edge = float(features.get("expected_edge_bps", signal * 25.0))

        expected_edge = base_edge
        expected_edge += (liquidity - 0.5) * 18.0
        expected_edge += imbalance * 12.0
        expected_edge -= spread_bps * 0.25
        expected_edge -= max(volatility - 40.0, 0.0) * 0.12

        expected_cost = spread_bps + float(features.get("fee_bps", book_snapshot.get("fee_bps", 0.4)))
        expected_cost = max(expected_cost, 0.0)

        confidence = 0.5 + expected_edge / 200.0
        confidence = max(0.0, min(1.0, confidence))

        base_qty = float(features.get("target_qty", features.get("base_qty", 1.0)))
        base_qty = max(base_qty, 0.0)

        side = "buy" if signal >= 0 else "sell"
        order_type = "limit" if spread_bps <= 15.0 else "market"

        mid_price = self._derive_mid_price(book_snapshot)
        direction = 1.0 if side == "buy" else -1.0

        limit_px = self._price_with_edge(mid_price, direction, expected_edge * 0.25)
        take_profit_px = self._price_with_edge(mid_price, direction, abs(expected_edge) * 0.8)
        stop_loss_px = self._price_with_edge(mid_price, -direction, abs(expected_edge) * 0.4)

        trailing = confidence >= 0.75 and order_type == "limit"

        return {
            "action": "trade",
            "side": side,
            "qty": base_qty,
            "type": order_type,
            "limit_px": limit_px,
            "tif": "GTC" if order_type == "limit" else "IOC",
            "tp": take_profit_px,
            "sl": stop_loss_px,
            "trailing": trailing,
            "expected_edge_bps": expected_edge,
            "expected_cost_bps": expected_cost,
            "confidence": confidence,
            "_base_qty": base_qty,
        }

    @staticmethod
    def _safe_get(
        features: Mapping[str, Any],
        book_snapshot: Mapping[str, Any],
        key: str,
        *,
        default: float,
    ) -> float:
        for source in (features, book_snapshot):
            value = source.get(key)
            if value is not None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    continue
        return float(default)

    @staticmethod
    def _derive_mid_price(book_snapshot: Mapping[str, Any]) -> Optional[float]:
        candidates = [
            book_snapshot.get("mid_price"),
            book_snapshot.get("mid"),
            book_snapshot.get("mark_price"),
        ]
        bid = book_snapshot.get("best_bid")
        ask = book_snapshot.get("best_ask")
        if bid is not None and ask is not None:
            try:
                candidates.append((float(bid) + float(ask)) / 2.0)
            except (TypeError, ValueError):  # pragma: no cover - defensive guard.
                pass
        for value in candidates:
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):  # pragma: no cover - unexpected payloads.
                continue
        return None

    @staticmethod
    def _price_with_edge(mid_price: Optional[float], direction: float, edge_bps: float) -> Optional[float]:
        if mid_price is None or not math.isfinite(mid_price):
            return None
        price = mid_price * (1.0 + direction * edge_bps / 10_000.0)
        return round(price, 4)

    def _finalise_intent(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        intent = dict(intent)
        base_qty = float(intent.pop("_base_qty", intent.get("qty", 0.0)))

        expected_edge = float(intent.get("expected_edge_bps", 0.0))
        expected_cost = float(intent.get("expected_cost_bps", 0.0))
        confidence = float(intent.get("confidence", 0.0))

        expected_edge = round(expected_edge, 4)
        expected_cost = round(max(expected_cost, 0.0), 4)
        confidence = max(0.0, min(1.0, round(confidence, 4)))

        intent["expected_edge_bps"] = expected_edge
        intent["expected_cost_bps"] = expected_cost
        intent["confidence"] = confidence

        if expected_edge <= expected_cost or confidence < _MIN_CONF_TO_TRADE:
            intent["action"] = "no_trade"
            intent["qty"] = 0.0
        elif confidence < _CONF_THRESHOLD:
            scaled_qty = base_qty * (confidence / _CONF_THRESHOLD)
            intent["action"] = "scale_trade"
            intent["qty"] = round(max(scaled_qty, 0.0), 6)
        else:
            intent["action"] = intent.get("action", "trade")
            intent["qty"] = round(max(base_qty, 0.0), 6)

        if intent["qty"] == 0.0:
            intent.setdefault("type", "limit")
            intent.setdefault("side", "buy" if expected_edge >= 0 else "sell")
        else:
            intent["side"] = intent.get("side", "buy" if expected_edge >= 0 else "sell")
            intent["type"] = intent.get("type", "limit")

        # Ensure optional numeric fields are rounded for readability.
        for field in ("limit_px", "tp", "sl"):
            if intent.get(field) is None:
                continue
            try:
                intent[field] = round(float(intent[field]), 4)
            except (TypeError, ValueError):
                intent[field] = None
        intent["trailing"] = bool(intent.get("trailing", False))
        intent.setdefault("tif", "GTC" if intent.get("type") == "limit" else "IOC")

        return intent


# Singleton instance used by the module level ``predict_intent`` facade.
_SERVER = _ModelServer(stage=os.getenv("POLICY_MODEL_STAGE", "prod"))


def predict_intent(
    account_id: str,
    symbol: str,
    features: Mapping[str, Any],
    book_snapshot: Mapping[str, Any],
) -> Dict[str, Any]:
    """Public entry point delegating to the shared model server instance."""

    return _SERVER.predict_intent(account_id, symbol, features, book_snapshot)


__all__ = ["predict_intent"]
