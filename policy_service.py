"""FastAPI microservice that produces policy decisions based on model intents."""

from __future__ import annotations


import importlib.util
import logging
import math
import os
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

from decimal import ROUND_HALF_UP, Decimal
from threading import Lock
from types import ModuleType
from typing import TYPE_CHECKING, Dict, List, MutableMapping, Sequence

import httpx
from fastapi import Depends, FastAPI, HTTPException, status

from auth.service import (
    InMemorySessionStore,
    SessionStoreProtocol,
    build_session_store_from_url,
)

from shared.common_bootstrap import ensure_common_helpers

try:  # pragma: no cover - dependency-light environments may stub services.common
    ensure_common_helpers()
except ModuleNotFoundError:
    pass

def _load_local_module(module_name: str, relative_path: str) -> ModuleType:
    """Load *module_name* from the repository even if pytest left a stub behind."""

    spec = importlib.util.spec_from_file_location(
        module_name, Path(__file__).resolve().parent / relative_path
    )
    if spec is None or spec.loader is None:
        raise ModuleNotFoundError(f"Unable to load module {module_name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(module_name, module)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module


try:
    from services.common import security
except (ImportError, AttributeError):  # pragma: no cover - fallback when bootstrap stubs remain
    try:
        import services.common.security as security
    except ModuleNotFoundError:
        security = _load_local_module("services.common.security", "services/common/security.py")


try:
    from services.common.precision import (
        PrecisionMetadataUnavailable,
        precision_provider,
    )
except ModuleNotFoundError:  # pragma: no cover - fallback when pytest leaves behind stubs
    precision_module = _load_local_module(
        "services.common.precision", "services/common/precision.py"
    )
    PrecisionMetadataUnavailable = precision_module.PrecisionMetadataUnavailable
    precision_provider = precision_module.precision_provider
try:
    from services.common.schemas import (
        ActionTemplate,
        BookSnapshot,
        ConfidenceMetrics,
        FeeBreakdown,
        PolicyDecisionRequest,
        PolicyDecisionResponse,
        PolicyState,
    )
except ModuleNotFoundError:  # pragma: no cover - fallback when pytest leaves behind stubs
    schemas_module = _load_local_module("services.common.schemas", "services/common/schemas.py")
    ActionTemplate = schemas_module.ActionTemplate
    BookSnapshot = schemas_module.BookSnapshot
    ConfidenceMetrics = schemas_module.ConfidenceMetrics
    FeeBreakdown = schemas_module.FeeBreakdown
    PolicyDecisionRequest = schemas_module.PolicyDecisionRequest
    PolicyDecisionResponse = schemas_module.PolicyDecisionResponse
    PolicyState = schemas_module.PolicyState
try:
    from services.policy.adaptive_horizon import get_horizon
except ModuleNotFoundError:  # pragma: no cover - fallback when pytest leaves behind stubs
    adaptive_module = _load_local_module(
        "services.policy.adaptive_horizon", "services/policy/adaptive_horizon.py"
    )
    get_horizon = adaptive_module.get_horizon
try:
    from services.risk.stablecoin_monitor import (
        format_depeg_alert,
        get_global_monitor,
    )
except ModuleNotFoundError:  # pragma: no cover - fallback when pytest leaves behind stubs
    monitor_module = _load_local_module(
        "services.risk.stablecoin_monitor", "services/risk/stablecoin_monitor.py"
    )
    format_depeg_alert = monitor_module.format_depeg_alert
    get_global_monitor = monitor_module.get_global_monitor

from ml.policy.fallback_policy import FallbackDecision, FallbackPolicy


from exchange_adapter import get_exchange_adapter, get_exchange_adapters_status
from metrics import (
    metric_context,
    record_abstention_rate,
    record_drift_score,
    setup_metrics,
)
try:
    from services.common.security import require_admin_account
except ModuleNotFoundError:  # pragma: no cover - fallback when pytest leaves behind stubs
    require_admin_account = security.require_admin_account
from shared.health import setup_health_checks
from shared.session_config import load_session_ttl_minutes
from shared.graceful_shutdown import flush_logging_handlers, setup_graceful_shutdown
try:
    from services.common.spot import require_spot_http
except ModuleNotFoundError:  # pragma: no cover - fallback when pytest leaves behind stubs
    spot_module = _load_local_module("services.common.spot", "services/common/spot.py")
    require_spot_http = spot_module.require_spot_http


FEES_SERVICE_URL = os.getenv("FEES_SERVICE_URL", "http://fees-service")
FEES_REQUEST_TIMEOUT = float(os.getenv("FEES_REQUEST_TIMEOUT", "1.0"))
CONFIDENCE_THRESHOLD = float(os.getenv("POLICY_CONFIDENCE_THRESHOLD", "0.55"))
ENABLE_SHADOW_EXECUTION = os.getenv("ENABLE_SHADOW_EXECUTION", "true").lower() in {
    "1",
    "true",
    "yes",
}
SHADOW_CLIENT_SUFFIX = os.getenv("SHADOW_CLIENT_SUFFIX", "-shadow")
DEFAULT_EXCHANGE = os.getenv("PRIMARY_EXCHANGE", "kraken")
MODEL_HEALTH_URL = os.getenv("MODEL_HEALTH_URL", "http://model-service/health/model")
MODEL_HEALTH_TIMEOUT = float(os.getenv("MODEL_HEALTH_TIMEOUT", "0.75"))

MODEL_VARIANTS: List[str] = ["trend_model", "meanrev_model", "vol_breakout"]
DEFAULT_MODEL_SHARPES: Dict[str, float] = {
    "trend_model": 1.0,
    "meanrev_model": 0.9,
    "vol_breakout": 1.1,
}

FOUR_DP = Decimal("0.0001")
EIGHT_DP = Decimal("0.00000001")
ZERO_DECIMAL = Decimal("0")

_ATR_CACHE: Dict[str, float] = {}

logger = logging.getLogger(__name__)


SHUTDOWN_TIMEOUT = float(os.getenv("POLICY_SHUTDOWN_TIMEOUT", os.getenv("SERVICE_SHUTDOWN_TIMEOUT", "60.0")))

app = FastAPI(title="Policy Service", version="2.0.0")
setup_metrics(app, service_name="policy-service")
EXCHANGE_ADAPTER = get_exchange_adapter(DEFAULT_EXCHANGE)


def _configure_session_store(application: FastAPI) -> SessionStoreProtocol:
    """Ensure the FastAPI app exposes the shared authentication session store."""

    existing = getattr(application.state, "session_store", None)
    if isinstance(existing, SessionStoreProtocol):
        store = existing
    else:
        raw_url = os.getenv("SESSION_REDIS_URL")
        allow_ephemeral = os.getenv("AETHER_ALLOW_EPHEMERAL_SESSIONS", "0") in {
            "1",
            "true",
            "yes",
        }
        pytest_active = "pytest" in sys.modules
        if raw_url is None or not raw_url.strip():
            if pytest_active or allow_ephemeral:
                logger.warning(
                    "SESSION_REDIS_URL is not configured; using in-memory session store for tests",
                )
                ttl_minutes = load_session_ttl_minutes()
                store = InMemorySessionStore(ttl_minutes=ttl_minutes)
            else:
                raise RuntimeError(
                    "SESSION_REDIS_URL is not configured. Provide a redis:// DSN for the shared session store."
                )
        else:
            redis_url = raw_url.strip()
            normalized = redis_url.lower()
            if not normalized:
                if pytest_active or allow_ephemeral:
                    logger.warning(
                        "SESSION_REDIS_URL is empty; using in-memory session store for tests",
                    )
                    ttl_minutes = load_session_ttl_minutes()
                    store = InMemorySessionStore(ttl_minutes=ttl_minutes)
                else:
                    raise RuntimeError(
                        "SESSION_REDIS_URL is set but empty; configure a redis:// or rediss:// DSN."
                    )
            else:
                if normalized.startswith("memory://") and not (pytest_active or allow_ephemeral):
                    raise RuntimeError(
                        "SESSION_REDIS_URL must use a redis:// or rediss:// DSN outside pytest so policy sessions persist across restarts."
                    )

                ttl_minutes = load_session_ttl_minutes()
                if normalized.startswith("memory://"):
                    store = InMemorySessionStore(ttl_minutes=ttl_minutes)
                else:
                    store = build_session_store_from_url(redis_url, ttl_minutes=ttl_minutes)

        application.state.session_store = store

    security.set_default_session_store(store)
    return store


SESSION_STORE = _configure_session_store(app)


def _health_check_session_store() -> None:
    store = getattr(app.state, "session_store", None)
    if not isinstance(store, SessionStoreProtocol):
        raise RuntimeError("session store unavailable")


setup_health_checks(app, {"session_store": _health_check_session_store})

shutdown_manager = setup_graceful_shutdown(
    app,
    service_name="policy-service",
    allowed_paths={"/", "/docs", "/openapi.json"},
    shutdown_timeout=SHUTDOWN_TIMEOUT,
    logger_instance=logger,
)


if TYPE_CHECKING:  # pragma: no cover - type checking only
    from services.models.model_server import Intent

try:  # pragma: no cover - optional dependency during testing
    from services.models.model_server import Intent as Intent
except Exception:  # pragma: no cover - fallback for limited environments
    class Intent:  # type: ignore[no-redef]
        def __init__(self, **kwargs) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)


def _predict_intent(**kwargs) -> "Intent":
    from services.models.model_server import predict_intent as _predict

    return _predict(**kwargs)


def predict_intent(**kwargs) -> "Intent":
    """Compatibility wrapper for callers patching predict_intent."""

    return _predict_intent(**kwargs)


def _enforce_stablecoin_guard() -> None:
    monitor = get_global_monitor()
    statuses = monitor.active_depegs()
    if not statuses:
        return

    detail = format_depeg_alert(statuses, monitor.config.depeg_threshold_bps)
    logger.error(
        "Stablecoin depeg guard triggered; refusing policy decision",
        extra={
            "threshold_bps": monitor.config.depeg_threshold_bps,
            "stablecoin_status": [
                {
                    "symbol": status.symbol,
                    "deviation_bps": round(status.deviation_bps, 3),
                    "price": round(status.price, 6),
                    "feed": status.feed,
                }
                for status in statuses
            ],
        },
    )
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=detail,
    )


@app.get("/exchange/adapters", tags=["exchange"])
async def list_exchange_adapters() -> List[Dict[str, object]]:
    """Expose discovery metadata for configured exchange adapters."""

    return await get_exchange_adapters_status()


@dataclass
class RegimeSnapshot:
    """Container holding the latest regime classification for a symbol."""

    symbol: str
    regime: str
    volatility: float
    trend_strength: float
    feature_scale: float
    size_scale: float
    sample_count: int
    updated_at: datetime

    def as_payload(self) -> Dict[str, float | int | str]:
        return {
            "symbol": self.symbol,
            "regime": self.regime,
            "volatility": round(self.volatility, 6),
            "trend_strength": round(self.trend_strength, 6),
            "feature_scale": round(self.feature_scale, 4),
            "size_scale": round(self.size_scale, 4),
            "sample_count": self.sample_count,
            "updated_at": self.updated_at.isoformat(),
        }


class RegimeClassifier:
    """Rolling-volatility regime classifier with lightweight trend detection."""

    def __init__(
        self,
        window: int = 50,
        min_samples: int = 5,
        high_vol_threshold: float = 0.012,
        trend_signal_threshold: float = 1.35,
        feature_scale_map: Dict[str, float] | None = None,
        size_scale_map: Dict[str, float] | None = None,
    ) -> None:
        self.window = max(window, 5)
        self.min_samples = max(min_samples, 2)
        self.high_vol_threshold = max(high_vol_threshold, 0.0)
        self.trend_signal_threshold = max(trend_signal_threshold, 0.0)
        self._prices: MutableMapping[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=self.window)
        )
        self._snapshots: Dict[str, RegimeSnapshot] = {}
        self._lock = Lock()
        self._feature_scale_map = feature_scale_map or {
            "trend": 1.1,
            "range": 1.0,
            "high_vol": 0.85,
        }
        self._size_scale_map = size_scale_map or {
            "trend": 1.15,
            "range": 0.85,
            "high_vol": 0.6,
        }

    def observe(self, symbol: str, price: float) -> RegimeSnapshot:
        norm_symbol = symbol.upper()
        with self._lock:
            price_series = self._prices[norm_symbol]
            if price > 0:
                price_series.append(float(price))
            volatility = self._compute_volatility(price_series)
            trend_strength = self._compute_trend_strength(price_series)
            regime = self._classify(volatility, trend_strength, len(price_series))
            feature_scale = self._feature_scale_map.get(regime, 1.0)
            size_scale = self._size_scale_map.get(regime, 1.0)
            snapshot = RegimeSnapshot(
                symbol=norm_symbol,
                regime=regime,
                volatility=volatility,
                trend_strength=trend_strength,
                feature_scale=feature_scale,
                size_scale=size_scale,
                sample_count=len(price_series),
                updated_at=datetime.now(timezone.utc),
            )
            self._snapshots[norm_symbol] = snapshot
            return snapshot

    def get_snapshot(self, symbol: str) -> RegimeSnapshot | None:
        norm_symbol = symbol.upper()
        with self._lock:
            snapshot = self._snapshots.get(norm_symbol)
            return replace(snapshot) if snapshot is not None else None

    def _compute_volatility(self, prices: Sequence[float]) -> float:
        if len(prices) < 2:
            return 0.0
        series = list(prices)
        log_returns: List[float] = []
        previous = series[0]
        for price in series[1:]:
            if previous <= 0 or price <= 0:
                continue
            log_returns.append(math.log(price / previous))
            previous = price
        if not log_returns:
            return 0.0
        mean_return = sum(log_returns) / len(log_returns)
        variance = sum((ret - mean_return) ** 2 for ret in log_returns) / len(log_returns)
        return math.sqrt(max(variance, 0.0))

    def _compute_trend_strength(self, prices: Sequence[float]) -> float:
        series = list(prices)
        count = len(series)
        if count < 2:
            return 0.0
        x_values = range(count)
        mean_x = (count - 1) / 2.0
        mean_y = sum(series) / float(count)
        numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_values, series))
        denominator = sum((x - mean_x) ** 2 for x in x_values)
        if denominator <= 0:
            return 0.0
        slope = numerator / denominator
        latest_price = series[-1] if series[-1] != 0 else 1.0
        return slope / latest_price

    def _classify(self, volatility: float, trend_strength: float, sample_count: int) -> str:
        if sample_count < self.min_samples:
            return "range"
        if volatility >= self.high_vol_threshold:
            return "high_vol"
        signal = abs(trend_strength) / max(volatility, 1e-6)
        if signal >= self.trend_signal_threshold:
            return "trend"
        return "range"


regime_classifier = RegimeClassifier()
fallback_policy = FallbackPolicy(
    top_symbols=["BTC-USD", "ETH-USD", "SOL-USD", "USDT-USD"],
    size_fraction=float(os.getenv("FALLBACK_SIZE_FRACTION", "0.35")),
    momentum_threshold=float(os.getenv("FALLBACK_MOMENTUM_THRESHOLD", "0.6")),
    max_risk_band_bps=float(os.getenv("FALLBACK_MAX_RISK_BPS", "25")),
)


def _flush_policy_event_buffers() -> None:
    """Flush buffered policy events/loggers prior to shutdown."""

    flush_logging_handlers("", __name__)
    snapshot_count = len(regime_classifier._snapshots)  # noqa: SLF001 - intentional diagnostic access
    if snapshot_count:
        logger.info(
            "Draining cached regime snapshots before shutdown",
            extra={"snapshot_count": snapshot_count},
        )


shutdown_manager.register_flush_callback(_flush_policy_event_buffers)


def _default_state() -> PolicyState:
    return PolicyState(regime="unknown", volatility=0.0, liquidity_score=0.0, conviction=0.0)


def _reset_regime_state() -> None:
    """Reset cached regime state. Intended for test isolation."""

    with regime_classifier._lock:  # type: ignore[attr-defined]
        regime_classifier._prices.clear()  # type: ignore[attr-defined]
        regime_classifier._snapshots.clear()  # type: ignore[attr-defined]




async def _resolve_precision(symbol: str) -> Dict[str, Decimal | str]:
    try:
        metadata = await precision_provider.require(symbol)

    except PrecisionMetadataUnavailable as exc:
        normalized_symbol = (symbol or "").replace("/", "-").strip().upper()
        alias_map = getattr(precision_provider, "_aliases", {})  # type: ignore[attr-defined]
        alias = alias_map.get(normalized_symbol)
        logger.error(
            "Precision metadata unavailable for instrument %s",
            symbol,
            extra={
                "requested_symbol": symbol,
                "normalized_symbol": normalized_symbol,
                "alias": alias,
            },
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Precision metadata unavailable",
        ) from exc

    metadata = dict(metadata)
    native_pair = metadata.get("native_pair")
    if not native_pair and symbol:
        metadata["native_pair"] = symbol.replace("-", "/")
    return metadata


def _snap(value: float | Decimal, step: float | Decimal) -> float:

    if step is None:
        return float(value)
    step_decimal = Decimal(str(step))
    if step_decimal <= 0:
        return float(value)
    operand = Decimal(str(value))
    snapped = (operand / step_decimal).to_integral_value(rounding=ROUND_HALF_UP) * step_decimal
    return float(snapped)




def _to_decimal(value: float | Decimal | None, *, default: Decimal = ZERO_DECIMAL) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (TypeError, ValueError, ArithmeticError):
        return default


def _quantize_decimal(value: Decimal, exponent: Decimal = FOUR_DP) -> Decimal:
    return value.quantize(exponent, rounding=ROUND_HALF_UP)


def _clamp_decimal(
    value: Decimal,
    lower: Decimal = ZERO_DECIMAL,
    upper: Decimal = Decimal("1"),
) -> Decimal:
    if value < lower:
        return lower
    if value > upper:
        return upper
    return value


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


async def _model_health_ok() -> bool:
    if not MODEL_HEALTH_URL:
        return True

    timeout = httpx.Timeout(MODEL_HEALTH_TIMEOUT)
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            response = await client.get(MODEL_HEALTH_URL)
            response.raise_for_status()
        except httpx.HTTPError:
            return False

    try:
        payload = response.json()
    except ValueError:
        return False

    if isinstance(payload, dict):
        status_value = str(payload.get("status") or payload.get("state") or "").lower()
        return status_value == "ok"
    return False


def _model_sharpe_weights() -> Dict[str, float]:
    weights: Dict[str, float] = {}
    for variant in MODEL_VARIANTS:
        env_key = f"POLICY_{variant.upper()}_SHARPE"
        default = DEFAULT_MODEL_SHARPES.get(variant, 1.0)
        raw_value = os.getenv(env_key)
        try:
            weight = float(raw_value) if raw_value is not None else default
        except (TypeError, ValueError):
            weight = default
        weights[variant] = max(weight, 0.0)
    return weights


def _normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
    filtered = {key: max(value, 0.0) for key, value in weights.items()}
    total = sum(filtered.values())
    if total <= 0:
        if not MODEL_VARIANTS:
            return {}
        uniform = 1.0 / float(len(MODEL_VARIANTS))
        return {variant: uniform for variant in MODEL_VARIANTS}
    return {key: (value / total if total > 0 else 0.0) for key, value in filtered.items()}


def _blend_confidence(intents: Dict[str, "Intent"], weights: Dict[str, float]) -> ConfidenceMetrics:
    model_conf = ZERO_DECIMAL
    state_conf = ZERO_DECIMAL
    exec_conf = ZERO_DECIMAL
    overall = ZERO_DECIMAL

    for key, intent in intents.items():
        weight = _to_decimal(weights.get(key, 0.0))
        if weight <= ZERO_DECIMAL:
            continue
        confidence = intent.confidence
        model_conf += _to_decimal(confidence.model_confidence) * weight
        state_conf += _to_decimal(confidence.state_confidence) * weight
        exec_conf += _to_decimal(confidence.execution_confidence) * weight
        overall += _to_decimal(confidence.overall_confidence, default=ZERO_DECIMAL) * weight

    blended = ConfidenceMetrics(
        model_confidence=float(_quantize_decimal(_clamp_decimal(model_conf))),
        state_confidence=float(_quantize_decimal(_clamp_decimal(state_conf))),
        execution_confidence=float(_quantize_decimal(_clamp_decimal(exec_conf))),
        overall_confidence=float(_quantize_decimal(_clamp_decimal(overall))),
    )
    return blended


def _blend_template_confidences(
    intents: Dict[str, "Intent"], weights: Dict[str, float]
) -> Dict[str, Decimal]:
    confidence_totals: Dict[str, Decimal] = defaultdict(lambda: ZERO_DECIMAL)
    weight_totals: Dict[str, Decimal] = defaultdict(lambda: ZERO_DECIMAL)

    for key, intent in intents.items():
        weight = _to_decimal(weights.get(key, 0.0))
        if weight <= ZERO_DECIMAL:
            continue
        for template in intent.action_templates or []:
            template_key = template.name.lower()
            confidence_totals[template_key] += _to_decimal(template.confidence) * weight
            weight_totals[template_key] += weight

    blended: Dict[str, Decimal] = {}
    for template_key, total_conf in confidence_totals.items():
        weight = weight_totals.get(template_key, ZERO_DECIMAL)
        if weight > ZERO_DECIMAL:
            average = total_conf / weight
        else:
            average = total_conf
        blended[template_key] = _quantize_decimal(_clamp_decimal(average))
    return blended


def _vote_entropy(votes: Dict[str, float]) -> float:
    total = sum(value for value in votes.values() if value > 0)
    if total <= 0:
        return 0.0
    entropy = 0.0
    for value in votes.values():
        if value <= 0:
            continue
        probability = value / total
        entropy -= probability * math.log(probability)
    return entropy


def _resolve_atr(symbol: str, snapshot: BookSnapshot, state: PolicyState | None) -> float:
    symbol_key = symbol.upper()

    atr_candidates: List[float] = []
    if state is not None:
        try:
            atr_candidates.append(float(getattr(state, "volatility", 0.0)) * 100.0)
        except (TypeError, ValueError):
            pass

    try:
        atr_candidates.append(float(snapshot.spread_bps) * 1.5)
    except (TypeError, ValueError):
        pass

    atr = next((value for value in atr_candidates if value and math.isfinite(value) and value > 0), None)
    if atr is None:
        atr = _ATR_CACHE.get(symbol_key, 10.0)
    else:
        previous = _ATR_CACHE.get(symbol_key)
        if previous is not None:
            atr = 0.5 * previous + 0.5 * atr
        _ATR_CACHE[symbol_key] = atr

    atr = max(_ATR_CACHE.get(symbol_key, atr), 1.0)
    _ATR_CACHE[symbol_key] = atr
    return atr


def _resolve_risk_band(
    request_value: float | None,
    intents: Dict[str, "Intent"],
    weights: Dict[str, float],
    attribute: str,
) -> float | None:
    """Resolve a blended risk band while honouring overrides."""

    def _coerce(value: object | None) -> float | None:
        try:
            numeric = float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None
        if not math.isfinite(numeric) or numeric <= 0:
            return None
        return numeric

    candidate = _coerce(request_value)
    if candidate is not None:
        return candidate

    weighted_total = 0.0
    total_weight = 0.0
    fallback: float | None = None

    for variant, intent in intents.items():
        value = _coerce(getattr(intent, attribute, None))
        if value is None:
            continue
        if fallback is None:
            fallback = value
        weight = weights.get(variant, 0.0)
        if weight <= 0:
            continue
        weighted_total += value * weight
        total_weight += weight

    if total_weight > 0:
        return weighted_total / total_weight

    return fallback



async def _fetch_effective_fee(
    account_id: str, symbol: str, liquidity: str, notional: Decimal | float
) -> Decimal:
    liquidity_normalized = liquidity.lower() if liquidity else "maker"
    if liquidity_normalized not in {"maker", "taker"}:
        liquidity_normalized = "maker"

    notional_decimal = _to_decimal(notional)
    if notional_decimal < ZERO_DECIMAL:
        notional_decimal = ZERO_DECIMAL

    params = {
        "pair": symbol,
        "liquidity": liquidity_normalized,
        "notional": f"{_quantize_decimal(notional_decimal, EIGHT_DP)}",
    }
    headers = {"X-Account-ID": account_id}
    timeout = httpx.Timeout(FEES_REQUEST_TIMEOUT)

    async with httpx.AsyncClient(base_url=FEES_SERVICE_URL, timeout=timeout) as client:
        try:
            response = await client.get("/fees/effective", params=params, headers=headers)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:  # pragma: no cover - surface upstream errors
            raise HTTPException(
                status_code=exc.response.status_code,
                detail="Fee service returned an error",
            ) from exc
        except httpx.HTTPError as exc:  # pragma: no cover - network failures
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to contact fee service",
            ) from exc

    payload = response.json()
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Fee service response malformed",
        )

    try:
        return _to_decimal(payload["bps"])
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Fee service response missing expected fields",
        ) from exc


@app.get("/health", tags=["health"])
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/ready", tags=["health"])
async def ready() -> Dict[str, str]:
    try:
        from services.models.model_server import predict_intent as _predict  # noqa: F401
    except ImportError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model server unavailable",
        ) from exc
    return {"status": "ready"}


@app.get("/policy/regime", tags=["policy"])
async def get_regime(
    symbol: str,
    account_id: str | None = None,
    caller: str = Depends(require_admin_account),
) -> Dict[str, float | int | str]:
    if account_id and account_id.strip().lower() != caller.strip().lower():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authenticated account is not authorized for the requested account.",
        )

    normalized_symbol = require_spot_http(symbol, param="symbol", logger=logger)

    snapshot = regime_classifier.get_snapshot(normalized_symbol)
    if snapshot is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No regime information available for symbol",
        )
    return snapshot.as_payload()


@app.post(
    "/policy/decide",
    response_model=PolicyDecisionResponse,
    status_code=status.HTTP_200_OK,
)

async def decide_policy(
    request: PolicyDecisionRequest,
    caller_account: str = Depends(require_admin_account),
) -> PolicyDecisionResponse:
    request_account = request.account_id.strip()
    account_id = caller_account.strip()
    caller_normalized = account_id.lower()
    if request_account.strip().lower() != caller_normalized:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between header and payload.",
        )

    if hasattr(request, "model_copy"):
        request = request.model_copy(update={"account_id": account_id})
    else:
        request.account_id = account_id  # type: ignore[attr-defined]

    logger.info(
        "Policy decision requested by %s for order %s on account %s",
        account_id,
        request.order_id,
        request_account,
    )
    precision = await _resolve_precision(request.instrument)
    snapped_price = _snap(request.price, precision["tick"])
    snapped_qty = _snap(request.quantity, precision["lot"])

    if snapped_price <= 0 or snapped_qty <= 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Snapped price or quantity is non-positive",
        )

    if request.book_snapshot is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Book snapshot is required for policy evaluation",
        )

    book_snapshot = request.book_snapshot

    _enforce_stablecoin_guard()

    state_model = request.state or _default_state()
    state_payload = (
        state_model.model_dump()
        if hasattr(state_model, "model_dump")
        else {
            "regime": getattr(state_model, "regime", "unknown"),
            "volatility": getattr(state_model, "volatility", 0.0),
            "liquidity_score": getattr(state_model, "liquidity_score", 0.0),
            "conviction": getattr(state_model, "conviction", 0.0),
        }
    )
    horizon_features = {
        "symbol": request.instrument,
        "regime": state_payload.get("regime"),
        "state": state_payload,
        "features": list(request.features or []),
    }
    horizon_seconds = get_horizon(horizon_features)
    features: List[float] = [float(value) for value in request.features]


    raw_weights = _model_sharpe_weights()
    weights = _normalize_weights(raw_weights)

    fallback_reason: str | None = None
    if not await _model_health_ok():
        fallback_reason = "model_health_check_failed"

    intents: Dict[str, "Intent"] = {}
    if fallback_reason is None:
        for variant in MODEL_VARIANTS:
            try:
                intents[variant] = predict_intent(
                    account_id=account_id,
                    symbol=request.instrument,
                    features=features,
                    book_snapshot=book_snapshot,
                    model_variant=variant,
                    horizon=horizon_seconds,
                )
            except Exception as exc:  # pragma: no cover - protective fallback path
                logger.exception(
                    "Model inference failed for order %s on variant %s: %s",
                    request.order_id,
                    variant,
                    exc,
                )
                fallback_reason = f"model_inference_error:{variant}"
                break

    if fallback_reason is not None:
        activation_start = time.monotonic()
        fallback_decision: FallbackDecision = fallback_policy.evaluate(
            request=request,
            book_snapshot=book_snapshot,
            reason=fallback_reason,
        )
        duration = time.monotonic() - activation_start
        fallback_policy.log_activation(reason=fallback_reason, duration=duration)

        await _dispatch_shadow_orders(
            fallback_decision.request,
            fallback_decision.response,
            actor=account_id,
        )
        return fallback_decision.response

    notional = _to_decimal(snapped_price) * _to_decimal(snapped_qty)
    maker_fee_bps = _to_decimal(
        await _fetch_effective_fee(account_id, request.instrument, "maker", notional)
    )
    taker_fee_bps = _to_decimal(
        await _fetch_effective_fee(account_id, request.instrument, "taker", notional)
    )

    effective_fee = FeeBreakdown(
        currency=request.fee.currency,
        maker=float(_quantize_decimal(maker_fee_bps)),
        taker=float(_quantize_decimal(taker_fee_bps)),
        maker_detail=request.fee.maker_detail,
        taker_detail=request.fee.taker_detail,
    )

    confidence = _blend_confidence(intents, weights)
    caller_confidence = request.confidence
    if caller_confidence is not None:
        confidence = ConfidenceMetrics(
            model_confidence=max(confidence.model_confidence, caller_confidence.model_confidence),
            state_confidence=max(confidence.state_confidence, caller_confidence.state_confidence),
            execution_confidence=max(
                confidence.execution_confidence, caller_confidence.execution_confidence
            ),
            overall_confidence=max(
                confidence.overall_confidence or 0.0,
                caller_confidence.overall_confidence or 0.0,
            ),
        )

    expected_edge = ZERO_DECIMAL
    for variant, intent in intents.items():
        weight = _to_decimal(weights.get(variant, 0.0))
        if weight <= ZERO_DECIMAL:
            continue
        expected_edge += weight * _to_decimal(intent.edge_bps, default=ZERO_DECIMAL)

    slippage_bps = _to_decimal(request.slippage_bps, default=ZERO_DECIMAL)
    maker_edge = _quantize_decimal(expected_edge - (maker_fee_bps + slippage_bps))
    taker_edge = _quantize_decimal(expected_edge - (taker_fee_bps + slippage_bps))

    template_confidences = _blend_template_confidences(intents, weights)
    maker_confidence = template_confidences.get(
        "maker", _to_decimal(confidence.execution_confidence)
    )
    taker_default = _to_decimal(confidence.execution_confidence) * Decimal("0.95")
    taker_confidence = template_confidences.get("taker", taker_default)
    maker_confidence = _quantize_decimal(_clamp_decimal(maker_confidence))
    taker_confidence = _quantize_decimal(_clamp_decimal(taker_confidence))

    action_templates = [
        ActionTemplate(
            name="maker",
            venue_type="maker",
            edge_bps=float(maker_edge),
            fee_bps=float(_quantize_decimal(maker_fee_bps)),
            confidence=float(maker_confidence),
        ),
        ActionTemplate(
            name="taker",
            venue_type="taker",
            edge_bps=float(taker_edge),
            fee_bps=float(_quantize_decimal(taker_fee_bps)),
            confidence=float(taker_confidence),
        ),
    ]

    edge_by_action = {"maker": maker_edge, "taker": taker_edge}

    vote_totals: Dict[str, float] = defaultdict(float)
    for variant, intent in intents.items():
        action = (intent.selected_action or "abstain").lower()
        if action not in {"maker", "taker"}:
            action = "abstain"
        vote_totals[action] += weights.get(variant, 0.0)
    for label in ("maker", "taker", "abstain"):
        vote_totals.setdefault(label, 0.0)

    action_priority = {"maker": 2, "taker": 1, "abstain": 0}
    winning_action = max(
        vote_totals.items(),
        key=lambda item: (item[1], action_priority.get(item[0], -1)),
    )[0]

    entropy = _vote_entropy(vote_totals)

    selected_template = next(
        (
            template
            for template in action_templates
            if template.name.lower() == winning_action
        ),
        None,
    )
    if selected_template is None and action_templates:
        selected_template = max(action_templates, key=lambda template: template.edge_bps)

    if winning_action in edge_by_action:
        fee_adjusted_edge = edge_by_action[winning_action]
    elif selected_template is not None:
        fee_adjusted_edge = _quantize_decimal(
            _to_decimal(selected_template.edge_bps),
        )
    else:
        fee_adjusted_edge = ZERO_DECIMAL
    approval_score = sum(
        weights.get(variant, 0.0) for variant, intent in intents.items() if intent.approved
    )

    atr = _resolve_atr(request.instrument, book_snapshot, state_model)
    take_profit_override = _resolve_risk_band(
        request.take_profit_bps, intents, weights, "take_profit_bps"
    )
    stop_loss_override = _resolve_risk_band(
        request.stop_loss_bps, intents, weights, "stop_loss_bps"
    )
    take_profit = take_profit_override if take_profit_override is not None else 3.0 * atr
    stop_loss = stop_loss_override if stop_loss_override is not None else 2.0 * atr

    approved = (
        winning_action in {"maker", "taker"}
        and approval_score >= 0.5
        and fee_adjusted_edge > 0
        and (confidence.overall_confidence or 0.0) >= CONFIDENCE_THRESHOLD
        and entropy <= 0.3
    )

    reason: str | None = None
    selected_action = winning_action if winning_action in {"maker", "taker"} else "abstain"
    if not approved:

        if fee_adjusted_edge <= ZERO_DECIMAL:
            reason = "Fee-adjusted edge non-positive"
        elif entropy > 0.3:
            reason = "High ensemble entropy"
        elif winning_action not in {"maker", "taker"}:
            reason = "Ensemble voted to abstain"
        elif approval_score < 0.5:
            reason = "Insufficient ensemble approval"
        elif (confidence.overall_confidence or 0.0) < CONFIDENCE_THRESHOLD:
            reason = "Confidence below threshold"
        else:
            reason = "Fee-adjusted edge non-positive"

        selected_action = "abstain"
        fee_adjusted_edge = min(fee_adjusted_edge, ZERO_DECIMAL)
    else:
        selected_action = selected_template.name if selected_template else selected_action


    drift_value = getattr(state_model, "conviction", 0.0)
    try:
        drift_value = float(drift_value)
    except (TypeError, ValueError):
        drift_value = 0.0

    metrics_ctx = metric_context(account_id=account_id, symbol=request.instrument)
    record_drift_score(
        account_id,
        request.instrument,
        drift_value,
        context=metrics_ctx,
    )
    abstain_metric = 0.0 if approved and selected_action != "abstain" else 1.0
    record_abstention_rate(
        account_id,
        request.instrument,
        abstain_metric,
        context=metrics_ctx,
    )

    response = PolicyDecisionResponse(
        approved=approved,
        reason=reason,
        effective_fee=effective_fee,
        expected_edge_bps=float(_quantize_decimal(expected_edge)),
        fee_adjusted_edge_bps=float(_quantize_decimal(fee_adjusted_edge)),
        selected_action=selected_action,
        action_templates=action_templates,
        confidence=confidence,
        features=features,
        book_snapshot=book_snapshot,
        state=state_model,
        take_profit_bps=round(float(take_profit), 4),
        stop_loss_bps=round(float(stop_loss), 4),
    )

    await _dispatch_shadow_orders(request, response, actor=account_id)

    return response


async def _dispatch_shadow_orders(
    request: PolicyDecisionRequest,
    response: PolicyDecisionResponse,
    *,
    actor: str | None = None,
) -> None:
    """Submit the primary execution as well as the paper shadow copy."""

    if not response.approved or response.selected_action.lower() == "abstain":
        return

    await _submit_execution(request, response, shadow=False, actor=actor)

    if not ENABLE_SHADOW_EXECUTION:
        return

    try:
        await _submit_execution(request, response, shadow=True, actor=actor)
    except Exception as exc:  # pragma: no cover - best-effort shadow dispatch
        logger.warning(
            "Shadow execution submission failed for order %s requested by %s: %s",
            request.order_id,
            actor or "unknown",
            exc,
        )


async def _submit_execution(
    request: PolicyDecisionRequest,
    response: PolicyDecisionResponse,
    *,
    shadow: bool,
    actor: str | None = None,
) -> None:
    """Submit the execution payload to the configured OMS endpoint."""

    precision = await _resolve_precision(request.instrument)
    snapped_price = _snap(request.price, precision["tick"])
    snapped_qty = _snap(request.quantity, precision["lot"])

    order_type = "limit" if response.selected_action.lower() == "maker" else "market"
    client_id = request.order_id
    if shadow and SHADOW_CLIENT_SUFFIX:
        client_id = f"{client_id}{SHADOW_CLIENT_SUFFIX}"

    payload: Dict[str, object] = {
        "account_id": request.account_id,
        "client_id": client_id,
        "symbol": request.instrument,
        "side": request.side.lower(),
        "order_type": order_type,
        "qty": snapped_qty,
        "post_only": response.selected_action.lower() == "maker",
        "reduce_only": False,
        "flags": [],
        "shadow": shadow,
    }
    if order_type == "limit":
        payload["limit_px"] = snapped_price
    if actor:
        payload["requested_by"] = actor

    if not EXCHANGE_ADAPTER.supports("place_order"):
        raise RuntimeError(
            f"Exchange adapter '{EXCHANGE_ADAPTER.name}' does not support order placement"
        )

    try:
        await EXCHANGE_ADAPTER.place_order(request.account_id, payload, shadow=shadow)
    except httpx.HTTPError as exc:
        if shadow:
            raise
        logger.error(
            "Primary OMS submission failed for order %s requested by %s: %s",
            request.order_id,
            actor or "unknown",
            exc,
        )
        raise
