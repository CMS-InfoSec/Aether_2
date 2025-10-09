"""FastAPI service exposing trade intent explainability for the UI dashboard."""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Callable, Iterable, Mapping, Sequence, TypeVar, cast

from fastapi import Depends, FastAPI, HTTPException, Query

from metrics import setup_metrics
from fastapi.responses import JSONResponse

from services.common.security import require_admin_account
from services.models.model_server import get_active_model
from shared.postgres import normalize_postgres_dsn
from shared.psycopg_compat import (
    PsycopgModule,
    RowFactory,
    load_dict_row,
    load_psycopg,
)

TCallable = TypeVar("TCallable", bound=Callable[..., Any])

psycopg: PsycopgModule | None
dict_row: RowFactory | None
psycopg, _ = load_psycopg()
dict_row = load_dict_row()


def typed_app_get(
    application: FastAPI, path: str, **kwargs: Any
) -> Callable[[TCallable], TCallable]:
    """Wrap ``FastAPI.get`` to preserve typing for decorated callables."""

    def decorator(func: TCallable) -> TCallable:
        wrapped = application.get(path, **kwargs)(func)
        return cast(TCallable, wrapped)

    return decorator


LOGGER = logging.getLogger(__name__)

_DATABASE_ENV_KEYS = (
    "REPORT_DATABASE_URL",
    "TIMESCALE_DSN",
    "DATABASE_URL",
)


def _database_url() -> str:
    """Resolve the TimescaleDB connection string from the environment."""

    for env_key in _DATABASE_ENV_KEYS:
        raw_value = os.getenv(env_key)
        if raw_value is None:
            continue
        candidate = raw_value.strip()
        if not candidate:
            continue
        try:
            normalized: str = normalize_postgres_dsn(
                candidate,
                allow_sqlite=False,
                label="Explain service database URL",
            )
            return normalized
        except RuntimeError as exc:  # pragma: no cover - configuration error paths
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    if "pytest" in sys.modules:
        raise HTTPException(
            status_code=500,
            detail=(
                "Explain service database URL is required even under pytest; "
                "set REPORT_DATABASE_URL to a PostgreSQL/Timescale DSN."
            ),
        )

    raise HTTPException(
        status_code=503,
        detail=(
            "Explain service database URL is not configured. Set REPORT_DATABASE_URL "
            "or TIMESCALE_DSN to a PostgreSQL/Timescale connection string."
        ),
    )


def _normalise_feature_payload(raw: Any) -> dict[str, float]:
    """Convert a feature payload into a flat mapping of floats."""

    if isinstance(raw, Mapping):
        normalised: dict[str, float] = {}
        for key, value in raw.items():
            try:
                normalised[str(key)] = float(value)
            except (TypeError, ValueError) as exc:  # pragma: no cover - validation guard
                raise HTTPException(
                    status_code=422,
                    detail=f"Feature '{key}' is not numeric",
                ) from exc
        return normalised

    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        sequence_normalised: dict[str, float] = {}
        for idx, value in enumerate(raw):
            try:
                sequence_normalised[f"feature_{idx}"] = float(value)
            except (TypeError, ValueError) as exc:  # pragma: no cover - validation guard
                raise HTTPException(
                    status_code=422,
                    detail=f"Feature at position {idx} is not numeric",
                ) from exc
        return sequence_normalised

    return {}


def _extract_feature_mapping(trade_row: Mapping[str, Any]) -> dict[str, float]:
    """Return the normalised feature vector stored with the trade."""

    candidates = ("features", "feature_vector", "feature_values")
    for key in candidates:
        if key in trade_row:
            mapping = _normalise_feature_payload(trade_row[key])
            if mapping:
                return mapping

    metadata_candidates = (
        trade_row.get("order_metadata"),
        trade_row.get("fill_metadata"),
        trade_row.get("metadata"),
    )
    for container in metadata_candidates:
        if isinstance(container, Mapping):
            for key in candidates:
                if key in container:
                    mapping = _normalise_feature_payload(container[key])
                    if mapping:
                        return mapping

    raise HTTPException(status_code=422, detail="Trade is missing feature metadata")


def _extract_model_version(trade_row: Mapping[str, Any]) -> str | None:
    """Attempt to find the model version recorded with the trade."""

    direct_version: object = trade_row.get("model_version")
    if isinstance(direct_version, str):
        return direct_version

    metadata_candidates = (
        trade_row.get("order_metadata"),
        trade_row.get("fill_metadata"),
        trade_row.get("metadata"),
    )
    for container in metadata_candidates:
        if isinstance(container, Mapping):
            model_version: object = container.get("model_version")
            if isinstance(model_version, str) and model_version:
                return model_version
    return None


def _extract_regime_label(trade_row: Mapping[str, Any]) -> str:
    """Extract the market regime associated with the trade, if present."""

    search_space: Iterable[Any] = (
        trade_row.get("regime"),
        trade_row.get("state"),
        trade_row.get("order_metadata"),
        trade_row.get("fill_metadata"),
        trade_row.get("metadata"),
    )

    for candidate in search_space:
        if isinstance(candidate, Mapping):
            regime: object = candidate.get("regime")
            if isinstance(regime, str) and regime:
                return regime
            state: object = candidate.get("state")
            if isinstance(state, Mapping):
                nested: object = state.get("regime")
                if isinstance(nested, str) and nested:
                    return nested
        elif isinstance(candidate, str) and candidate:
            return candidate

    return "unknown"


def _resolve_model_identifier(trade_row: Mapping[str, Any], model: Any) -> str:
    """Determine the identifier to report for the model used."""

    version = _extract_model_version(trade_row)
    if version:
        return version

    for attr in ("name", "model_name", "version"):
        value = getattr(model, attr, None)
        if isinstance(value, str) and value:
            return value

    return type(model).__name__


def _load_trade_record(trade_id: str) -> Mapping[str, Any]:
    """Fetch trade context and metadata from TimescaleDB."""

    if psycopg is None:  # pragma: no cover - executed when psycopg isn't available
        raise HTTPException(
            status_code=503,
            detail="TimescaleDB driver (psycopg) is not installed in this environment.",
        )

    if dict_row is None:  # pragma: no cover - executed when psycopg rows helpers are missing
        raise HTTPException(
            status_code=503,
            detail="psycopg row factory helpers are not available in this environment.",
        )

    query = """
        SELECT
            f.fill_id,
            f.order_id,
            COALESCE(f.account_id, o.account_id) AS account_id,
            COALESCE(f.market, f.symbol, o.symbol) AS instrument,
            COALESCE(f.fill_time, f.fill_ts) AS executed_at,
            f.price,
            f.size,
            f.metadata AS fill_metadata,
            o.metadata AS order_metadata
        FROM fills AS f
        LEFT JOIN orders AS o ON o.order_id = f.order_id
        WHERE f.fill_id = %(trade_id)s
    """

    try:
        with psycopg.connect(_database_url(), row_factory=dict_row) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, {"trade_id": trade_id})
                row: Mapping[str, Any] | None = cursor.fetchone()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - database errors
        LOGGER.exception("Failed to load trade context", extra={"trade_id": trade_id})
        raise HTTPException(status_code=500, detail="Unable to load trade context") from exc

    if row is None:
        raise HTTPException(status_code=404, detail="Trade not found")

    return dict(row)


app = FastAPI(title="Explainability Service", version="1.0.0")
setup_metrics(app, service_name="explain-service")


@typed_app_get(app, "/explain/trade")
def get_trade_explanation(
    trade_id: str = Query(..., description="Unique trade/fill identifier"),
    actor_account: str = Depends(require_admin_account),
) -> JSONResponse:
    """Return the key drivers behind a trade intent for UI consumption."""

    LOGGER.info(
        "Trade explanation requested",
        extra={"trade_id": trade_id, "actor_account": actor_account},
    )

    trade = _load_trade_record(trade_id)

    account_id = trade.get("account_id")
    if not account_id:
        raise HTTPException(status_code=422, detail="Trade is missing account context")

    instrument = trade.get("instrument") or trade.get("symbol")
    if not instrument:
        raise HTTPException(status_code=422, detail="Trade is missing instrument context")

    features = _extract_feature_mapping(trade)
    model = get_active_model(str(account_id), str(instrument))

    try:
        raw_importance = model.explain(features)
    except Exception as exc:  # pragma: no cover - unexpected model errors
        LOGGER.exception(
            "Model explanation failed",
            extra={
                "trade_id": trade_id,
                "account_id": account_id,
                "actor_account": actor_account,
            },
        )
        raise HTTPException(status_code=500, detail="Unable to generate explanation") from exc

    if isinstance(raw_importance, Mapping):
        contributions = ((str(name), float(value)) for name, value in raw_importance.items())
    elif isinstance(raw_importance, Sequence) and not isinstance(
        raw_importance, (str, bytes, bytearray)
    ):
        contributions = (
            (f"feature_{idx}", float(value))
            for idx, value in enumerate(raw_importance)
        )
    else:  # pragma: no cover - unexpected return type from model.explain
        raise HTTPException(status_code=500, detail="Model explanation returned unsupported format")

    ordered = sorted(
        contributions,
        key=lambda entry: abs(entry[1]),
        reverse=True,
    )

    top_features = [
        {"feature": name, "importance": importance}
        for name, importance in ordered[:5]
    ]

    payload = {
        "trade_id": trade_id,
        "top_features": top_features,
        "regime": _extract_regime_label(trade),
        "model_used": _resolve_model_identifier(trade, model),
    }
    return JSONResponse(content=payload)


__all__ = ["app", "get_trade_explanation"]
