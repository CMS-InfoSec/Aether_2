"""Risk exposure forecasting FastAPI endpoints."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable, ClassVar, Dict, List, Mapping, Optional, Sequence, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, status

try:  # pragma: no cover - prefer psycopg (v3) when available
    import psycopg
    from psycopg import sql as psycopg_sql
    from psycopg.rows import dict_row as psycopg_dict_row
except Exception:  # pragma: no cover - runtime may only ship psycopg2
    psycopg = None  # type: ignore[assignment]
    psycopg_sql = None  # type: ignore[assignment]
    psycopg_dict_row = None  # type: ignore[assignment]

try:  # pragma: no cover - retain psycopg2 compatibility
    import psycopg2
    from psycopg2 import sql as psycopg2_sql
    from psycopg2.extras import RealDictCursor
except Exception:  # pragma: no cover - fallback when psycopg2 is unavailable
    psycopg2 = None  # type: ignore[assignment]
    psycopg2_sql = None  # type: ignore[assignment]
    RealDictCursor = Any  # type: ignore[assignment]

_SQL_MODULE = psycopg_sql or psycopg2_sql

from shared.common_bootstrap import ensure_common_helpers

ensure_common_helpers()

from services.common.config import get_timescale_session
from services.common.security import require_admin_account

from ml.exposure_forecaster import ExposureModelManager, TrainingSummary
from ml.models.supervised import (
    MissingDependencyError,
    SklearnPipelineTrainer,
    SupervisedDataset,
    SupervisedTrainer,
)


PNL_CURVE_QUERY = """
SELECT
    as_of,
    COALESCE(nav, 0) AS nav
FROM pnl_curves
WHERE account_id = %(account_id)s
  AND as_of >= %(start)s
  AND as_of < %(end)s
ORDER BY as_of
"""


DAILY_FEES_AND_VOLUME_QUERY = """
SELECT
    DATE_TRUNC('day', f.fill_time) AS day,
    COALESCE(SUM(ABS(f.quantity * f.price)), 0) AS notional,
    COALESCE(SUM(f.fee), 0) AS fees
FROM fills AS f
JOIN orders AS o ON o.order_id = f.order_id
WHERE o.account_id = %(account_id)s
  AND f.fill_time >= %(start)s
  AND f.fill_time < %(end)s
GROUP BY day
ORDER BY day
"""


LATEST_POSITIONS_QUERY = """
WITH latest_positions AS (
    SELECT DISTINCT ON (market)
        market,
        quantity,
        COALESCE(entry_price, 0) AS entry_price,
        as_of
    FROM positions
    WHERE account_id = %(account_id)s
    ORDER BY market, as_of DESC
)
SELECT market, quantity, entry_price
FROM latest_positions
"""


OHLCV_FEATURE_QUERY = """
SELECT
    market,
    bucket_start,
    open,
    high,
    low,
    close,
    volume
FROM ohlcv_bars
WHERE market = ANY(%(markets)s)
  AND bucket_start >= %(start)s
  AND bucket_start < %(end)s
ORDER BY bucket_start
"""


LOGGER = logging.getLogger(__name__)


DecimalLike = Decimal | float | int | str

ZERO = Decimal("0")
CONFIDENCE_Z_SCORE = Decimal("1.96")
DEFAULT_QUANTIZATION = Decimal("0.00000001")


def _parse_datetime(value: Any) -> Optional[datetime]:
    """Return *value* normalised to an aware UTC ``datetime`` when possible."""

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def _as_decimal(value: DecimalLike) -> Decimal:
    """Convert arbitrary numeric inputs to :class:`~decimal.Decimal`."""

    if isinstance(value, Decimal):
        return value
    if isinstance(value, str):
        return Decimal(value)
    if isinstance(value, int):
        return Decimal(value)
    return Decimal(str(value))


def _population_std(values: Sequence[Decimal]) -> Decimal:
    """Return the population standard deviation of *values* as a decimal."""

    if len(values) <= 1:
        return ZERO

    mean = sum(values, ZERO) / Decimal(len(values))
    variance = sum((value - mean) ** 2 for value in values) / Decimal(len(values))
    return variance.sqrt()


@dataclass(slots=True)
class ForecastResult:
    """Structured response describing a forecast value and its confidence interval."""

    value: Decimal
    lower: Decimal
    upper: Decimal
    horizon_days: int
    quantization: Decimal = field(default=DEFAULT_QUANTIZATION)

    def _quantize(self, value: Decimal) -> Decimal:
        return value.quantize(self.quantization, rounding=ROUND_HALF_UP)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "value": float(self._quantize(self.value)),
            "confidence_interval": [
                float(self._quantize(self.lower)),
                float(self._quantize(self.upper)),
            ],
            "horizon_days": self.horizon_days,
        }


class _ExposureMLPipeline:
    """Materialises features from TimescaleDB and predicts exposure via regression."""

    def __init__(
        self,
        account_id: str,
        retrain_cadence: timedelta,
        lookback_days: int,
        trainer_factory: Callable[[], SupervisedTrainer],
    ) -> None:
        self._account_id = account_id
        self._retrain_cadence = retrain_cadence
        self._lookback_window = timedelta(days=lookback_days)
        self._trainer_factory = trainer_factory
        self._factory_tag = id(trainer_factory)
        self._trainer: SupervisedTrainer | None = None
        self._last_trained: datetime | None = None
        self._residual_std: float = 0.0
        self._training_samples: int = 0
        self._model_manager: ExposureModelManager | None = None
        self._last_summary: TrainingSummary | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def forecast(
        self,
        *,
        nav_rows: Sequence[Mapping[str, Any]],
        positions_rows: Sequence[Mapping[str, Any]],
        now: datetime,
        horizon_days: int,
    ) -> ForecastResult | None:
        if not nav_rows or not positions_rows:
            return None

        try:  # Optional dependency guard for environments lacking pandas
            import pandas as pd  # type: ignore
        except Exception:  # pragma: no cover - optional dependency
            LOGGER.debug("pandas unavailable; skipping ML exposure forecast")
            return None

        features_frame = self._load_feature_frame(positions_rows, now)
        if features_frame is None:
            return None

        dataset = self._prepare_dataset(nav_rows, positions_rows, features_frame, pd)
        if dataset is None or dataset.features.empty:
            return None

        latest_features = dataset.features.iloc[[-1]]
        if self._trainer is None or self._needs_retrain(now):
            if not self._train(dataset, now):
                return None

        return self._predict(latest_features, horizon_days)

    @property
    def factory_tag(self) -> int:
        return self._factory_tag

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _needs_retrain(self, now: datetime) -> bool:
        if self._last_trained is None:
            return True
        return now - self._last_trained >= self._retrain_cadence

    def _train(self, dataset: SupervisedDataset, now: datetime) -> bool:
        manager = self._ensure_model_manager()
        if manager is None:
            LOGGER.debug("Exposure model manager unavailable; skipping ML retrain")
            return False

        try:
            summary = manager.train(dataset, trained_at=now)
        except MissingDependencyError:
            LOGGER.debug("Missing dependencies for ML exposure pipeline", exc_info=True)
            return False
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.debug("Exposure ML trainer failed", exc_info=True)
            return False

        self._trainer = manager.trainer
        self._last_trained = now
        self._training_samples = summary.samples
        residual_std = summary.metrics.get("residual_std", 0.0)
        try:
            self._residual_std = float(residual_std)
        except (TypeError, ValueError):  # pragma: no cover - defensive guard
            self._residual_std = 0.0
        self._last_summary = summary

        return True

    def _predict(
        self, latest_features: "pd.DataFrame", horizon_days: int
    ) -> ForecastResult | None:
        try:
            manager = self._ensure_model_manager()
        except Exception:  # pragma: no cover - defensive guard
            LOGGER.debug("Exposure model manager unavailable during prediction", exc_info=True)
            return None

        if manager is None:
            return None

        try:
            predictions = manager.predict(latest_features)
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.debug("Exposure ML prediction failed", exc_info=True)
            return None

        try:
            first_prediction = predictions[0]
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.debug("Exposure ML prediction result was empty", exc_info=True)
            return None

        try:
            prediction_value = float(first_prediction)
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.debug("Exposure ML prediction result could not be coerced to float", exc_info=True)
            return None

        value = max(Decimal(str(prediction_value)), ZERO)

        margin = ZERO
        if self._residual_std > 0.0 and self._training_samples > 1:
            margin = CONFIDENCE_Z_SCORE * Decimal(str(self._residual_std))
        elif value > ZERO:
            margin = value * Decimal("0.1")

        lower = max(value - margin, ZERO)
        upper = value + margin
        return ForecastResult(value=value, lower=lower, upper=upper, horizon_days=horizon_days)

    def _load_feature_frame(
        self, positions_rows: Sequence[Mapping[str, Any]], now: datetime
    ) -> "pd.DataFrame" | None:
        try:
            import pandas as pd  # type: ignore
        except Exception:  # pragma: no cover - optional dependency
            return None

        markets = sorted(
            {
                str(row.get("market", "")).upper()
                for row in positions_rows
                if isinstance(row.get("market"), str)
            }
        )
        markets = [market for market in markets if market]
        if not markets:
            return None

        start = now - self._lookback_window
        params = {"markets": markets, "start": start, "end": now}
        try:
            rows = self._query_timescale(OHLCV_FEATURE_QUERY, params)
        except Exception:  # pragma: no cover - defensive guard
            LOGGER.debug("Failed to query OHLCV features for exposure forecast", exc_info=True)
            return None

        if not rows:
            return None

        frame = pd.DataFrame(rows)
        frame["bucket_start"] = pd.to_datetime(frame["bucket_start"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["bucket_start"])
        if frame.empty:
            return None

        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = frame[column].apply(lambda value: float(_as_decimal(value or ZERO)))

        frame = frame.sort_values(["market", "bucket_start"]).reset_index(drop=True)
        frame["return"] = frame.groupby("market")["close"].pct_change().fillna(0.0)
        return frame

    def _prepare_dataset(
        self,
        nav_rows: Sequence[Mapping[str, Any]],
        positions_rows: Sequence[Mapping[str, Any]],
        feature_frame: "pd.DataFrame",
        pd: Any,
    ) -> SupervisedDataset | None:
        nav_frame = pd.DataFrame(nav_rows)
        if nav_frame.empty:
            return None

        nav_frame["as_of"] = nav_frame["as_of"].apply(_parse_datetime)
        nav_frame = nav_frame.dropna(subset=["as_of", "nav"])
        if nav_frame.empty:
            return None

        nav_frame["nav"] = nav_frame["nav"].apply(lambda value: float(_as_decimal(value or ZERO)))
        nav_frame = nav_frame.sort_values("as_of").reset_index(drop=True)
        nav_frame["nav_return"] = nav_frame["nav"].pct_change().fillna(0.0)
        nav_frame["nav_ema"] = nav_frame["nav"].ewm(span=5, adjust=False).mean().fillna(method="bfill")

        exposures_total = ZERO
        for row in positions_rows:
            quantity = _as_decimal(row.get("quantity", ZERO) or ZERO)
            entry_price = _as_decimal(row.get("entry_price", ZERO) or ZERO)
            exposures_total += abs(quantity * entry_price)

        nav_latest = nav_frame["nav"].iloc[-1] if not nav_frame.empty else 0.0
        leverage_ratio = float(exposures_total) / nav_latest if nav_latest > 0 else 1.0
        nav_frame["exposure_target"] = nav_frame["nav"] * leverage_ratio

        grouped = feature_frame.groupby("bucket_start").agg(
            close_mean=("close", "mean"),
            close_std=("close", "std"),
            volume_sum=("volume", "sum"),
            volume_mean=("volume", "mean"),
            return_mean=("return", "mean"),
            return_std=("return", "std"),
            high_max=("high", "max"),
            low_min=("low", "min"),
        )
        grouped["high_low_spread"] = grouped["high_max"] - grouped["low_min"]
        grouped["market_count"] = feature_frame.groupby("bucket_start")["market"].nunique()
        grouped = grouped.fillna(0.0)

        merged = pd.merge_asof(
            nav_frame,
            grouped.sort_index().reset_index(),
            left_on="as_of",
            right_on="bucket_start",
            direction="backward",
            tolerance=pd.Timedelta("1H"),
        )

        merged = merged.dropna(subset=["close_mean", "volume_sum", "return_mean"])
        if merged.empty:
            return None

        feature_columns = [
            "close_mean",
            "close_std",
            "volume_sum",
            "volume_mean",
            "return_mean",
            "return_std",
            "high_low_spread",
            "market_count",
            "nav",
            "nav_return",
            "nav_ema",
        ]

        features = merged[feature_columns].fillna(method="ffill").fillna(0.0)
        labels = merged["exposure_target"].astype(float)
        valid_mask = labels.notna()
        features = features[valid_mask]
        labels = labels[valid_mask]
        if features.empty:
            return None

        return SupervisedDataset(features=features, labels=labels)

    def _ensure_model_manager(self) -> ExposureModelManager | None:
        if self._model_manager is not None:
            return self._model_manager
        try:
            self._model_manager = ExposureModelManager(
                self._account_id, trainer_factory=self._trainer_factory
            )
        except Exception:  # pragma: no cover - optional dependency guard
            LOGGER.debug("Failed to initialise exposure model manager", exc_info=True)
            self._model_manager = None
        return self._model_manager

    def _query_timescale(self, query: str, params: Mapping[str, Any]) -> List[Dict[str, Any]]:
        if _SQL_MODULE is None:
            raise RuntimeError("Timescale SQL helpers unavailable")

        session = get_timescale_session(self._account_id)
        connection: Any | None = None
        cursor: Any | None = None
        rows: Sequence[Mapping[str, Any]] | None = None
        try:
            if psycopg is not None:
                assert psycopg_dict_row is not None  # nosec - guarded by import
                connection = psycopg.connect(session.dsn)
                connection.autocommit = True
                cursor = connection.cursor(row_factory=psycopg_dict_row)
            elif psycopg2 is not None:
                connection = psycopg2.connect(session.dsn)
                connection.autocommit = True
                cursor = connection.cursor(cursor_factory=RealDictCursor)
            else:  # pragma: no cover - guarded by _SQL_MODULE check
                raise RuntimeError("psycopg or psycopg2 is required for exposure ML queries")

            if session.account_schema:
                statement = _SQL_MODULE.SQL("SET search_path TO {}, public").format(
                    _SQL_MODULE.Identifier(session.account_schema)
                )
                cursor.execute(statement)

            cursor.execute(query, params)
            rows = cursor.fetchall()
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

        return [dict(row) for row in rows or []]


_ML_PIPELINE_CACHE: Dict[Tuple[str, int, int], _ExposureMLPipeline] = {}


def get_exposure_ml_pipeline(
    account_id: str,
    *,
    retrain_cadence: timedelta | None = None,
    lookback_days: int = 90,
    trainer_factory: Callable[[], SupervisedTrainer] | None = None,
) -> _ExposureMLPipeline:
    """Return a cached ML exposure pipeline for ``account_id``."""

    cadence = retrain_cadence or timedelta(minutes=60)
    factory = trainer_factory or SklearnPipelineTrainer
    key = (account_id, int(cadence.total_seconds()), lookback_days)
    pipeline = _ML_PIPELINE_CACHE.get(key)
    if pipeline is None or pipeline.factory_tag != id(factory):
        pipeline = _ExposureMLPipeline(account_id, cadence, lookback_days, factory)
        _ML_PIPELINE_CACHE[key] = pipeline
    return pipeline


class _BaseExposureStore:
    """Abstract storage backend used by :class:`ExposureForecaster`."""

    def __init__(self, account_id: str) -> None:
        self._account_id = account_id

    def fetch_nav_history(self, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def fetch_fee_history(self, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def fetch_positions(self) -> List[Dict[str, Any]]:
        raise NotImplementedError


class _PsycopgExposureStore(_BaseExposureStore):
    """Timescale-backed exposure store using psycopg-compatible connections."""

    def __init__(self, account_id: str) -> None:
        if _SQL_MODULE is None or (psycopg is None and psycopg2 is None):
            raise RuntimeError(
                "psycopg or psycopg2 is not available; cannot use the database-backed exposure store"
            )
        super().__init__(account_id)
        self._session = get_timescale_session(account_id)

    def _query(self, query: str, params: Mapping[str, Any]) -> List[Dict[str, Any]]:
        if _SQL_MODULE is None:
            raise RuntimeError(
                "psycopg or psycopg2 is not available; cannot execute Timescale queries"
            )

        connection: Any | None = None
        cursor: Any | None = None
        try:
            if psycopg is not None:
                assert psycopg_dict_row is not None  # nosec - guarded by import
                connection = psycopg.connect(self._session.dsn)
                connection.autocommit = True
                cursor = connection.cursor(row_factory=psycopg_dict_row)
            elif psycopg2 is not None:
                connection = psycopg2.connect(self._session.dsn)
                connection.autocommit = True
                cursor = connection.cursor(cursor_factory=RealDictCursor)
            else:  # pragma: no cover - guarded by _SQL_MODULE check
                raise RuntimeError(
                    "psycopg or psycopg2 is not available; cannot execute Timescale queries"
                )

            if self._session.account_schema:
                statement = _SQL_MODULE.SQL("SET search_path TO {}, public").format(
                    _SQL_MODULE.Identifier(self._session.account_schema)
                )
                cursor.execute(statement)
            cursor.execute(query, params)
            rows = cursor.fetchall()
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

        return [dict(row) for row in rows]

    def fetch_nav_history(self, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        return self._query(
            PNL_CURVE_QUERY,
            {"account_id": self._account_id, "start": start, "end": end},
        )

    def fetch_fee_history(self, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        return self._query(
            DAILY_FEES_AND_VOLUME_QUERY,
            {"account_id": self._account_id, "start": start, "end": end},
        )

    def fetch_positions(self) -> List[Dict[str, Any]]:
        return self._query(LATEST_POSITIONS_QUERY, {"account_id": self._account_id})


class _InMemoryExposureStore(_BaseExposureStore):
    """In-memory fallback that mimics the exposure queries."""

    _NAV_HISTORY: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}
    _FEE_HISTORY: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}
    _POSITIONS: ClassVar[Dict[str, List[Dict[str, Any]]]] = {}
    _LOCK: ClassVar[threading.Lock] = threading.Lock()

    def fetch_nav_history(self, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        with self._LOCK:
            rows = [row.copy() for row in self._NAV_HISTORY.get(self._account_id, [])]

        if not rows:
            return []

        filtered: List[Dict[str, Any]] = []
        for row in rows:
            as_of = _parse_datetime(row.get("as_of"))
            if as_of is not None and not (start <= as_of < end):
                continue
            entry = row.copy()
            if as_of is not None:
                entry["as_of"] = as_of
            filtered.append(entry)
        return filtered

    def fetch_fee_history(self, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        with self._LOCK:
            rows = [row.copy() for row in self._FEE_HISTORY.get(self._account_id, [])]

        if not rows:
            return []

        filtered: List[Dict[str, Any]] = []
        for row in rows:
            day = _parse_datetime(row.get("day"))
            if day is not None and not (start <= day < end):
                continue
            entry = row.copy()
            if day is not None:
                entry["day"] = day
            filtered.append(entry)
        return filtered

    def fetch_positions(self) -> List[Dict[str, Any]]:
        with self._LOCK:
            rows = [row.copy() for row in self._POSITIONS.get(self._account_id, [])]
        return rows

    @classmethod
    def seed_nav_history(cls, account_id: str, rows: Sequence[Mapping[str, Any]]) -> None:
        prepared: List[Dict[str, Any]] = []
        for row in rows:
            entry = dict(row)
            entry.setdefault("nav", ZERO)
            parsed = _parse_datetime(entry.get("as_of"))
            if parsed is not None:
                entry["as_of"] = parsed
            prepared.append(entry)

        prepared.sort(
            key=lambda item: item.get("as_of") or datetime.min.replace(tzinfo=timezone.utc)
        )

        with cls._LOCK:
            cls._NAV_HISTORY[account_id] = prepared

    @classmethod
    def seed_fee_history(cls, account_id: str, rows: Sequence[Mapping[str, Any]]) -> None:
        prepared: List[Dict[str, Any]] = []
        for row in rows:
            entry = dict(row)
            parsed = _parse_datetime(entry.get("day"))
            if parsed is not None:
                entry["day"] = parsed
            entry.setdefault("notional", ZERO)
            entry.setdefault("fees", ZERO)
            prepared.append(entry)

        prepared.sort(
            key=lambda item: item.get("day") or datetime.min.replace(tzinfo=timezone.utc)
        )

        with cls._LOCK:
            cls._FEE_HISTORY[account_id] = prepared

    @classmethod
    def seed_positions(cls, account_id: str, rows: Sequence[Mapping[str, Any]]) -> None:
        prepared = [dict(row) for row in rows]
        with cls._LOCK:
            cls._POSITIONS[account_id] = prepared

    @classmethod
    def reset(cls, account_id: Optional[str] = None) -> None:
        with cls._LOCK:
            if account_id is None:
                cls._NAV_HISTORY.clear()
                cls._FEE_HISTORY.clear()
                cls._POSITIONS.clear()
                return
            cls._NAV_HISTORY.pop(account_id, None)
            cls._FEE_HISTORY.pop(account_id, None)
            cls._POSITIONS.pop(account_id, None)


if psycopg2 is None:
    LOGGER.warning(
        "psycopg2 is not installed; exposure forecast service will use an in-memory store"
    )
    _DEFAULT_EXPOSURE_STORE: Callable[[str], _BaseExposureStore] = _InMemoryExposureStore
else:
    _DEFAULT_EXPOSURE_STORE = _PsycopgExposureStore


def seed_exposure_store(
    account_id: str,
    *,
    nav_history: Sequence[Mapping[str, Any]] | None = None,
    fee_history: Sequence[Mapping[str, Any]] | None = None,
    positions: Sequence[Mapping[str, Any]] | None = None,
) -> None:
    """Populate the in-memory exposure store with deterministic fixtures."""

    if nav_history is not None:
        _InMemoryExposureStore.seed_nav_history(account_id, nav_history)
    if fee_history is not None:
        _InMemoryExposureStore.seed_fee_history(account_id, fee_history)
    if positions is not None:
        _InMemoryExposureStore.seed_positions(account_id, positions)


def reset_exposure_store(account_id: Optional[str] = None) -> None:
    """Clear cached exposure data for ``account_id`` or every account."""

    _InMemoryExposureStore.reset(account_id)

class ExposureForecaster:
    """Domain service encapsulating the exposure forecasting logic."""

    def __init__(
        self,
        *,
        account_id: str,
        window_days: int = 90,
        store_factory: Callable[[str], _BaseExposureStore] | None = None,
        ml_retrain_minutes: int = 60,
        ml_trainer_factory: Callable[[], SupervisedTrainer] | None = None,
    ) -> None:
        self._account_id = account_id
        self._window_days = window_days

        factory = store_factory or _DEFAULT_EXPOSURE_STORE
        try:
            self._store = factory(account_id)
        except Exception as exc:
            if factory is _InMemoryExposureStore:
                raise
            factory_name = getattr(factory, "__name__", factory.__class__.__name__)
            LOGGER.warning(
                "Failed to initialise %s for account %s: %s; using in-memory exposure store",
                factory_name,
                account_id,
                exc,
            )
            self._store = _InMemoryExposureStore(account_id)

        self._ml_pipeline: _ExposureMLPipeline | None = None
        self._ml_trainer_factory = ml_trainer_factory or SklearnPipelineTrainer
        try:
            self._ml_pipeline = get_exposure_ml_pipeline(
                account_id,
                retrain_cadence=timedelta(minutes=ml_retrain_minutes),
                lookback_days=window_days,
                trainer_factory=self._ml_trainer_factory,
            )
        except Exception:  # pragma: no cover - optional dependency
            LOGGER.debug(
                "ML exposure pipeline unavailable for account %s", account_id, exc_info=True
            )
            self._ml_pipeline = None

    def forecast(self) -> Dict[str, Any]:
        """Generate the NAV, fee, and margin forecasts for the account."""

        now = datetime.now(timezone.utc)
        start = now - timedelta(days=self._window_days)

        nav_rows = self._store.fetch_nav_history(start, now)
        if not nav_rows:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No NAV history available for account",
            )

        peak_nav = ZERO
        max_drawdown = ZERO
        for row in nav_rows:
            nav_value = _as_decimal(row.get("nav", ZERO) or ZERO)
            if nav_value > peak_nav:
                peak_nav = nav_value
            if peak_nav > ZERO:
                drawdown = (peak_nav - nav_value) / peak_nav
                if drawdown > max_drawdown:
                    max_drawdown = drawdown

        fees_rows = self._store.fetch_fee_history(start, now)
        positions_rows = self._store.fetch_positions()

        nav_forecast = self._forecast_nav_volatility(nav_rows)
        fee_forecast = self._forecast_fee_spend(fees_rows)

        ml_forecast: ForecastResult | None = None
        if self._ml_pipeline is not None:
            try:
                ml_forecast = self._ml_pipeline.forecast(
                    nav_rows=nav_rows,
                    positions_rows=positions_rows,
                    now=now,
                    horizon_days=nav_forecast.horizon_days,
                )
            except Exception:  # pragma: no cover - best effort integration
                LOGGER.debug(
                    "Failed to compute ML-based exposure forecast for %s",
                    self._account_id,
                    exc_info=True,
                )
                ml_forecast = None

        margin_forecast = self._forecast_margin_usage(
            positions_rows, nav_forecast, ml_forecast
        )

        try:  # pragma: no cover - metrics module optional during unit tests
            from metrics import record_account_drawdown, record_account_exposure

            record_account_drawdown(
                self._account_id,
                drawdown_pct=float(max_drawdown),
                service="risk",
            )
            for position in positions_rows:
                symbol = str(position.get("market") or "")
                quantity = _as_decimal(position.get("quantity", ZERO) or ZERO)
                entry_price = _as_decimal(position.get("entry_price", ZERO) or ZERO)
                exposure_value = abs(quantity * entry_price)
                record_account_exposure(
                    self._account_id,
                    symbol=symbol,
                    exposure_usd=float(exposure_value),
                    service="risk",
                )
        except Exception:  # pragma: no cover - metrics emission is best effort
            LOGGER.debug(
                "Failed to record exposure metrics for account %s", self._account_id, exc_info=True
            )

        return {
            "account_id": self._account_id,
            "as_of": now.isoformat(),
            "projected_nav_volatility": nav_forecast.as_dict(),
            "projected_fee_spend": fee_forecast.as_dict(),
            "projected_margin_usage": margin_forecast.as_dict(),
        }

    def _forecast_nav_volatility(self, rows: Sequence[Mapping[str, Any]]) -> ForecastResult:
        nav_values = [_as_decimal(row.get("nav", ZERO) or ZERO) for row in rows]
        if len(nav_values) < 2:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Not enough NAV observations to compute volatility",
            )

        returns: List[Decimal] = []
        for previous, current in zip(nav_values, nav_values[1:]):
            if previous != ZERO:
                returns.append((current - previous) / previous)
            else:
                returns.append(ZERO)

        if not returns:
            return ForecastResult(value=ZERO, lower=ZERO, upper=ZERO, horizon_days=7)

        span = min(20, len(returns))
        alpha = Decimal(2) / Decimal(span + 1)
        complement = Decimal(1) - alpha
        ewma_var = returns[0] ** 2
        for ret in returns[1:]:
            ewma_var = alpha * (ret**2) + complement * ewma_var

        daily_vol = max(ewma_var, ZERO).sqrt()
        horizon_days = 7
        horizon_vol = daily_vol * Decimal(horizon_days).sqrt()

        window_vols = self._rolling_window_vols(
            returns, window=span, horizon_days=horizon_days
        )
        std_dev = _population_std(window_vols)
        lower, upper = self._confidence_interval(
            horizon_vol, std_dev, max(len(window_vols), 1)
        )

        return ForecastResult(
            value=horizon_vol,
            lower=lower,
            upper=upper,
            horizon_days=horizon_days,
            quantization=Decimal("0.00000001"),
        )

    def _forecast_fee_spend(self, rows: Sequence[Mapping[str, Any]]) -> ForecastResult:
        if not rows:
            return ForecastResult(value=ZERO, lower=ZERO, upper=ZERO, horizon_days=7)

        notional_totals = [_as_decimal(row.get("notional", ZERO) or ZERO) for row in rows]
        fee_totals = [_as_decimal(row.get("fees", ZERO) or ZERO) for row in rows]

        total_notional = sum(notional_totals, ZERO)
        total_fees = sum(fee_totals, ZERO)
        observations = len(rows)

        avg_daily_volume = (
            total_notional / Decimal(observations) if observations else ZERO
        )
        avg_fee_rate = (total_fees / total_notional) if total_notional else ZERO

        horizon_days = 7
        horizon_decimal = Decimal(horizon_days)
        projected_volume = avg_daily_volume * horizon_decimal
        projected_fees = projected_volume * avg_fee_rate

        std_dev_daily = _population_std(fee_totals)
        std_dev_horizon = std_dev_daily * horizon_decimal.sqrt()
        lower, upper = self._confidence_interval(
            projected_fees, std_dev_horizon, max(observations, 1)
        )

        return ForecastResult(
            value=projected_fees,
            lower=lower,
            upper=upper,
            horizon_days=horizon_days,
        )

    def _forecast_margin_usage(
        self,
        rows: Sequence[Mapping[str, Any]],
        nav_volatility: ForecastResult,
        ml_exposure: ForecastResult | None = None,
    ) -> ForecastResult:
        if not rows and ml_exposure is None:
            return ForecastResult(value=ZERO, lower=ZERO, upper=ZERO, horizon_days=7)

        exposure = ZERO
        for row in rows:
            quantity = _as_decimal(row.get("quantity", ZERO) or ZERO)
            entry_price = _as_decimal(row.get("entry_price", ZERO) or ZERO)
            exposure += abs(quantity * entry_price)

        base_value = exposure
        base_lower = exposure
        base_upper = exposure
        if ml_exposure is not None:
            base_value = max(ml_exposure.value, ZERO)
            base_lower = max(ml_exposure.lower, ZERO)
            base_upper = max(ml_exposure.upper, ZERO)

        nav_candidates = [
            Decimal(1) + nav_volatility.value,
            Decimal(1) + nav_volatility.lower,
            Decimal(1) + nav_volatility.upper,
        ]

        projected_usage = base_value * nav_candidates[0]
        scaled_candidates = [
            base_lower * nav_candidates[1],
            base_lower * nav_candidates[2],
            base_upper * nav_candidates[1],
            base_upper * nav_candidates[2],
        ]

        scaled_candidates.append(projected_usage)
        lower_usage = max(min(scaled_candidates), ZERO)
        upper_usage = max(scaled_candidates)

        return ForecastResult(
            value=projected_usage,
            lower=lower_usage,
            upper=upper_usage,
            horizon_days=nav_volatility.horizon_days,
        )

    @staticmethod
    def _rolling_window_vols(
        returns: Sequence[Decimal], *, window: int, horizon_days: int
    ) -> List[Decimal]:
        if window <= 1:
            horizon_factor = Decimal(horizon_days).sqrt()
            return [abs(r) * horizon_factor for r in returns]

        vols: List[Decimal] = []
        horizon_factor = Decimal(horizon_days).sqrt()
        for idx in range(window, len(returns) + 1):
            window_slice = returns[idx - window : idx]
            if not window_slice:
                continue
            variance = sum(ret**2 for ret in window_slice) / Decimal(len(window_slice))
            vols.append(max(variance, ZERO).sqrt() * horizon_factor)
        return vols or [abs(r) * horizon_factor for r in returns]

    @staticmethod
    def _confidence_interval(
        value: Decimal, std_dev: Decimal, samples: int
    ) -> Tuple[Decimal, Decimal]:
        if samples <= 1 or std_dev <= ZERO:
            lower = upper = max(value, ZERO)
            return lower, upper

        margin = CONFIDENCE_Z_SCORE * (
            std_dev / Decimal(samples).sqrt()
        )
        lower = max(value - margin, ZERO)
        upper = value + margin
        return lower, upper


router = APIRouter(prefix="/risk", tags=["risk"])


@router.get("/forecast")
def get_exposure_forecast(
    requested_account_id: str = Query(..., alias="account_id", description="Account identifier"),
    authorized_account_id: str = Depends(require_admin_account),
) -> Dict[str, Any]:
    """Return projected NAV volatility, fee spend, and margin usage for the next week."""

    if requested_account_id != authorized_account_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account mismatch between credential and requested account",
        )

    forecaster = ExposureForecaster(account_id=requested_account_id)
    return forecaster.forecast()


__all__ = [
    "router",
    "ExposureForecaster",
    "ForecastResult",
    "seed_exposure_store",
    "reset_exposure_store",
]

