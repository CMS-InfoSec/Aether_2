"""Feast feature materialisation pipeline for engineered market factors."""

from __future__ import annotations

import argparse
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
from sqlalchemy import Column, DateTime, MetaData, String, Table, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert as pg_insert

try:  # Optional dependency
    from feast import FeatureStore
except ImportError:  # pragma: no cover - optional at runtime
    FeatureStore = None  # type: ignore

try:  # Optional dependency
    import great_expectations as ge
    from great_expectations.checkpoint import SimpleCheckpoint
    from great_expectations.core.batch import RuntimeBatchRequest
except ImportError:  # pragma: no cover - optional at runtime
    ge = None  # type: ignore
    SimpleCheckpoint = None  # type: ignore
    RuntimeBatchRequest = None  # type: ignore


LOGGER = logging.getLogger(__name__)

SEMVER_PATTERN = re.compile(r"^v\d+\.\d+\.\d+$")

DEFAULT_DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://localhost:5432/aether")
DEFAULT_FEAST_REPO = Path(os.getenv("FEAST_REPO_PATH", "data/feast"))
DEFAULT_FEATURE_VIEW = os.getenv("FEATURE_VIEW_NAME", "market_technical_features")
DEFAULT_GE_ROOT = Path(os.getenv("GE_ROOT", "data/great_expectations"))
DEFAULT_GE_CHECKPOINT = os.getenv("GE_FEATURE_CHECKPOINT", "feature_schema_checkpoint")
DEFAULT_GE_SUITE = os.getenv("GE_FEATURE_SUITE", "engineered_feature_schema")
OHLCV_TABLE = os.getenv("OHLCV_TABLE", "ohlcv_bars")
TRADES_TABLE = os.getenv("AGG_TRADES_TABLE", "aggregated_trades")
ENTITY_COLUMN = os.getenv("FEATURE_ENTITY_COLUMN", "market")
EVENT_TIMESTAMP_COLUMN = os.getenv("FEATURE_EVENT_TIMESTAMP", "event_timestamp")
CREATED_AT_COLUMN = os.getenv("FEATURE_CREATED_AT", "created_at")
FEATURE_VERSION_TABLE = os.getenv("FEATURE_VERSION_TABLE", "feature_versions")


@dataclass(frozen=True)
class FeatureBuildConfig:
    """Configuration for a feature build execution."""

    symbols: tuple[str, ...]
    granularity: str
    version: str
    changelog: str
    start: datetime | None = None
    end: datetime | None = None

    def validate(self) -> None:
        if not self.symbols:
            raise ValueError("At least one symbol must be provided")
        if not SEMVER_PATTERN.match(self.version):
            raise ValueError(
                f"Feature version '{self.version}' does not follow semantic versioning (vMAJOR.MINOR.PATCH)"
            )


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Materialise engineered features into Feast")
    parser.add_argument("--symbols", required=True, help="Comma separated list of symbols (e.g., BTC,ETH)")
    parser.add_argument("--gran", required=True, help="Bar granularity identifier (e.g., 1m, 5m)")
    parser.add_argument("--version", required=True, help="Semantic feature version identifier (e.g., v1.0.0)")
    parser.add_argument("--changelog", required=True, help="Changelog entry describing this version")
    parser.add_argument("--start", help="Optional ISO timestamp lower bound")
    parser.add_argument("--end", help="Optional ISO timestamp upper bound")
    return parser


def _parse_symbols(raw: str) -> tuple[str, ...]:
    return tuple(sorted({symbol.strip().upper() for symbol in raw.split(",") if symbol.strip()}))


def _parse_optional_ts(raw: str | None) -> datetime | None:
    if not raw:
        return None
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed


def _granularity_to_timedelta(granularity: str) -> pd.Timedelta:
    try:
        delta = pd.to_timedelta(granularity)
    except ValueError:
        LOGGER.warning("Unable to parse granularity '%s'; defaulting to 1 minute", granularity)
        return pd.Timedelta("1min")
    if delta <= pd.Timedelta(0):
        LOGGER.warning("Non-positive granularity '%s'; defaulting to 1 minute", granularity)
        return pd.Timedelta("1min")
    return delta


def _resolve_engine() -> Engine:
    LOGGER.debug("Connecting to TimescaleDB using %s", DEFAULT_DATABASE_URL)
    return create_engine(DEFAULT_DATABASE_URL)


def _load_dataframe(engine: Engine, stmt: Any, params: Mapping[str, object]) -> pd.DataFrame:
    try:
        with engine.connect() as connection:
            result = connection.execute(stmt, params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
    except SQLAlchemyError as exc:  # pragma: no cover - database failure
        raise RuntimeError("Failed to fetch data from TimescaleDB") from exc
    if df.empty:
        LOGGER.warning("No rows returned for params: %s", params)
    return df


def _fetch_ohlcv(
    engine: Engine,
    symbol: str,
    granularity: str,
    *,
    start: datetime | None,
    end: datetime | None,
) -> pd.DataFrame:
    query = text(
        f"""
        SELECT
            bucket_start AS {EVENT_TIMESTAMP_COLUMN},
            open,
            high,
            low,
            close,
            volume,
            COALESCE(vwap, NULL) AS vwap
        FROM {OHLCV_TABLE}
        WHERE market = :symbol
          AND granularity = :granularity
          AND (:start IS NULL OR bucket_start >= :start)
          AND (:end IS NULL OR bucket_start <= :end)
        ORDER BY bucket_start
        """
    )
    params = {"symbol": symbol, "granularity": granularity, "start": start, "end": end}
    frame = _load_dataframe(engine, query, params)
    if frame.empty:
        return frame
    frame[EVENT_TIMESTAMP_COLUMN] = pd.to_datetime(frame[EVENT_TIMESTAMP_COLUMN], utc=True)
    frame = frame.set_index(EVENT_TIMESTAMP_COLUMN).sort_index()
    return frame


def _fetch_microstructure(
    engine: Engine,
    symbol: str,
    granularity: str,
    *,
    start: datetime | None,
    end: datetime | None,
) -> pd.DataFrame:
    query = text(
        f"""
        SELECT
            bucket_start AS {EVENT_TIMESTAMP_COLUMN},
            trade_count,
            buy_volume,
            sell_volume,
            buy_trades,
            sell_trades,
            avg_trade_size,
            vwap AS trade_vwap
        FROM {TRADES_TABLE}
        WHERE market = :symbol
          AND granularity = :granularity
          AND (:start IS NULL OR bucket_start >= :start)
          AND (:end IS NULL OR bucket_start <= :end)
        ORDER BY bucket_start
        """
    )
    params = {"symbol": symbol, "granularity": granularity, "start": start, "end": end}
    frame = _load_dataframe(engine, query, params)
    if frame.empty:
        return frame
    frame[EVENT_TIMESTAMP_COLUMN] = pd.to_datetime(frame[EVENT_TIMESTAMP_COLUMN], utc=True)
    frame = frame.set_index(EVENT_TIMESTAMP_COLUMN).sort_index()
    return frame


def _compute_technical_features(ohlcv: pd.DataFrame) -> pd.DataFrame:
    if ohlcv.empty:
        return pd.DataFrame()

    features = pd.DataFrame(index=ohlcv.index)
    close = ohlcv["close"]
    high = ohlcv["high"]
    low = ohlcv["low"]
    volume = ohlcv["volume"].replace(0, np.nan)

    for window in (1, 5, 15):
        features[f"momentum_{window}"] = close.pct_change(window)

    prev_close = close.shift(1)
    tr_components = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1)
    true_range = tr_components.max(axis=1)
    features["atr_14"] = true_range.rolling(window=14, min_periods=1).mean()

    log_returns = np.log(close).diff().fillna(0.0)
    window = 30
    features["realized_vol_30"] = log_returns.rolling(window=window, min_periods=5).apply(
        lambda vals: float(np.sqrt(np.sum(vals**2))), raw=True
    )

    rolling_mean = close.rolling(window=20, min_periods=5).mean()
    rolling_std = close.rolling(window=20, min_periods=5).std(ddof=0)
    features["bb_width_20"] = (2 * rolling_std) / rolling_mean

    delta = close.diff()
    gains = delta.clip(lower=0.0)
    losses = -delta.clip(upper=0.0)
    avg_gain = gains.rolling(window=14, min_periods=14).mean()
    avg_loss = losses.rolling(window=14, min_periods=14).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    features["rsi_14"] = 100 - (100 / (1 + rs))

    ema_fast = close.ewm(span=12, adjust=False).mean()
    ema_slow = close.ewm(span=26, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    features["macd"] = macd_line
    features["macd_signal"] = signal_line
    features["macd_hist"] = macd_line - signal_line

    if "vwap" in ohlcv and not ohlcv["vwap"].isna().all():
        vwap = ohlcv["vwap"].copy()
    else:
        typical_price = (high + low + close) / 3
        cumulative_volume = volume.cumsum()
        vwap = (typical_price * volume).cumsum() / cumulative_volume
    features["vwap_divergence"] = close - vwap

    features.replace([-np.inf, np.inf], np.nan, inplace=True)
    return features


def _compute_microstructure_features(
    micro: pd.DataFrame,
    ohlcv: pd.DataFrame,
    tolerance: pd.Timedelta,
) -> pd.DataFrame:
    if micro.empty:
        index = ohlcv.index if not ohlcv.empty else pd.Index([], name=EVENT_TIMESTAMP_COLUMN)
        return pd.DataFrame(
            index=index,
            data={
                "trade_count": np.nan,
                "order_imbalance": np.nan,
                "volume_imbalance": np.nan,
                "avg_trade_size": np.nan,
                "vwap_micro_divergence": np.nan,
            },
        )

    features = micro.copy()
    if "trade_count" not in features:
        features["trade_count"] = np.nan
    buy_volume = features.get("buy_volume", pd.Series(index=features.index, dtype=float))
    sell_volume = features.get("sell_volume", pd.Series(index=features.index, dtype=float))
    features["total_volume"] = buy_volume.add(sell_volume, fill_value=0.0)
    net_volume = buy_volume.sub(sell_volume, fill_value=0.0)
    features["order_imbalance"] = net_volume.divide(features["total_volume"].replace(0.0, np.nan))
    features["volume_imbalance"] = net_volume
    if "avg_trade_size" not in features or features["avg_trade_size"].isna().all():
        features["avg_trade_size"] = features["total_volume"].divide(features["trade_count"].replace(0, np.nan))

    if "trade_vwap" in features:
        price_reference = ohlcv["close"].reindex(features.index, method="nearest") if not ohlcv.empty else np.nan
        features["vwap_micro_divergence"] = price_reference - features["trade_vwap"]
    else:
        features["vwap_micro_divergence"] = np.nan

    projected = features[[
        "trade_count",
        "order_imbalance",
        "volume_imbalance",
        "avg_trade_size",
        "vwap_micro_divergence",
    ]].copy()
    projected.replace([-np.inf, np.inf], np.nan, inplace=True)
    if not ohlcv.empty:
        projected = projected.reindex(ohlcv.index, method="nearest", tolerance=tolerance)
    return projected


def _combine_features(
    symbol: str,
    version: str,
    ohlcv: pd.DataFrame,
    technical: pd.DataFrame,
    micro: pd.DataFrame,
) -> pd.DataFrame:
    if ohlcv.empty or technical.empty:
        LOGGER.warning("Skipping symbol %s due to insufficient OHLCV data", symbol)
        return pd.DataFrame()

    combined = technical.join(micro, how="left")
    combined.reset_index(inplace=True)
    combined[ENTITY_COLUMN] = symbol
    combined[CREATED_AT_COLUMN] = pd.Timestamp.now(tz=timezone.utc)
    combined["feature_version"] = version
    ordered_cols = [EVENT_TIMESTAMP_COLUMN, ENTITY_COLUMN, CREATED_AT_COLUMN, "feature_version"] + [
        col for col in combined.columns
        if col not in {EVENT_TIMESTAMP_COLUMN, ENTITY_COLUMN, CREATED_AT_COLUMN, "feature_version"}
    ]
    combined = combined[ordered_cols]
    return combined


def _write_to_feast(store: FeatureStore | None, feature_view: str, frame: pd.DataFrame) -> None:
    if store is None:
        LOGGER.warning("Feast not installed or configured; skipping write to feature store")
        return
    if frame.empty:
        LOGGER.warning("No features to persist to Feast")
        return
    LOGGER.info("Writing %d feature rows to Feast view %s", len(frame), feature_view)
    store.write_to_online_store(feature_view, frame)


def _ensure_version_table(engine: Engine) -> Table:
    metadata = MetaData()
    table = Table(
        FEATURE_VERSION_TABLE,
        metadata,
        Column("version", String, primary_key=True),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("changelog", String, nullable=False),
    )
    metadata.create_all(engine, tables=[table])
    return table


def _record_version(engine: Engine, table: Table, version: str, changelog: str) -> None:
    LOGGER.info("Recording feature version %s", version)
    created_at = datetime.now(timezone.utc)
    stmt = pg_insert(table).values(version=version, created_at=created_at, changelog=changelog)
    stmt = stmt.on_conflict_do_update(index_elements=[table.c.version], set_={"created_at": created_at, "changelog": changelog})
    with engine.begin() as connection:
        connection.execute(stmt)


def _run_expectations(frame: pd.DataFrame, *, version: str) -> None:
    if ge is None or SimpleCheckpoint is None or RuntimeBatchRequest is None:  # pragma: no cover - optional
        LOGGER.warning("Great Expectations not installed; skipping validation")
        return
    if frame.empty:
        LOGGER.warning("Skipping validation because feature frame is empty")
        return

    context = ge.get_context(context_root_dir=str(DEFAULT_GE_ROOT))
    suite_name = DEFAULT_GE_SUITE
    try:
        context.get_expectation_suite(suite_name)
    except ge.exceptions.DataContextError:
        LOGGER.info("Creating expectation suite %s", suite_name)
        context.create_expectation_suite(suite_name, overwrite_existing=True)
        validator = context.get_validator(
            batch_request=RuntimeBatchRequest(
                datasource_name="timescale_default",
                data_connector_name="default_runtime_data_connector",
                data_asset_name="engineered_features",
                runtime_parameters={"batch_data": frame},
                batch_identifiers={"default_identifier_name": "bootstrap"},
            ),
            expectation_suite_name=suite_name,
        )
        required_columns = [
            EVENT_TIMESTAMP_COLUMN,
            ENTITY_COLUMN,
            CREATED_AT_COLUMN,
            "feature_version",
            "momentum_1",
            "momentum_5",
            "momentum_15",
            "atr_14",
            "realized_vol_30",
            "bb_width_20",
            "rsi_14",
            "macd",
            "macd_signal",
            "macd_hist",
            "vwap_divergence",
            "trade_count",
            "order_imbalance",
            "volume_imbalance",
            "avg_trade_size",
            "vwap_micro_divergence",
        ]
        for column in required_columns:
            validator.expect_column_to_exist(column)
        validator.expect_column_values_to_not_be_null("feature_version")
        validator.expect_column_values_to_be_between("rsi_14", min_value=0, max_value=100, mostly=0.95)
        validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint = SimpleCheckpoint(
        name=DEFAULT_GE_CHECKPOINT,
        data_context=context,
        validations=[
            {
                "batch_request": RuntimeBatchRequest(
                    datasource_name="timescale_default",
                    data_connector_name="default_runtime_data_connector",
                    data_asset_name="engineered_features",
                    runtime_parameters={"batch_data": frame},
                    batch_identifiers={"default_identifier_name": version},
                ),
                "expectation_suite_name": suite_name,
            }
        ],
    )
    result = checkpoint.run()
    if not result.success:
        raise ValueError("Great Expectations validation failed for engineered features")


def materialise_features(config: FeatureBuildConfig) -> pd.DataFrame:
    config.validate()
    engine = _resolve_engine()
    feature_store = (
        FeatureStore(repo_path=str(DEFAULT_FEAST_REPO))
        if FeatureStore is not None and DEFAULT_FEAST_REPO.exists()
        else None
    )

    frames: list[pd.DataFrame] = []
    tolerance = _granularity_to_timedelta(config.granularity)
    for symbol in config.symbols:
        LOGGER.info("Processing %s", symbol)
        ohlcv = _fetch_ohlcv(engine, symbol, config.granularity, start=config.start, end=config.end)
        technical = _compute_technical_features(ohlcv)
        micro = _compute_microstructure(
            engine,
            symbol,
            config.granularity,
            start=config.start,
            end=config.end,
        )
        micro_features = _compute_microstructure_features(micro, ohlcv, tolerance)
        frame = _combine_features(symbol, config.version, ohlcv, technical, micro_features)
        if not frame.empty:
            frames.append(frame)

    if not frames:
        LOGGER.warning("No features computed for requested symbols")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined.sort_values(by=[EVENT_TIMESTAMP_COLUMN, ENTITY_COLUMN], inplace=True)

    _run_expectations(combined, version=config.version)
    _write_to_feast(feature_store, DEFAULT_FEATURE_VIEW, combined)

    version_table = _ensure_version_table(engine)
    _record_version(engine, version_table, config.version, config.changelog)

    return combined


def main(argv: Iterable[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    config = FeatureBuildConfig(
        symbols=_parse_symbols(args.symbols),
        granularity=args.gran,
        version=args.version,
        changelog=args.changelog,
        start=_parse_optional_ts(args.start),
        end=_parse_optional_ts(args.end),
    )

    materialise_features(config)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())

