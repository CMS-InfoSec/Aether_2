"""Tests for the ML training workflow utilities."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

pd = pytest.importorskip("pandas")

from ml.training.workflow import (
    TrainingJobConfig,
    TrainingMetadata,
    TrainingConfig,
    TimescaleSourceConfig,
    ModelConfig,
    MetricThresholds,
    ObjectStorageConfig,
    MLflowConfig,
    ChronologicalSplitConfig,
    OutlierConfig,
    _build_registration_tags,
    _compute_config_hash,
    _prepare_supervised_frame,
    _chronological_split,
    _apply_outlier_handling,
    _TARGET_COLUMN,
)


def _build_config() -> TrainingJobConfig:
    return TrainingJobConfig(
        timescale=TimescaleSourceConfig(
            uri="postgresql://localhost",  # pragma: allowlist secret
            table="market_data",
            entity_column="symbol",
            timestamp_column="as_of",
            label_column="label",
            feature_columns=["f1", "f2"],
        ),
        model=ModelConfig(),
        training=TrainingConfig(),
        thresholds=MetricThresholds(),
        artifacts=ObjectStorageConfig(),
        mlflow=MLflowConfig(
            tracking_uri="http://mlflow.test",
            experiment_name="test-exp",
        ),
        metadata=TrainingMetadata(
            feature_version="fv1",
            label_horizon="15m",
            granularity="1m",
            symbols=["ETH-USD", "BTC-USD", "ETH-USD"],
        ),
    )


def _sample_frame() -> pd.DataFrame:
    timestamps = [
        datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
    ]
    return pd.DataFrame(
        {
            "symbol": ["BTC-USD", "ETH-USD"],
            "as_of": timestamps,
            "label": [0.1, -0.2],
            "f1": [1.0, 2.0],
            "f2": [0.3, 0.4],
        }
    )


def _chronological_frame() -> pd.DataFrame:
    timestamps = pd.date_range(
        start=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        periods=10,
        freq="1min",
    )
    prices = [100, 101, 102, 90, 95, 120, 118, 119, 121, 150]
    return pd.DataFrame(
        {
            "symbol": ["BTC-USD"] * 10,
            "as_of": timestamps,
            "label": prices,
            "f1": range(10),
            "f2": [val * 0.1 for val in range(10)],
        }
    )


def test_build_registration_tags_includes_expected_metadata() -> None:
    config = _build_config()
    frame = _sample_frame()

    commit = "deadbeef"
    tags = _build_registration_tags(config, frame, git_commit=commit)

    assert tags["feature_version"] == "fv1"
    assert tags["label_horizon"] == "15m"
    assert tags["granularity"] == "1m"
    assert tags["symbols"] == "BTC-USD, ETH-USD"
    assert tags["data_from"].startswith("2024-01-01T00:00:00")
    assert tags["data_to"].startswith("2024-01-01T00:01:00")
    assert tags["git_commit"] == commit


def test_config_hash_matches_helper_output() -> None:
    config = _build_config()
    expected_hash = _compute_config_hash(config)
    frame = _sample_frame()

    tags = _build_registration_tags(config, frame, git_commit="cafebabe")

    assert tags["config_hash"] == expected_hash


def test_prepare_supervised_frame_creates_forward_labels_without_overlap() -> None:
    frame = _chronological_frame()
    config = TimescaleSourceConfig(
        uri="postgresql://localhost",  # pragma: allowlist secret
        table="market_data",
        entity_column="symbol",
        timestamp_column="as_of",
        label_column="label",
        feature_columns=["f1", "f2"],
        label_horizon=2,
    )

    prepared = _prepare_supervised_frame(frame, config)

    assert len(prepared) == len(frame) - 2
    expected_first = (frame.loc[2, "label"] - frame.loc[0, "label"]) / frame.loc[0, "label"]
    assert prepared.iloc[0][_TARGET_COLUMN] == pytest.approx(expected_first)
    last_timestamp = prepared["as_of"].max()
    assert last_timestamp == frame.iloc[-3]["as_of"]


def test_chronological_split_respects_timestamp_boundaries() -> None:
    frame = _chronological_frame()
    config = TimescaleSourceConfig(
        uri="postgresql://localhost",  # pragma: allowlist secret
        table="market_data",
        entity_column="symbol",
        timestamp_column="as_of",
        label_column="label",
        feature_columns=["f1", "f2"],
    )
    prepared = _prepare_supervised_frame(frame, config)
    split = ChronologicalSplitConfig(train_fraction=0.5, validation_fraction=0.3, test_fraction=0.2)

    train_frame, val_frame, test_frame = _chronological_split(prepared, "as_of", split)

    assert not train_frame.empty and not val_frame.empty and not test_frame.empty
    assert train_frame["as_of"].max() < val_frame["as_of"].min()
    assert val_frame["as_of"].max() < test_frame["as_of"].min()
    assert len(train_frame) + len(val_frame) + len(test_frame) == len(prepared)


def test_outlier_handling_clip_and_drop() -> None:
    frame = _chronological_frame()
    config = TimescaleSourceConfig(
        uri="postgresql://localhost",  # pragma: allowlist secret
        table="market_data",
        entity_column="symbol",
        timestamp_column="as_of",
        label_column="label",
        feature_columns=["f1", "f2"],
    )
    prepared = _prepare_supervised_frame(frame, config)

    clip_config = OutlierConfig(method="clip", lower_quantile=0.1, upper_quantile=0.9)
    clipped = _apply_outlier_handling(prepared, _TARGET_COLUMN, clip_config)
    lower = prepared[_TARGET_COLUMN].quantile(0.1)
    upper = prepared[_TARGET_COLUMN].quantile(0.9)
    assert clipped[_TARGET_COLUMN].max() <= upper + 1e-9
    assert clipped[_TARGET_COLUMN].min() >= lower - 1e-9

    drop_config = OutlierConfig(method="drop", lower_quantile=0.1, upper_quantile=0.9)
    dropped = _apply_outlier_handling(prepared, _TARGET_COLUMN, drop_config)
    assert len(dropped) < len(prepared)
    assert dropped[_TARGET_COLUMN].between(lower, upper).all()
