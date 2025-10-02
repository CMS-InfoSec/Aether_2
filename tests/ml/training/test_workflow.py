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
    _build_registration_tags,
    _compute_config_hash,
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
