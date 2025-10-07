"""Tests for the ML training workflow utilities."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

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
    _write_artifacts,
    _normalise_s3_prefix,
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


def test_write_artifacts_normalises_relative_base_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    config = ObjectStorageConfig(base_path="artifacts/output")

    result = _write_artifacts({"metrics.json": b"{}"}, config)

    expected_path = tmp_path / "artifacts" / "output" / "metrics.json"
    assert Path(result["metrics.json"]) == expected_path
    assert expected_path.exists()
    assert Path(config.base_path) == tmp_path / "artifacts" / "output"


def test_write_artifacts_rejects_parent_reference_in_base_path() -> None:
    with pytest.raises(ValueError, match="parent directory"):
        ObjectStorageConfig(base_path="../escape")


def test_write_artifacts_rejects_symlink_base_path(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink"):
        ObjectStorageConfig(base_path=str(link))


def test_write_artifacts_allows_symlink_parent(tmp_path: Path) -> None:
    real_root = tmp_path / "real"
    real_root.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real_root, target_is_directory=True)

    config = ObjectStorageConfig(base_path=str(link / "output"))

    result = _write_artifacts({"metrics.json": b"{}"}, config)

    expected_root = (real_root / "output").resolve()
    expected_path = expected_root / "metrics.json"
    assert Path(config.base_path) == expected_root
    assert Path(result["metrics.json"]) == expected_path
    assert expected_path.exists()


def test_write_artifacts_s3_prefix_normalised(monkeypatch: pytest.MonkeyPatch) -> None:
    class _StubClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, bytes]] = []

        def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:  # noqa: N803 - boto3 signature
            self.calls.append((Bucket, Key, Body))

    stub_client = _StubClient()

    class _StubBoto3:
        def client(self, name: str) -> _StubClient:
            assert name == "s3"
            return stub_client

    monkeypatch.setattr("ml.training.workflow.boto3", _StubBoto3())

    config = ObjectStorageConfig(s3_bucket="aether", s3_prefix=" /unsafe//prefix/ ")

    result = _write_artifacts({"metrics.json": b"{}"}, config)

    assert stub_client.calls == [("aether", "unsafe/prefix/metrics.json", b"{}")]  # type: ignore[arg-type]
    assert result["metrics.json"] == "s3://aether/unsafe/prefix/metrics.json"
    assert config.s3_prefix == "unsafe/prefix"


def test_write_artifacts_rejects_traversal_in_s3_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    class _StubBoto3:
        def client(self, name: str) -> None:  # pragma: no cover - should not be called
            raise AssertionError("client should not be invoked when prefix is invalid")

    monkeypatch.setattr("ml.training.workflow.boto3", _StubBoto3())

    with pytest.raises(ValueError, match="prefix"):
        ObjectStorageConfig(s3_bucket="aether", s3_prefix="../escape")


def test_object_storage_config_normalises_base_and_prefix(tmp_path: Path) -> None:
    base = tmp_path / "artifacts"
    prefix = " /reports///daily/ "

    config = ObjectStorageConfig(base_path=str(base), s3_bucket="bucket", s3_prefix=prefix)

    assert Path(config.base_path) == base
    assert config.s3_prefix == "reports/daily"


def test_write_artifacts_rejects_traversal_in_artifact_name(tmp_path: Path) -> None:
    base = tmp_path / "artifacts"
    config = ObjectStorageConfig(base_path=str(base))

    with pytest.raises(ValueError, match="parent directory"):
        _write_artifacts({"../escape": b"{}"}, config)

    with pytest.raises(ValueError, match="parent directory"):
        _write_artifacts({"..\\escape": b"{}"}, config)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("", ""),
        ("  prefix/child  ", "prefix/child"),
        ("prefix//child", "prefix/child"),
        (None, ""),
    ],
)
def test_normalise_s3_prefix_valid_inputs(raw: str | None, expected: str) -> None:
    assert _normalise_s3_prefix(raw) == expected


@pytest.mark.parametrize("raw", ["../escape", "prefix/../escape", "bad\nprefix"])
def test_normalise_s3_prefix_rejects_invalid_inputs(raw: str) -> None:
    with pytest.raises(ValueError):
        _normalise_s3_prefix(raw)
