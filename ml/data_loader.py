"""Data loading utilities for walk-forward modeling workflows.

This module provides utilities to load feature and label data from
TimescaleDB and Feast, assemble purged walk-forward datasets, and
compute fee-adjusted labels that reflect transaction costs.
"""
from __future__ import annotations

import importlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass
class TimescaleFeastConfig:
    """Configuration for retrieving features and labels.

    Attributes
    ----------
    timescale_uri:
        SQLAlchemy-compatible URI for the TimescaleDB instance.
    base_query:
        SQL query returning the base time-series data. The query must include
        the columns defined by ``time_column`` and ``entity_column``.
    time_column:
        Name of the timestamp column used for ordering observations.
    entity_column:
        Name of the column that identifies the entity (e.g., symbol).
    label_column:
        Column that contains the raw target before fee adjustments.
    transaction_fee_bps:
        Per-trade transaction fee expressed in basis points.
    feature_view:
        Feast feature view to materialise.
    feature_service:
        Optional Feast feature service name for group retrieval.
    feature_store_repo:
        Path to the Feast repository configuration.
    purge_gap:
        Number of periods to purge between training and validation splits to
        avoid information leakage.
    """

    timescale_uri: str
    base_query: str
    time_column: str
    entity_column: str
    label_column: str
    transaction_fee_bps: float
    feature_view: str
    feature_service: Optional[str] = None
    feature_store_repo: Optional[str] = None
    purge_gap: int = 0


@dataclass
class WalkForwardSplit:
    """Container for a single purged walk-forward split."""

    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame
    metadata: Dict[str, datetime]


def _create_engine(uri: str) -> Engine:
    """Create a SQLAlchemy engine for TimescaleDB interactions."""

    return create_engine(uri, pool_pre_ping=True, pool_recycle=3600)


def _load_base_frame(engine: Engine, query: str, time_column: str) -> pd.DataFrame:
    """Load the base time-series frame from TimescaleDB."""

    frame = pd.read_sql(query, engine)
    frame[time_column] = pd.to_datetime(frame[time_column], utc=True)
    return frame.sort_values(time_column).reset_index(drop=True)


def _load_feast_features(
    feature_view: str,
    entities: pd.DataFrame,
    feature_service: Optional[str] = None,
    feature_store_repo: Optional[str] = None,
) -> pd.DataFrame:
    """Load feature vectors from Feast if the dependency is available."""

    feast_spec = importlib.util.find_spec("feast")
    if feast_spec is None:
        raise ModuleNotFoundError(
            "Feast is required to fetch online features. Install feast to "
            "enable feature retrieval."
        )

    feast_module = importlib.import_module("feast")
    feature_store_cls = getattr(feast_module, "FeatureStore")
    store = feature_store_cls(repo_path=feature_store_repo)

    if feature_service:
        features = store.get_historical_features(
            entity_df=entities, feature_service=feature_service
        )
    else:
        features = store.get_historical_features(
            entity_df=entities, features=[feature_view]
        )
    return features.to_df()


def compute_fee_adjusted_label(
    label: float,
    turnover: float,
    transaction_fee_bps: float,
) -> float:
    """Compute a fee-adjusted label.

    Parameters
    ----------
    label:
        Raw label, such as forward return.
    turnover:
        Portfolio turnover used to approximate trading activity.
    transaction_fee_bps:
        Transaction fee in basis points.
    """

    fee = turnover * transaction_fee_bps / 10_000.0
    return label - fee


def assemble_walk_forward_splits(
    frame: pd.DataFrame,
    time_column: str,
    label_column: str,
    transaction_fee_bps: float,
    turnover_column: str,
    train_window: timedelta,
    validation_window: timedelta,
    test_window: timedelta,
    purge_gap: int = 0,
) -> List[WalkForwardSplit]:
    """Create purged walk-forward splits from a base frame."""

    frame = frame.copy()
    frame[label_column] = compute_fee_adjusted_labels(
        frame[label_column], frame[turnover_column], transaction_fee_bps
    )

    splits: List[WalkForwardSplit] = []
    min_time = frame[time_column].min()
    max_time = frame[time_column].max()

    start = min_time
    while start + train_window + validation_window + test_window <= max_time:
        train_end = start + train_window
        val_end = train_end + validation_window
        test_end = val_end + test_window

        train_mask = (frame[time_column] >= start) & (frame[time_column] < train_end)
        val_mask = (frame[time_column] >= train_end) & (frame[time_column] < val_end)
        test_mask = (frame[time_column] >= val_end) & (frame[time_column] < test_end)

        if purge_gap:
            gap_delta = validation_window / max(purge_gap, 1)
            purge_start = val_end - gap_delta
            train_mask &= frame[time_column] < purge_start

        splits.append(
            WalkForwardSplit(
                train=frame.loc[train_mask].copy(),
                validation=frame.loc[val_mask].copy(),
                test=frame.loc[test_mask].copy(),
                metadata={
                    "train_start": start,
                    "train_end": train_end,
                    "validation_end": val_end,
                    "test_end": test_end,
                },
            )
        )
        start = start + test_window

    return splits


def compute_fee_adjusted_labels(
    labels: Sequence[float],
    turnovers: Sequence[float],
    transaction_fee_bps: float,
) -> np.ndarray:
    """Vectorised helper for ``compute_fee_adjusted_label``."""

    labels_array = np.asarray(labels, dtype=float)
    turnover_array = np.asarray(turnovers, dtype=float)
    fees = turnover_array * transaction_fee_bps / 10_000.0
    return labels_array - fees


class PurgedWalkForwardDataLoader:
    """High-level orchestrator for constructing training datasets."""

    def __init__(
        self,
        config: TimescaleFeastConfig,
        turnover_column: str,
        train_window: timedelta,
        validation_window: timedelta,
        test_window: timedelta,
    ) -> None:
        self.config = config
        self.turnover_column = turnover_column
        self.train_window = train_window
        self.validation_window = validation_window
        self.test_window = test_window

    def load(self) -> List[WalkForwardSplit]:
        """Load splits by combining TimescaleDB and Feast data."""

        engine = _create_engine(self.config.timescale_uri)
        base_frame = _load_base_frame(
            engine=engine,
            query=self.config.base_query,
            time_column=self.config.time_column,
        )

        entities = base_frame[[self.config.time_column, self.config.entity_column]].copy()
        features = _load_feast_features(
            feature_view=self.config.feature_view,
            entities=entities,
            feature_service=self.config.feature_service,
            feature_store_repo=self.config.feature_store_repo,
        )
        merged = base_frame.merge(
            features,
            on=[self.config.time_column, self.config.entity_column],
            how="left",
        )

        return assemble_walk_forward_splits(
            frame=merged,
            time_column=self.config.time_column,
            label_column=self.config.label_column,
            transaction_fee_bps=self.config.transaction_fee_bps,
            turnover_column=self.turnover_column,
            train_window=self.train_window,
            validation_window=self.validation_window,
            test_window=self.test_window,
            purge_gap=self.config.purge_gap,
        )


__all__ = [
    "TimescaleFeastConfig",
    "WalkForwardSplit",
    "PurgedWalkForwardDataLoader",
    "assemble_walk_forward_splits",
    "compute_fee_adjusted_label",
    "compute_fee_adjusted_labels",
    "main",
]



def _resolve_output_path(raw_path: str) -> Path:
    """Normalise and validate the walk-forward output path."""

    if any(ord(char) < 32 for char in raw_path):
        raise ValueError("OUTPUT_PATH contains control characters")

    candidate = Path(raw_path).expanduser()

    if any(part == ".." for part in candidate.parts):
        raise ValueError("OUTPUT_PATH must not contain parent directory traversal")

    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate

    # Avoid following symlinks while ensuring a canonical absolute path.
    candidate = candidate.absolute()

    for ancestor in (candidate, *candidate.parents):
        if ancestor.exists() and ancestor.is_symlink():
            raise ValueError("OUTPUT_PATH must not include symlinked paths")

    if candidate.exists() and candidate.is_dir():
        raise ValueError("OUTPUT_PATH must reference a file, not a directory")

    return candidate


def main() -> None:
    """CLI entry-point for the data loader."""

    import json
    import os

    config_payload = os.environ.get("DATA_LOADER_CONFIG")
    if not config_payload:
        raise RuntimeError("DATA_LOADER_CONFIG environment variable is required.")
    config_dict = json.loads(config_payload)
    config = TimescaleFeastConfig(**config_dict)

    turnover_column = os.environ.get("TURNOVER_COLUMN", "turnover")
    train_window = pd.to_timedelta(os.environ.get("TRAIN_WINDOW", "90D"))
    validation_window = pd.to_timedelta(os.environ.get("VALIDATION_WINDOW", "30D"))
    test_window = pd.to_timedelta(os.environ.get("TEST_WINDOW", "30D"))

    loader = PurgedWalkForwardDataLoader(
        config=config,
        turnover_column=turnover_column,
        train_window=train_window,
        validation_window=validation_window,
        test_window=test_window,
    )
    splits = loader.load()
    output_path = _resolve_output_path(
        os.environ.get("OUTPUT_PATH", "/tmp/walk_forward_splits.parquet")
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        str(idx): {
            "train": split.train.to_dict(orient="records"),
            "validation": split.validation.to_dict(orient="records"),
            "test": split.test.to_dict(orient="records"),
            "metadata": {k: v.isoformat() for k, v in split.metadata.items()},
        }
        for idx, split in enumerate(splits)
    }
    output_path.write_text(json.dumps(payload))


if __name__ == "__main__":
    main()
