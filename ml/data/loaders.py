"""Data loading utilities for curated feature and label retrieval.

This module provides abstractions for accessing both TimescaleDB and Feast
feature stores while respecting purged walk-forward validation splits. The
loaders return pandas DataFrames ready for consumption by downstream
modeling code. All loaders emit dataframes with a multi-index of
``[entity_id, event_timestamp]`` to make time-based joins explicit.
"""
from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import pandas as pd

try:  # Optional dependency – the loader still works without Feast installed.
    from feast import FeatureStore
except Exception:  # pragma: no cover - executed only when Feast is unavailable.
    FeatureStore = None  # type: ignore

try:
    import psycopg
except Exception:  # pragma: no cover - executed only when psycopg is unavailable.
    psycopg = None  # type: ignore

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class WalkForwardSplit:
    """Definition of a single walk-forward split.

    The ``purge_window`` removes data immediately preceding the test window
    from the training period to prevent look-ahead bias in overlapping
    samples.
    """

    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime
    purge_window: timedelta = timedelta()

    def purged_train_range(self) -> Tuple[datetime, datetime]:
        """Return the training range with the purge window removed."""

        effective_end = min(self.train_end, self.test_start - self.purge_window)
        if effective_end <= self.train_start:
            raise ValueError(
                "Purge window is too large and removes the entire training range."
            )
        return self.train_start, effective_end


@dataclass(frozen=True)
class TimescaleQuery:
    """Configuration for querying TimescaleDB."""

    table: str
    timestamp_col: str
    entity_col: str
    feature_columns: Tuple[str, ...]

    def build_sql(self, start: datetime, end: datetime) -> str:
        columns = ", ".join([self.entity_col, self.timestamp_col, *self.feature_columns])
        return (
            f"SELECT {columns} FROM {self.table} "
            f"WHERE {self.timestamp_col} >= %(start)s "
            f"AND {self.timestamp_col} < %(end)s"
        )


@dataclass(frozen=True)
class FeastFeatureView:
    """Lightweight wrapper describing a Feast feature view."""

    name: str
    entities: List[str]
    features: List[str]


@dataclass
class DataSlice:
    """Container for a single walk-forward slice."""

    features: pd.DataFrame
    labels: pd.DataFrame
    metadata: Dict[str, datetime]


class BaseWalkForwardDataLoader:
    """Common logic shared across walk-forward data loaders."""

    def __init__(self, splits: Iterable[WalkForwardSplit]):
        self._splits: Tuple[WalkForwardSplit, ...] = tuple(splits)
        if not self._splits:
            raise ValueError("At least one walk-forward split must be provided.")

    def iter_slices(self) -> Iterator[DataSlice]:
        """Yield all configured walk-forward slices."""

        for split in self._splits:
            LOGGER.debug("Processing split: %s", split)
            yield self._build_slice(split)

    def _build_slice(self, split: WalkForwardSplit) -> DataSlice:
        raise NotImplementedError


class TimescaleFeastDataLoader(BaseWalkForwardDataLoader):
    """Loader that combines TimescaleDB features with Feast-derived labels.

    Parameters
    ----------
    connection_str:
        Psycopg connection string for the TimescaleDB instance.
    timescale_query:
        Configuration describing the TimescaleDB table and feature columns.
    feast_repo_path:
        Path to the Feast repository. Only required when Feast integration is
        used. If Feast is unavailable the loader will still yield Timescale-only
        slices.
    label_feature_view:
        Optional Feast feature view used to load labels for each split.
    entity_id_column:
        Name of the entity identifier column in the features dataframe. The
        same column is expected in the label dataframe.
    """

    def __init__(
        self,
        connection_str: str,
        timescale_query: TimescaleQuery,
        splits: Iterable[WalkForwardSplit],
        feast_repo_path: Optional[str] = None,
        label_feature_view: Optional[FeastFeatureView] = None,
        entity_id_column: str = "entity_id",
    ) -> None:
        super().__init__(splits)
        self.connection_str = connection_str
        self.timescale_query = timescale_query
        self.feast_repo_path = feast_repo_path
        self.label_feature_view = label_feature_view
        self.entity_id_column = entity_id_column
        if label_feature_view and FeatureStore is None:
            raise ImportError(
                "Feast is required for loading labels but is not installed."
            )
        if psycopg is None:
            raise ImportError(
                "psycopg is required for TimescaleDB connections but is not installed."
            )

    def _build_slice(self, split: WalkForwardSplit) -> DataSlice:
        train_start, train_end = split.purged_train_range()
        features = self._load_features(train_start, split.test_end)
        labels = self._load_labels(split.test_start, split.test_end)
        metadata = {
            "train_start": train_start,
            "train_end": train_end,
            "test_start": split.test_start,
            "test_end": split.test_end,
        }
        return DataSlice(features=features, labels=labels, metadata=metadata)

    def _load_features(self, start: datetime, end: datetime) -> pd.DataFrame:
        LOGGER.debug("Loading TimescaleDB features between %s and %s", start, end)
        sql = self.timescale_query.build_sql(start, end)
        with psycopg.connect(self.connection_str) as conn:
            df = pd.read_sql(sql, conn, params={"start": start, "end": end})
        df = df.set_index([self.timescale_query.entity_col, self.timescale_query.timestamp_col])
        df = df.sort_index()
        return df

    def _load_labels(self, start: datetime, end: datetime) -> pd.DataFrame:
        if not self.label_feature_view or FeatureStore is None:
            LOGGER.debug("Feast label view not configured. Returning empty labels.")
            return pd.DataFrame(
                [],
                columns=[self.entity_id_column, "event_timestamp", "label"],
            ).set_index([self.entity_id_column, "event_timestamp"])

        LOGGER.debug("Loading Feast labels between %s and %s", start, end)
        store = FeatureStore(repo_path=self.feast_repo_path)
        entity_df = pd.DataFrame(
            {
                self.entity_id_column: self._distinct_entities(),
                "event_timestamp": pd.date_range(start, end, freq="1min"),
            }
        )
        feature_refs = [f"{self.label_feature_view.name}:{feature}" for feature in self.label_feature_view.features]
        feature_data = store.get_historical_features(
            entity_df=entity_df,
            features=feature_refs,
        ).to_df()
        feature_data = feature_data[
            (feature_data["event_timestamp"] >= start)
            & (feature_data["event_timestamp"] < end)
        ]
        feature_data = feature_data.set_index([self.entity_id_column, "event_timestamp"])
        return feature_data

    def _distinct_entities(self) -> List[str]:
        sql = (
            f"SELECT DISTINCT {self.timescale_query.entity_col} "
            f"FROM {self.timescale_query.table}"
        )
        with psycopg.connect(self.connection_str) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return [row[0] for row in rows]


class InMemoryDataLoader(BaseWalkForwardDataLoader):
    """Utility loader for testing and local experimentation."""

    def __init__(
        self,
        features: pd.DataFrame,
        labels: pd.DataFrame,
        splits: Iterable[WalkForwardSplit],
        entity_id_column: str = "entity_id",
        timestamp_column: str = "event_timestamp",
    ) -> None:
        super().__init__(splits)
        self.features = features.set_index([entity_id_column, timestamp_column])
        self.labels = labels.set_index([entity_id_column, timestamp_column])

    def _build_slice(self, split: WalkForwardSplit) -> DataSlice:
        train_start, train_end = split.purged_train_range()
        train_mask = (self.features.index.get_level_values(1) >= train_start) & (
            self.features.index.get_level_values(1) < train_end
        )
        test_mask = (self.features.index.get_level_values(1) >= split.test_start) & (
            self.features.index.get_level_values(1) < split.test_end
        )
        feature_slice = pd.concat(
            [self.features[train_mask], self.features[test_mask]],
            axis=0,
        )
        label_slice = self.labels[test_mask]
        metadata = {
            "train_start": train_start,
            "train_end": train_end,
            "test_start": split.test_start,
            "test_end": split.test_end,
        }
        return DataSlice(features=feature_slice.sort_index(), labels=label_slice.sort_index(), metadata=metadata)


@contextlib.contextmanager
def temporary_timescale_loader(
    connection_str: str,
    timescale_query: TimescaleQuery,
    splits: Iterable[WalkForwardSplit],
    feast_repo_path: Optional[str] = None,
    label_feature_view: Optional[FeastFeatureView] = None,
    entity_id_column: str = "entity_id",
) -> Iterator[TimescaleFeastDataLoader]:
    """Context manager that yields a configured ``TimescaleFeastDataLoader``.

    This helper simplifies usage in scripts where a loader is required for a
    short period of time. All resources are cleaned up automatically.
    """

    loader = TimescaleFeastDataLoader(
        connection_str=connection_str,
        timescale_query=timescale_query,
        splits=splits,
        feast_repo_path=feast_repo_path,
        label_feature_view=label_feature_view,
        entity_id_column=entity_id_column,
    )
    try:
        yield loader
    finally:
        LOGGER.debug("TimescaleFeastDataLoader context closed.")
