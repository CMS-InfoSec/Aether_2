"""Regression tests for optional dependency handling in data loaders."""

from __future__ import annotations

import datetime as dt
import importlib
from types import ModuleType

import pytest


def _module() -> ModuleType:
    """Reload the data loader module to ensure a clean slate for patches."""

    return importlib.reload(importlib.import_module("ml.data.loaders"))


def _build_split(module):
    return module.WalkForwardSplit(
        train_start=dt.datetime(2024, 1, 1),
        train_end=dt.datetime(2024, 1, 2),
        test_start=dt.datetime(2024, 1, 3),
        test_end=dt.datetime(2024, 1, 4),
    )


def _build_query(module):
    return module.TimescaleQuery(
        table="features",
        timestamp_col="event_ts",
        entity_col="entity_id",
        feature_columns=("value",),
    )


def test_timescale_loader_requires_pandas(monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "pd", None)
    monkeypatch.setattr(module, "_require_psycopg", lambda: None)

    with pytest.raises(module.MissingDependencyError, match="pandas is required"):
        module.TimescaleFeastDataLoader(
            connection_str="postgresql://localhost/db",
            timescale_query=_build_query(module),
            splits=[_build_split(module)],
        )


def test_timescale_loader_requires_psycopg(monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "pd", object())
    monkeypatch.setattr(module, "psycopg", None)

    with pytest.raises(module.MissingDependencyError, match="psycopg is required"):
        module.TimescaleFeastDataLoader(
            connection_str="postgresql://localhost/db",
            timescale_query=_build_query(module),
            splits=[_build_split(module)],
        )


def test_timescale_loader_requires_feast_for_labels(monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "pd", object())
    monkeypatch.setattr(module, "_require_psycopg", lambda: None)
    monkeypatch.setattr(module, "FeatureStore", None)

    with pytest.raises(module.MissingDependencyError, match="Feast is required"):
        module.TimescaleFeastDataLoader(
            connection_str="postgresql://localhost/db",
            timescale_query=_build_query(module),
            splits=[_build_split(module)],
            label_feature_view=module.FeastFeatureView(
                name="labels", entities=["entity_id"], features=["label"]
            ),
        )
