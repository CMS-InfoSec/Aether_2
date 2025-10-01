from __future__ import annotations

import csv

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, List, Mapping

import pytest

from reports.weekly_xai import (
    SHAP_QUERY,
    fetch_shap_values,
    generate_weekly_xai,
    summarize_weekly_shap,
)
from reports.storage import ArtifactStorage



class FakeResult:
    def __init__(self, rows: Iterable[Mapping[str, Any]]):
        self._rows = list(rows)

    def mappings(self) -> List[Mapping[str, Any]]:
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)



class ShapRecordingSession:
    def __init__(self, shap_rows: Iterable[Mapping[str, Any]]):
        self._rows = list(shap_rows)
        self.audit_entries: List[Mapping[str, Any]] = []
        self.queries: List[str] = []

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = " ".join(query.lower().split())
        self.queries.append(query.strip())

        if "insert into audit_log" in normalized:
            self.audit_entries.append(params)
            return FakeResult([])

        assert "from ml_shap_outputs" in normalized
        assert "inference_time" in normalized

        start = params.get("start")
        end = params.get("end")
        account_filter = params.get("account_ids")

        def include(row: Mapping[str, Any]) -> bool:
            ts = row["inference_time"]
            if start and ts < start:
                return False
            if end and ts >= end:
                return False
            if account_filter and row["account_id"] not in account_filter:
                return False
            return True

        filtered = [row for row in self._rows if include(row)]
        return FakeResult(filtered)


@pytest.fixture
def shap_rows() -> List[Mapping[str, Any]]:
    base = datetime(2024, 4, 29, tzinfo=timezone.utc)
    return [
        {
            "account_id": "alpha",
            "feature_name": "funding_rate",
            "shap_value": 0.25,
            "inference_time": base + timedelta(days=offset),
            "model_version": "v1",
            "metadata": {"symbol": "BTC-USD"},
        }
        for offset in range(3)
    ] + [
        {
            "account_id": "beta",
            "feature_name": "momentum",
            "shap_value": value,
            "inference_time": base + timedelta(days=offset),
            "model_version": "v1",
            "metadata": {"symbol": "ETH-USD"},
        }
        for offset, value in enumerate([-0.5, 0.1, 0.4])
    ]


def test_fetch_shap_values_filters_time_and_accounts(shap_rows: List[Mapping[str, Any]]) -> None:
    session = ShapRecordingSession(shap_rows)
    start = datetime(2024, 4, 30, tzinfo=timezone.utc)
    end = datetime(2024, 5, 7, tzinfo=timezone.utc)

    rows = fetch_shap_values(session, start=start, end=end, account_ids=("beta",))

    assert len(rows) == 2
    assert all(row["account_id"] == "beta" for row in rows)
    assert "account_id = any(%(account_ids)s)" in " ".join(session.queries[0].lower().split())


def test_summarize_weekly_shap_computes_statistics(shap_rows: List[Mapping[str, Any]]) -> None:
    beta_rows = [row for row in shap_rows if row["account_id"] == "beta"]
    summary = summarize_weekly_shap(beta_rows)

    assert len(summary) == 1
    entry = summary[0]
    assert entry.account_id == "beta"
    assert entry.feature_name == "momentum"
    assert pytest.approx(entry.mean_shap) == pytest.approx(sum(row["shap_value"] for row in beta_rows) / len(beta_rows))
    assert pytest.approx(entry.mean_abs_shap) == pytest.approx(sum(abs(row["shap_value"]) for row in beta_rows) / len(beta_rows))
    assert entry.sample_count == len(beta_rows)


def test_generate_weekly_xai_outputs_csv_and_audit_log(
    tmp_path: Path, shap_rows: List[Mapping[str, Any]]
) -> None:
    session = ShapRecordingSession(shap_rows)
    storage = ArtifactStorage(tmp_path)

    keys = generate_weekly_xai(
        session,
        storage,
        week_ending=date(2024, 5, 5),
        output_formats=("csv",),
    )

    assert keys == ["weekly_xai/2024-05-05.csv"]

    csv_path = tmp_path / "global" / keys[0]
    with csv_path.open("r", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert {row["account_id"] for row in rows} == {"alpha", "beta"}
    beta_row = next(row for row in rows if row["account_id"] == "beta")
    assert beta_row["feature_name"] == "momentum"
    assert beta_row["sample_count"] == "3"

    assert session.audit_entries, "Expected audit log entries to be recorded"
    sha_path = tmp_path / "global" / f"{keys[0]}.sha256"
    assert sha_path.exists()

    payload = json.loads(session.audit_entries[0]["metadata"])
    assert payload["row_count"] == len(rows)
    assert payload["storage_backend"] == "filesystem"


def test_generate_weekly_xai_scopes_account_specific_reports(
    tmp_path: Path, shap_rows: List[Mapping[str, Any]]
) -> None:
    session = ShapRecordingSession(shap_rows)
    storage = ArtifactStorage(tmp_path)

    keys = generate_weekly_xai(
        session,
        storage,
        week_ending=date(2024, 5, 5),
        account_ids=("beta",),
        output_formats=("csv",),
    )

    assert keys == ["weekly_xai/2024-05-05.csv"]

    csv_path = tmp_path / "multi-account" / keys[0]
    with csv_path.open("r", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert {row["account_id"] for row in rows} == {"beta"}

    normalized_query = " ".join(session.queries[0].lower().split())
    assert "account_id = any(%(account_ids)s)" in normalized_query
    assert "where inference_time" in normalized_query
    assert "from ml_shap_outputs" in normalized_query


def test_shap_query_targets_expected_table() -> None:
    normalized = " ".join(SHAP_QUERY.lower().split())
    assert "from ml_shap_outputs" in normalized
    assert "where inference_time" in normalized

