from __future__ import annotations

import csv
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import pytest

from reports.storage import ArtifactStorage
from reports.weekly_xai import fetch_shap_values, generate_weekly_xai


class FakeResult:
    def __init__(self, rows: Iterable[Mapping[str, Any]]):
        self._rows = list(rows)

    def mappings(self) -> List[Mapping[str, Any]]:
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


class RecordingSession:
    def __init__(self, shap_rows: List[Dict[str, Any]]):
        self._rows = shap_rows
        self.queries: List[str] = []
        self.audit_entries: List[Mapping[str, Any]] = []
        self.commits = 0

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> FakeResult:
        params = params or {}
        self.queries.append(query.strip())
        if "INSERT INTO audit_log" in query:
            self.audit_entries.append(params)
            return FakeResult([])
        if "FROM ml_shap_outputs" in query:
            start = params["start"]
            end = params["end"]
            account_ids = params.get("account_ids")
            rows = [
                row
                for row in self._rows
                if start <= row["inference_time"] < end
                and (not account_ids or row["account_id"] in account_ids)
            ]
            return FakeResult(rows)
        raise AssertionError(f"Unexpected query: {query}")

    def commit(self) -> None:
        self.commits += 1


@pytest.fixture
def shap_rows() -> List[Dict[str, Any]]:
    base_time = datetime(2024, 5, 1, 12, 0, tzinfo=timezone.utc)
    return [
        {
            "account_id": 1,
            "feature_name": "volatility_5m",
            "shap_value": 0.5,
            "inference_time": base_time,
            "model_version": "v1",
            "metadata": {"instrument": "BTC-USD"},
        },
        {
            "account_id": 1,
            "feature_name": "volatility_5m",
            "shap_value": -0.2,
            "inference_time": base_time + timedelta(days=1),
            "model_version": "v1",
            "metadata": {"instrument": "BTC-USD"},
        },
        {
            "account_id": 2,
            "feature_name": "momentum",
            "shap_value": 0.3,
            "inference_time": base_time,
            "model_version": "v2",
            "metadata": {"instrument": "ETH-USD"},
        },
    ]


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="") as handle:
        return list(csv.DictReader(handle))


def test_fetch_shap_values_validates_schema(shap_rows: List[Dict[str, Any]]) -> None:
    broken_rows = [
        {key: value for key, value in shap_rows[0].items() if key != "metadata"}
    ]
    session = RecordingSession(broken_rows)
    start = datetime(2024, 4, 29, tzinfo=timezone.utc)
    end = datetime(2024, 5, 6, tzinfo=timezone.utc)
    with pytest.raises(ValueError) as exc:
        fetch_shap_values(session, start=start, end=end)
    assert "metadata" in str(exc.value)


def test_generate_weekly_xai_summarizes_shap(tmp_path: Path, shap_rows: List[Dict[str, Any]]) -> None:
    session = RecordingSession(shap_rows)
    storage = ArtifactStorage(tmp_path)
    keys = generate_weekly_xai(
        session,
        storage,
        week_ending=date(2024, 5, 4),
        output_formats=("csv",),
    )
    assert keys == ["weekly_xai/2024-05-04.csv"]
    csv_path = tmp_path / "global" / keys[0]
    rows = read_csv(csv_path)
    assert {row["account_id"] for row in rows} == {"1", "2"}
    alpha_row = next(row for row in rows if row["feature_name"] == "volatility_5m")
    assert pytest.approx(float(alpha_row["mean_shap"])) == 0.15
    assert pytest.approx(float(alpha_row["mean_abs_shap"])) == 0.35
    assert alpha_row["sample_count"] == "2"
    # ensure audit log entry written with schema metadata present
    assert session.audit_entries
