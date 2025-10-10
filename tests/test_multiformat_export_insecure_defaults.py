import datetime as dt
import json
from pathlib import Path

import pytest

import multiformat_export


@pytest.fixture(autouse=True)
def _patch_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(multiformat_export, "psycopg", None)
    monkeypatch.setattr(multiformat_export, "dict_row", None)
    monkeypatch.setattr(multiformat_export, "boto3", None)
    monkeypatch.setattr(multiformat_export, "markdown2", None)
    monkeypatch.setattr(multiformat_export, "SimpleDocTemplate", None)
    monkeypatch.setattr(multiformat_export, "Paragraph", None)
    monkeypatch.setattr(multiformat_export, "Spacer", None)
    monkeypatch.setattr(multiformat_export, "Table", None)
    monkeypatch.setattr(multiformat_export, "TableStyle", None)
    monkeypatch.setattr(multiformat_export, "colors", None)
    monkeypatch.setattr(multiformat_export, "LETTER", None)
    monkeypatch.setattr(multiformat_export, "getSampleStyleSheet", None)


def _write_snapshot(tmp_path: Path, kind: str, for_date: dt.date, payload: list[dict]) -> None:
    snapshot_dir = tmp_path / "log_export" / "snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    path = snapshot_dir / f"{kind}-{for_date.isoformat()}.json"
    path.write_text(json.dumps(payload))


def test_export_persists_artifacts_locally(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MULTIFORMAT_EXPORT_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("AETHER_STATE_DIR", str(tmp_path))
    monkeypatch.delenv("MULTIFORMAT_EXPORT_BUCKET", raising=False)
    monkeypatch.delenv("EXPORT_BUCKET", raising=False)

    for_date = dt.date(2024, 1, 2)
    _write_snapshot(
        tmp_path,
        "audit",
        for_date,
        [
            {
                "id": "audit-1",
                "actor": "system",
                "action": "fallback",
                "entity": "logs",
                "before": {},
                "after": {},
                "ts": "2024-01-02T00:00:00+00:00",
            }
        ],
    )

    exporter = multiformat_export.LogExporter(
        config=multiformat_export.StorageConfig(bucket="local", prefix="local"),
        dsn=None,
    )

    result = exporter.export(for_date=for_date)

    assert set(result.artifacts.keys()) == {"json", "csv", "md", "pdf"}
    for artifact in result.artifacts.values():
        assert artifact.data

    metadata_path = tmp_path / "log_export" / "multiformat_metadata.json"
    assert metadata_path.exists()
    metadata = json.loads(metadata_path.read_text())
    assert metadata[-1]["run_id"] == result.run_id

    artifact_dir = tmp_path / "log_export" / "multiformat" / for_date.isoformat() / result.run_id
    assert artifact_dir.exists()
    saved_formats = {path.suffix.lstrip(".") for path in artifact_dir.iterdir()}
    assert saved_formats == {"json", "csv", "md", "pdf"}
