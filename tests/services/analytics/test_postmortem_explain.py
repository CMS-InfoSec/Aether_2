"""Tests for the postmortem explain analytics service."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from fastapi import HTTPException

from services.analytics import postmortem_explain as explain


def _persist(payload: dict[str, object], html: str, *, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "artifacts"
    monkeypatch.setattr(explain, "ARTIFACT_ROOT", root)
    return root, explain._persist_artifacts(payload, html)


def test_persist_artifacts_stores_payload_securely(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"trade_id": "abc-123", "value": 42}
    html_report = "<html><body>report</body></html>"

    root, info = _persist(payload, html_report, tmp_path=tmp_path, monkeypatch=monkeypatch)

    json_path = Path(info["json"]).resolve()
    html_path = Path(info["html"]).resolve()

    assert json_path.exists()
    assert html_path.exists()
    assert root.resolve() in json_path.parents
    assert root.resolve() in html_path.parents

    with json_path.open("r", encoding="utf-8") as handle:
        stored = json.load(handle)
    assert stored["trade_id"] == "abc-123"

    assert html_path.read_text(encoding="utf-8") == html_report


def test_persist_artifacts_rejects_symlink_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real_root = tmp_path / "real"
    real_root.mkdir()
    link_root = tmp_path / "link"
    link_root.symlink_to(real_root, target_is_directory=True)
    monkeypatch.setattr(explain, "ARTIFACT_ROOT", link_root)

    with pytest.raises(HTTPException) as excinfo:
        explain._persist_artifacts({"foo": "bar"}, "<html></html>")

    assert excinfo.value.status_code == 500


def test_persist_artifacts_rejects_symlink_shard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    monkeypatch.setattr(explain, "ARTIFACT_ROOT", root)

    payload = {"foo": "bar"}
    serialized = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    digest = hashlib.sha256(serialized).hexdigest()

    first = digest[:2]
    second = digest[2:4]

    shard_parent = root / first
    shard_parent.mkdir(parents=True)

    escape = tmp_path / "escape"
    escape.mkdir()
    (shard_parent / second).symlink_to(escape, target_is_directory=True)

    with pytest.raises(HTTPException) as excinfo:
        explain._persist_artifacts(payload, "<html></html>")

    assert excinfo.value.status_code == 500
