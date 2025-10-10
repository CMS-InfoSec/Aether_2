from __future__ import annotations

import json
from pathlib import Path

import postmortem


def test_postmortem_insecure_defaults_creates_artifacts(tmp_path, monkeypatch):
    monkeypatch.setenv("POSTMORTEM_ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("AETHER_STATE_DIR", str(tmp_path / "state"))
    output_dir = tmp_path / "reports"

    monkeypatch.setattr(postmortem, "psycopg", None)
    monkeypatch.setattr(postmortem, "_NUMPY_MODULE", None)
    monkeypatch.setattr(postmortem, "_PANDAS_MODULE", None)

    analyzer = postmortem.create_analyzer(
        account_identifier="company",
        hours=4,
        output_dir=output_dir,
    )
    summary = analyzer.run()
    analyzer.close()

    artifacts = summary["artifacts"]
    json_path = Path(artifacts["json"])
    html_path = Path(artifacts["html"])
    assert json_path.exists()
    assert html_path.exists()

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["actual"]["equity_curve"], "equity curve should not be empty"

    state_file = tmp_path / "state" / "postmortem" / "company.json"
    assert state_file.exists()
