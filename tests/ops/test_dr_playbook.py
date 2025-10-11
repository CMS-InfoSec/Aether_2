"""Regression coverage for the DR playbook restore hardening."""
from __future__ import annotations

import io
import json
import os
import shutil
import tarfile
from pathlib import Path

import pytest

import dr_playbook
from dr_playbook import DisasterRecoveryConfig, _restore_mlflow_artifacts


def _config_for(path: Path) -> DisasterRecoveryConfig:
    return DisasterRecoveryConfig(
        timescale_dsn="postgresql://localhost/postgres",
        redis_url="redis://localhost:6379/0",
        mlflow_artifact_uri="s3://dummy/artifacts",
        object_store_bucket="dummy",
        mlflow_restore_path=path,
    )


def _write_archive(path: Path, members: dict[str, bytes]) -> None:
    with tarfile.open(path, "w:gz") as archive:
        for name, payload in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DR_TIMESCALE_DSN", "postgresql://localhost/postgres")
    monkeypatch.setenv("DR_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("DR_MLFLOW_ARTIFACT_URI", "s3://dummy/artifacts")
    monkeypatch.setenv("DR_OBJECT_STORE_BUCKET", "dummy")


def test_restore_mlflow_artifacts_rejects_traversal(tmp_path: Path) -> None:
    archive = tmp_path / "malicious.tar.gz"
    _write_archive(archive, {"../escape.txt": b"bad"})

    config = _config_for(tmp_path / "restore")

    with pytest.raises(ValueError, match="traversal"):
        _restore_mlflow_artifacts(config, archive)

    assert not any(tmp_path.rglob("escape.txt"))


def test_restore_mlflow_artifacts_rejects_symlink_base(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    os.symlink(target, link)

    archive = tmp_path / "safe.tar.gz"
    _write_archive(archive, {"artifact/file.txt": b"data"})

    config = _config_for(link)

    with pytest.raises(ValueError, match="symlink"):
        _restore_mlflow_artifacts(config, archive)

    assert not any(target.rglob("file.txt"))


def test_from_env_resolves_relative_work_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DR_WORK_DIR", "relative/work")

    config = DisasterRecoveryConfig.from_env()

    expected = (tmp_path / "relative" / "work" / "aether-dr").resolve()
    assert config.work_dir == expected
    assert config.work_dir.is_dir()


def test_from_env_rejects_symlink_work_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _set_required_env(monkeypatch)
    base = tmp_path / "base"
    base.mkdir()
    link = tmp_path / "link"
    os.symlink(base, link)
    monkeypatch.setenv("DR_WORK_DIR", str(link))

    with pytest.raises(ValueError, match="DR_WORK_DIR must not reference symlinked directories"):
        DisasterRecoveryConfig.from_env()


def test_from_env_rejects_file_work_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _set_required_env(monkeypatch)
    file_path = tmp_path / "work-file"
    file_path.write_text("not a directory")
    monkeypatch.setenv("DR_WORK_DIR", str(file_path))

    with pytest.raises(ValueError, match="DR_WORK_DIR must reference a directory"):
        DisasterRecoveryConfig.from_env()


def test_log_dr_action_bootstraps_log_table(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls: list[tuple[str, tuple[str, str] | None]] = []

    class DummyCursor:
        def __enter__(self) -> "DummyCursor":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def execute(self, query, params=None) -> None:
            calls.append((str(query), params))

    class DummyConnection:
        def __enter__(self) -> "DummyConnection":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def cursor(self) -> DummyCursor:
            return DummyCursor()

    def fake_connect(*args, **kwargs):  # type: ignore[no-untyped-def]
        return DummyConnection()

    class _Identifier:
        def __init__(self, name: str) -> None:
            self.name = name

        def __str__(self) -> str:
            return f'"{self.name}"'

    class _SQL:
        def __init__(self, template: str) -> None:
            self.template = template

        def format(self, **kwargs: object) -> "_SQL":
            rendered = self.template
            for key, value in kwargs.items():
                rendered = rendered.replace("{" + key + "}", str(value))
            return _SQL(rendered)

        def __str__(self) -> str:
            return self.template

    monkeypatch.setattr(dr_playbook, "psycopg", type("PsycoStub", (), {"connect": staticmethod(fake_connect)}))
    monkeypatch.setattr(
        dr_playbook,
        "sql",
        type("SqlStub", (), {"SQL": staticmethod(lambda template: _SQL(template)), "Identifier": staticmethod(_Identifier)}),
    )
    monkeypatch.setattr(dr_playbook, "_LOG_TABLE_BOOTSTRAPPED", set())

    config = DisasterRecoveryConfig(
        timescale_dsn="postgresql://localhost/postgres",
        redis_url="redis://localhost:6379/0",
        mlflow_artifact_uri="s3://dummy/artifacts",
        object_store_bucket="dummy",
        mlflow_restore_path=tmp_path,
    )

    dr_playbook._log_dr_action(config, "snapshot:start")
    dr_playbook._log_dr_action(config, "snapshot:complete")

    assert "CREATE TABLE IF NOT EXISTS \"dr_log\"" in calls[0][0]
    assert calls[0][1] is None
    assert calls[1][0].startswith("INSERT INTO \"dr_log\"")
    assert calls[1][1] == (config.actor, "snapshot:start")
    assert calls[2][0].startswith("INSERT INTO \"dr_log\"")
    assert calls[2][1] == (config.actor, "snapshot:complete")


def test_snapshot_cluster_records_actions(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bundle_path = tmp_path / "bundle.tar.gz"
    bundle_path.write_bytes(b"bundle")

    config = DisasterRecoveryConfig(
        timescale_dsn="postgresql://localhost/postgres",
        redis_url="redis://localhost:6379/0",
        mlflow_artifact_uri="s3://dummy/artifacts",
        object_store_bucket="dummy",
        work_dir=tmp_path,
    )

    actions: list[str] = []
    monkeypatch.setattr(dr_playbook, "_log_dr_action", lambda cfg, action: actions.append(action))
    monkeypatch.setattr(dr_playbook, "create_snapshot_bundle", lambda cfg: bundle_path)
    monkeypatch.setattr(dr_playbook, "push_snapshot", lambda bundle, cfg: f"{cfg.object_store_prefix}/{bundle.name}")

    key = dr_playbook.snapshot_cluster(config)

    assert key == f"{config.object_store_prefix}/{bundle_path.name}"
    assert actions == [
        "snapshot:start",
        f"snapshot:complete:{config.object_store_prefix}/{bundle_path.name}",
    ]


def test_restore_cluster_happy_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundle_root"
    bundle_root.mkdir()
    manifest = {
        "timescale_dump": "timescale.dump",
        "redis_snapshot": "redis.rdb",
        "mlflow_archive": "mlflow.tar.gz",
    }
    for name in manifest.values():
        (bundle_root / name).write_text(f"payload:{name}")
    (bundle_root / "manifest.json").write_text(json.dumps(manifest))

    bundle_path = tmp_path / "bundle.tar.gz"
    with tarfile.open(bundle_path, "w:gz") as tar:
        for name in ["manifest.json", *manifest.values()]:
            tar.add(bundle_root / name, arcname=name)

    class DummyClient:
        def download_file(self, bucket: str, key: str, dest: str) -> None:
            shutil.copyfile(bundle_path, dest)

    monkeypatch.setattr(dr_playbook, "_build_object_store_client", lambda cfg: DummyClient())

    actions: list[str] = []
    monkeypatch.setattr(dr_playbook, "_log_dr_action", lambda cfg, action: actions.append(action))

    calls: list[tuple[str, Path]] = []

    def _capture_timescale(config: DisasterRecoveryConfig, dump: Path, *, drop_existing: bool = True) -> None:
        calls.append(("timescale", dump))

    def _capture_redis(config: DisasterRecoveryConfig, snapshot: Path) -> None:
        calls.append(("redis", snapshot))

    def _capture_mlflow(config: DisasterRecoveryConfig, archive: Path) -> None:
        calls.append(("mlflow", archive))

    monkeypatch.setattr(dr_playbook, "_restore_timescaledb", _capture_timescale)
    monkeypatch.setattr(dr_playbook, "_restore_redis_state", _capture_redis)
    monkeypatch.setattr(dr_playbook, "_restore_mlflow_artifacts", _capture_mlflow)

    config = DisasterRecoveryConfig(
        timescale_dsn="postgresql://localhost/postgres",
        redis_url="redis://localhost:6379/0",
        mlflow_artifact_uri="s3://dummy/artifacts",
        object_store_bucket="dummy",
        work_dir=tmp_path,
    )

    dr_playbook.restore_cluster("snapshot-key.tar.gz", config)

    expected_key = f"{config.object_store_prefix}/snapshot-key.tar.gz"
    assert actions == [
        f"restore:start:{expected_key}",
        f"restore:complete:{expected_key}",
    ]

    expected_names = {
        "timescale": manifest["timescale_dump"],
        "redis": manifest["redis_snapshot"],
        "mlflow": manifest["mlflow_archive"],
    }
    assert {kind for kind, _ in calls} == set(expected_names)
    for kind, path in calls:
        assert path.name == expected_names[kind]
