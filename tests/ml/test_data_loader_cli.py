from pathlib import Path

import pytest

pytest.importorskip("numpy", exc_type=ImportError)
pytest.importorskip("pandas", exc_type=ImportError)

from ml.data_loader import _resolve_output_path


@pytest.mark.parametrize(
    "raw_path, expected",
    [
        ("/tmp/walk_forward.parquet", Path("/tmp/walk_forward.parquet").absolute()),
    ],
)
def test_resolve_output_path_absolute(raw_path, expected):
    assert _resolve_output_path(raw_path) == expected


def test_resolve_output_path_normalises_relative(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = _resolve_output_path("outputs/split.parquet")
    assert result == (tmp_path / "outputs" / "split.parquet").absolute()


@pytest.mark.parametrize(
    "raw_path, message",
    [
        ("../escape.parquet", "parent directory"),
        ("bad\x00path.parquet", "control characters"),
    ],
)
def test_resolve_output_path_rejects_invalid_inputs(raw_path, message):
    with pytest.raises(ValueError) as exc:
        _resolve_output_path(raw_path)
    assert message in str(exc.value)


def test_resolve_output_path_rejects_directories(tmp_path):
    directory = tmp_path / "existing"
    directory.mkdir()
    with pytest.raises(ValueError) as exc:
        _resolve_output_path(str(directory))
    assert "file" in str(exc.value)


def test_resolve_output_path_rejects_symlink(tmp_path):
    target = tmp_path / "target.parquet"
    target.write_text("data")
    link = tmp_path / "link.parquet"
    link.symlink_to(target)
    with pytest.raises(ValueError) as exc:
        _resolve_output_path(str(link))
    assert "symlink" in str(exc.value)


def test_resolve_output_path_rejects_symlinked_parent(tmp_path):
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    symlink_parent = tmp_path / "alias"
    symlink_parent.symlink_to(real_dir, target_is_directory=True)
    with pytest.raises(ValueError) as exc:
        _resolve_output_path(str(symlink_parent / "file.parquet"))
    assert "symlink" in str(exc.value)
