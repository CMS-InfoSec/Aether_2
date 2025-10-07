from __future__ import annotations

from pathlib import Path

import pytest

from docs.runbooks.scripts import daily_report


def test_resolve_output_dir_with_absolute_path(tmp_path: Path) -> None:
    target = tmp_path / "reports"
    resolved = daily_report._resolve_output_dir(str(target))

    assert resolved == target
    assert resolved.is_absolute()


def test_resolve_output_dir_rejects_parent_traversal() -> None:
    with pytest.raises(ValueError):
        daily_report._resolve_output_dir("../outside")


def test_resolve_output_dir_rejects_symlink(tmp_path: Path) -> None:
    destination = tmp_path / "real"
    destination.mkdir()
    symlink_path = tmp_path / "alias"
    symlink_path.symlink_to(destination, target_is_directory=True)

    with pytest.raises(ValueError):
        daily_report._resolve_output_dir(str(symlink_path))
