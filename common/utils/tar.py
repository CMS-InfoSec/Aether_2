"""Secure helpers for working with tar archives."""
from __future__ import annotations

import tarfile
from pathlib import Path, PurePosixPath


def safe_extract_tar(archive: tarfile.TarFile, destination: Path) -> None:
    """Safely extract ``archive`` into ``destination``.

    The destination directory is created if necessary and validated to ensure
    it is not a symbolic link.  Each archive member is inspected before
    extraction to verify that it represents a regular file or directory with a
    relative path that stays within the destination tree.  Any attempt at path
    traversal, absolute paths, or special file types raises ``ValueError`` and
    aborts the extraction.
    """

    if destination.exists():
        if destination.is_symlink():
            raise ValueError("Tar extraction destination must not be a symlink")
        if not destination.is_dir():
            raise ValueError("Tar extraction destination must be a directory")
    destination.mkdir(parents=True, exist_ok=True)

    destination_resolved = destination.resolve(strict=True)
    members = archive.getmembers()

    for member in members:
        if not (member.isfile() or member.isdir()):
            raise ValueError("Tar archive entries must be regular files or directories")

        member_path = PurePosixPath(member.name)
        if member_path.is_absolute():
            raise ValueError("Tar archive entries must be relative paths")
        if any(part == ".." for part in member_path.parts):
            raise ValueError("Tar archive entries must not contain traversal sequences")

        candidate = destination_resolved.joinpath(Path(*member_path.parts))
        resolved_candidate = candidate.resolve(strict=False)
        try:
            resolved_candidate.relative_to(destination_resolved)
        except ValueError as exc:  # pragma: no cover - defensive programming
            raise ValueError("Tar archive entry escapes extraction directory") from exc

    archive.extractall(path=destination_resolved, filter="data")


__all__ = ["safe_extract_tar"]
