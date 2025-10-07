"""Secure helpers for working with tar archives."""
from __future__ import annotations

import tarfile
from pathlib import Path, PurePosixPath


def safe_extract_tar(archive: tarfile.TarFile, destination: Path) -> None:
    """Safely extract ``archive`` into ``destination``.

    The destination directory is created if necessary and validated to ensure
    it is absolute and not backed by symbolic links.  Each archive member is
    inspected before extraction to verify that it represents a regular file or
    directory with a relative path that stays within the destination tree.  Any
    attempt at path traversal, absolute paths, or special file types raises
    ``ValueError`` and aborts the extraction.
    """

    if not destination.is_absolute():
        raise ValueError("Tar extraction destination must be an absolute path")

    for ancestor in (destination,) + tuple(destination.parents):
        if ancestor.exists() and ancestor.is_symlink():
            raise ValueError(
                "Tar extraction destination and its ancestors must not be symlinks"
            )

    if destination.exists():
        if not destination.is_dir():
            raise ValueError("Tar extraction destination must be a directory")
    destination.mkdir(parents=True, exist_ok=True)

    destination_resolved = destination.resolve(strict=True)
    members = archive.getmembers()

    def _sanitise_metadata(member: tarfile.TarInfo) -> tarfile.TarInfo:
        """Emulate the Python 3.12 ``filter="data"`` behaviour for metadata."""

        replacements = {}

        if member.mode is not None:
            safe_mode = member.mode & 0o755

            if member.isfile():
                if not safe_mode & 0o100:
                    safe_mode &= ~0o111
                safe_mode |= 0o600
            elif member.isdir():
                safe_mode = None
            else:  # pragma: no cover - defensive programming
                raise ValueError("Tar archive entries must be regular files or directories")

            if safe_mode != member.mode:
                replacements["mode"] = safe_mode

        if member.uid is not None:
            replacements["uid"] = None
        if member.gid is not None:
            replacements["gid"] = None
        if member.uname is not None:
            replacements["uname"] = None
        if member.gname is not None:
            replacements["gname"] = None

        if replacements:
            member = member.replace(**replacements)

        return member

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

    sanitised_members = [_sanitise_metadata(member) for member in members]

    archive.extractall(path=destination_resolved, members=sanitised_members)


__all__ = ["safe_extract_tar"]
