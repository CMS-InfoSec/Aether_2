#!/usr/bin/env python3
"""Static lint to ensure insecure defaults remain opt-in."""

from __future__ import annotations

import pathlib
import re
import sys
from typing import Iterable

ROOT = pathlib.Path(__file__).resolve().parents[1]
IGNORE_DIRS: tuple[str, ...] = (
    "tests",
    "docs",
    "deploy",
    "frontend",
    "argo",
    "pipelines",
    ".github",
)
# Match assignments that enable insecure defaults by default, e.g.
# os.environ.setdefault("FOO_ALLOW_INSECURE_DEFAULTS", "1")
_INSECURE_ENABLE = re.compile(r"ALLOW_INSECURE_DEFAULTS[^\n]*[\'\"]1[\'\"]")


def _should_check(path: pathlib.Path) -> bool:
    try:
        relative = path.relative_to(ROOT)
    except ValueError:  # pragma: no cover - defensive guard
        return False

    top_level = relative.parts[0]
    if top_level in IGNORE_DIRS:
        return False
    return True


def _scan_file(path: pathlib.Path) -> Iterable[str]:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:  # pragma: no cover - non-text file guard
        return []

    matches: list[str] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if _INSECURE_ENABLE.search(line):
            matches.append(f"{path.relative_to(ROOT)}:{lineno}: {line.strip()}")
    return matches


def main() -> int:
    violations: list[str] = []

    for path in ROOT.rglob("*.py"):
        if not _should_check(path):
            continue
        violations.extend(_scan_file(path))

    if violations:
        print("Insecure defaults must remain opt-in. Remove explicit '...=\"1\"' assignments:")
        print("\n".join(violations))
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
