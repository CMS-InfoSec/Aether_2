#!/usr/bin/env python3
"""Fail the build when pytest reports skipped tests."""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def _collect_skipped(xml_path: Path) -> list[str]:
    if not xml_path.exists():
        raise SystemExit(f"pytest report '{xml_path}' not found")

    tree = ET.parse(xml_path)
    root = tree.getroot()

    skipped: list[str] = []

    if root.tag == "testsuite":
        suites = [root]
    else:
        suites = list(root.findall("testsuite"))

    for suite in suites:
        for case in suite.findall("testcase"):
            skip = case.find("skipped")
            if skip is not None:
                classname = case.attrib.get("classname", "")
                name = case.attrib.get("name", "")
                skipped.append("::".join(filter(None, (classname, name))))

    return skipped


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: ensure_no_skips.py <pytest-xml-path>")

    xml_path = Path(sys.argv[1])
    skipped = _collect_skipped(xml_path)

    if skipped:
        print("Skipped tests detected; treat them as failures:")
        for test in skipped:
            print(f" - {test}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
