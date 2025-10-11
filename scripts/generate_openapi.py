#!/usr/bin/env python3
"""Generate an OpenAPI specification snapshot for the admin platform."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

os.environ.setdefault("AETHER_ALLOW_INSECURE_TEST_DEFAULTS", "1")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import create_app  # noqa: E402
from auth.service import InMemoryAdminRepository, InMemorySessionStore  # noqa: E402


def build_app():
    repository = InMemoryAdminRepository()
    session_store = InMemorySessionStore()
    return create_app(admin_repository=repository, session_store=session_store)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/api/openapi.json"),
        help="Destination file for the OpenAPI document",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indentation (default: 2)",
    )
    args = parser.parse_args()

    app = build_app()
    spec = app.openapi()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(spec, indent=args.indent, sort_keys=True))
    print(f"Wrote OpenAPI spec to {args.output}")


if __name__ == "__main__":
    main()
