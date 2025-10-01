"""Utility script to trigger Feast materialization for nightly jobs."""
from __future__ import annotations

import argparse
import datetime as dt
import subprocess
from pathlib import Path
from typing import Sequence

FEAST_BIN = "feast"


def run_materialize(start: dt.datetime, end: dt.datetime, repo_path: Path) -> None:
    cmd = [
        FEAST_BIN,
        "-c",
        str(repo_path),
        "materialize",
        start.isoformat(),
        end.isoformat(),
    ]
    subprocess.run(cmd, check=True)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Materialize Feast offline data into Redis online store")
    parser.add_argument("--repo", type=Path, default=Path(__file__).parent, help="Path to Feast repository")
    parser.add_argument(
        "--days", type=int, default=1, help="Number of trailing days to materialize (default: 1)"
    )
    args = parser.parse_args(argv)

    end = dt.datetime.utcnow()
    start = end - dt.timedelta(days=args.days)
    run_materialize(start, end, args.repo)


if __name__ == "__main__":
    main()
