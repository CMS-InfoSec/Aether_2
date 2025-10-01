
"""Helpers for running Feast materialization jobs."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

from feast import FeatureStore


def get_store(repo_path: str | None) -> FeatureStore:
    path = Path(repo_path or Path(__file__).parent)
    return FeatureStore(repo_path=str(path))


def materialize_daily(store: FeatureStore, days: int = 1) -> None:
    end_date = datetime.now(tz=timezone.utc)
    start_date = end_date - timedelta(days=days)
    store.materialize(start_date=start_date, end_date=end_date)


def materialize_incremental(store: FeatureStore) -> None:
    end_date = datetime.now(tz=timezone.utc)
    store.materialize_incremental(end_date=end_date)


def refresh_online_store(store: FeatureStore, days: int = 1) -> None:
    materialize_daily(store, days=days)
    materialize_incremental(store)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Feast materialization helpers")
    parser.add_argument("--repo", default=str(Path(__file__).parent), help="Path to the Feast repository")
    parser.add_argument("--days", type=int, default=1, help="Number of days to backfill for full materialization")
    parser.add_argument(
        "--mode",
        choices=["daily", "incremental", "refresh"],
        default="refresh",
        help="Materialization mode",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    store = get_store(args.repo)
    if args.mode == "daily":
        materialize_daily(store, days=args.days)
    elif args.mode == "incremental":
        materialize_incremental(store)
    else:
        refresh_online_store(store, days=args.days)



if __name__ == "__main__":
    main()
