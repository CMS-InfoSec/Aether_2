import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from capital_optimizer import (
    Base,
    CapitalOptimizationRun,
    RunRepository,
    migrate_runs_from_file,
)


@pytest.fixture()
def optimizer_session_factory(tmp_path: Path):
    database_url = f"sqlite+pysqlite:///{(tmp_path / 'optimizer.db').as_posix()}"
    engine = create_engine(
        database_url,
        future=True,
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(bind=engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    try:
        yield factory
    finally:
        engine.dispose()


def _create_sample_payload(run_suffix: str) -> Tuple[str, dict]:
    timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc)
    run_id = f"run_{run_suffix}"
    return run_id, {
        "run_id": run_id,
        "inputs": {"accounts": []},
        "outputs": {
            "accounts": {"acct": 0.7},
            "strategies": {"strat": 0.7},
        },
        "method": "mean_variance",
        "ts": timestamp.isoformat(),
    }


def test_repository_persists_and_reads_latest(optimizer_session_factory):
    timestamp = datetime.now(timezone.utc)
    with optimizer_session_factory() as session:
        with session.begin():
            repo = RunRepository(session)
            repo.create(
                run_id="run_primary",
                method="mean_variance",
                inputs={"payload": 1},
                outputs={
                    "accounts": {"acct": 0.6},
                    "strategies": {"strat": 0.6},
                },
                timestamp=timestamp,
            )

    with optimizer_session_factory() as session:
        with session.begin():
            repo = RunRepository(session)
            latest = repo.latest()
            assert latest is not None
            assert latest.run_id == "run_primary"
            assert latest.outputs["accounts"]["acct"] == pytest.approx(0.6)


def test_migrate_runs_from_file(tmp_path: Path, optimizer_session_factory):
    run_id, payload = _create_sample_payload("legacy")
    legacy_path = tmp_path / "legacy_runs.json"
    legacy_path.write_text(json.dumps([payload]), encoding="utf-8")

    inserted = migrate_runs_from_file(legacy_path, session_factory=optimizer_session_factory)
    assert inserted == 1

    with optimizer_session_factory() as session:
        with session.begin():
            repo = RunRepository(session)
            migrated = repo.latest()
            assert migrated is not None
            assert migrated.run_id == run_id
            assert migrated.method == "mean_variance"
            assert migrated.outputs["accounts"]["acct"] == pytest.approx(0.7)


def test_repository_handles_concurrent_writes_and_reads(optimizer_session_factory):
    base_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)

    def writer(idx: int) -> Tuple[str, str]:
        run_id = f"run_{idx:03d}"
        with optimizer_session_factory() as session:
            with session.begin():
                repo = RunRepository(session)
                repo.create(
                    run_id=run_id,
                    method="mean_variance",
                    inputs={"seed": idx},
                    outputs={
                        "accounts": {f"acct_{idx}": 1.0},
                        "strategies": {f"strat_{idx}": 1.0},
                    },
                    timestamp=base_ts + timedelta(seconds=idx),
                )
        return ("write", run_id)

    def reader() -> Tuple[str, Optional[str]]:
        with optimizer_session_factory() as session:
            with session.begin():
                repo = RunRepository(session)
                latest = repo.latest()
                return ("read", latest.run_id if latest else None)

    events: List[Tuple[str, Optional[str]]] = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for idx in range(8):
            futures.append(executor.submit(writer, idx))
            futures.append(executor.submit(reader))
        for future in futures:
            events.append(future.result())

    write_ids = [value for kind, value in events if kind == "write"]
    read_ids = [value for kind, value in events if kind == "read" and value is not None]

    assert len(write_ids) == 8

    with optimizer_session_factory() as session:
        with session.begin():
            repo = RunRepository(session)
            latest = repo.latest()
            assert latest is not None
            assert latest.run_id == max(write_ids)
            stored_ids = session.execute(select(CapitalOptimizationRun.run_id)).scalars().all()
            assert sorted(stored_ids) == sorted(write_ids)

    assert any(rid in write_ids for rid in read_ids)
