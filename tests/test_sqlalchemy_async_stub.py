"""Regression tests for the async helpers provided by the SQLAlchemy shim."""

import importlib
import sys

import pytest


def _clear_sqlalchemy(monkeypatch) -> None:
    for name in list(sys.modules):
        if name.startswith("sqlalchemy"):
            monkeypatch.delitem(sys.modules, name, raising=False)


@pytest.mark.asyncio
async def test_async_engine_context_manager(monkeypatch) -> None:
    """The shimmed async engine should support the async context manager protocol."""

    _clear_sqlalchemy(monkeypatch)

    stub = importlib.import_module("services.common.sqlalchemy_stub")
    stub.install()

    from sqlalchemy import Column, Integer, MetaData, Table
    from sqlalchemy.ext.asyncio import create_async_engine

    metadata = MetaData()
    table = Table("example", metadata, Column("id", Integer))

    engine = create_async_engine("sqlite+aiosqlite://")

    async with engine.begin() as connection:
        await connection.execute(table.insert().values({"id": 1}))

    await engine.dispose()

    assert table._records == [{"id": 1}]
