from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import Column, DateTime, Float, Integer, MetaData, Numeric, String, Table, create_engine, select

from capital_flow_migrations import upgrade


def _legacy_schema(metadata: MetaData) -> tuple[Table, Table]:
    capital_flows = Table(
        "capital_flows",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("account_id", String, nullable=False),
        Column("type", String, nullable=False),
        Column("amount", Float, nullable=False),
        Column("currency", String, nullable=False),
        Column("ts", DateTime(timezone=True), nullable=False),
    )

    nav_baselines = Table(
        "nav_baselines",
        metadata,
        Column("account_id", String, primary_key=True),
        Column("currency", String, nullable=False),
        Column("baseline", Float, nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False),
    )

    return capital_flows, nav_baselines


def test_upgrade_casts_existing_float_values(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path}/legacy_capital.db", future=True)
    metadata = MetaData()
    flows_table, baselines_table = _legacy_schema(metadata)
    metadata.create_all(engine)

    recorded_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    with engine.begin() as connection:
        connection.execute(
            flows_table.insert(),
            [
                {
                    "account_id": "acct",
                    "type": "deposit",
                    "amount": 100.25,
                    "currency": "USD",
                    "ts": recorded_at,
                },
                {
                    "account_id": "acct",
                    "type": "withdraw",
                    "amount": 0.05,
                    "currency": "USD",
                    "ts": recorded_at,
                },
            ],
        )
        connection.execute(
            baselines_table.insert(),
            [
                {
                    "account_id": "acct",
                    "currency": "USD",
                    "baseline": 100.2,
                    "updated_at": recorded_at,
                }
            ],
        )

    upgrade(engine)

    post_metadata = MetaData()
    flows = Table("capital_flows", post_metadata, autoload_with=engine)
    baselines = Table("nav_baselines", post_metadata, autoload_with=engine)

    assert isinstance(flows.c.amount.type, (Numeric, String))
    assert isinstance(baselines.c.baseline.type, (Numeric, String))

    with engine.connect() as connection:
        raw_amounts = connection.execute(select(flows.c.amount).order_by(flows.c.id)).scalars().all()
        raw_baselines = connection.execute(select(baselines.c.baseline)).scalars().all()

    def _as_decimal(value: object) -> Decimal:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    amounts = [_as_decimal(value) for value in raw_amounts]
    baselines_after = [_as_decimal(value) for value in raw_baselines]

    assert all(isinstance(value, Decimal) for value in amounts)
    assert all(isinstance(value, Decimal) for value in baselines_after)
    assert amounts[0] == Decimal("100.25")
    assert amounts[1] == Decimal("0.05")
    assert baselines_after[0] == Decimal("100.2")
