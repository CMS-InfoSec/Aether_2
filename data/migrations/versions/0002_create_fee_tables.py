"""Create fee schedule tables and seed Kraken spot schedule."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from alembic import op
import sqlalchemy as sa


revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


FEE_TIERS_TABLE = sa.table(
    "fee_tiers",
    sa.column("tier_id", sa.String(length=32)),
    sa.column("notional_threshold_usd", sa.Numeric(20, 2)),
    sa.column("maker_bps", sa.Numeric(10, 4)),
    sa.column("taker_bps", sa.Numeric(10, 4)),
    sa.column("effective_from", sa.DateTime(timezone=True)),
)


DEFAULT_KRAKEN_SCHEDULE = (
    ("tier_0", Decimal("0"), Decimal("16"), Decimal("26")),
    ("tier_1", Decimal("10000"), Decimal("14"), Decimal("24")),
    ("tier_2", Decimal("50000"), Decimal("12"), Decimal("22")),
    ("tier_3", Decimal("100000"), Decimal("10"), Decimal("20")),
    ("tier_4", Decimal("250000"), Decimal("8"), Decimal("18")),
    ("tier_5", Decimal("500000"), Decimal("6"), Decimal("16")),
    ("tier_6", Decimal("1000000"), Decimal("4"), Decimal("14")),
    ("tier_7", Decimal("2500000"), Decimal("2"), Decimal("12")),
    ("tier_8", Decimal("5000000"), Decimal("0"), Decimal("10")),
    ("tier_9", Decimal("10000000"), Decimal("0"), Decimal("8")),
    ("tier_10", Decimal("25000000"), Decimal("0"), Decimal("6")),
    ("tier_11", Decimal("100000000"), Decimal("0"), Decimal("4")),
)


def upgrade() -> None:
    op.create_table(
        "fee_tiers",
        sa.Column("tier_id", sa.String(length=32), primary_key=True),
        sa.Column("notional_threshold_usd", sa.Numeric(20, 2), nullable=False, server_default="0"),
        sa.Column("maker_bps", sa.Numeric(10, 4), nullable=False),
        sa.Column("taker_bps", sa.Numeric(10, 4), nullable=False),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "account_volume_30d",
        sa.Column("account_id", sa.String(length=64), primary_key=True),
        sa.Column("notional_usd_30d", sa.Numeric(20, 2), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    effective_from = datetime(2020, 1, 1, tzinfo=timezone.utc)
    op.bulk_insert(
        FEE_TIERS_TABLE,
        [
            {
                "tier_id": tier_id,
                "notional_threshold_usd": threshold,
                "maker_bps": maker,
                "taker_bps": taker,
                "effective_from": effective_from,
            }
            for tier_id, threshold, maker, taker in DEFAULT_KRAKEN_SCHEDULE
        ],
    )


def downgrade() -> None:
    op.drop_table("account_volume_30d")
    op.drop_table("fee_tiers")

