"""Create TimescaleDB hypertables for Aether datasets."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0001_create_core_tables"
down_revision = None
branch_labels = None
depends_on = None


TIMESERIES_TABLES = {
    "bars": "ts",
    "orderbook_events": "ts",
    "features": "event_ts",
    "labels": "event_ts",
    "orders": "submitted_ts",
    "fills": "fill_ts",
    "positions": "event_ts",
    "pnl": "event_ts",
    "risk_events": "event_ts",
    "audit_logs": "created_at",
}


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

    op.create_table(
        "accounts",
        sa.Column("account_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("admin_slug", sa.String(length=255), nullable=True, unique=True),
        sa.Column("owner", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("metadata", sa.JSON(), nullable=True),
    )

    op.create_table(
        "credentials",
        sa.Column("credential_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.account_id", ondelete="CASCADE"), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("encrypted_secret", sa.LargeBinary(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("account_id", "provider", "name", name="uq_credentials_provider_name"),
    )

    op.create_table(
        "universe",
        sa.Column("symbol", sa.String(length=32), primary_key=True),
        sa.Column("exchange", sa.String(length=32), nullable=False),
        sa.Column("base_asset", sa.String(length=32), nullable=False),
        sa.Column("quote_asset", sa.String(length=32), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("listing_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delisting_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
    )

    op.create_table(
        "config_versions",
        sa.Column("config_key", sa.String(length=255), primary_key=True),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("checksum", sa.String(length=128), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
    )

    op.create_table(
        "bars",
        sa.Column("symbol", sa.String(length=32), sa.ForeignKey("universe.symbol", ondelete="CASCADE"), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(24, 10), nullable=False),
        sa.Column("high", sa.Numeric(24, 10), nullable=False),
        sa.Column("low", sa.Numeric(24, 10), nullable=False),
        sa.Column("close", sa.Numeric(24, 10), nullable=False),
        sa.Column("volume", sa.Numeric(28, 10), nullable=False),
        sa.Column("trade_count", sa.Integer(), nullable=True),
        sa.Column("vwap", sa.Numeric(24, 10), nullable=True),
        sa.PrimaryKeyConstraint("symbol", "ts", name="pk_bars"),
    )

    op.create_table(
        "orderbook_events",
        sa.Column("symbol", sa.String(length=32), sa.ForeignKey("universe.symbol", ondelete="CASCADE"), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("side", sa.String(length=4), nullable=False),
        sa.Column("price", sa.Numeric(28, 10), nullable=False),
        sa.Column("size", sa.Numeric(28, 10), nullable=False),
        sa.Column("action", sa.String(length=16), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=True),
        sa.Column("meta", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("symbol", "ts", "side", "price", name="pk_orderbook_events"),
    )

    op.create_table(
        "features",
        sa.Column("feature_name", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("event_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("value", sa.Numeric(30, 12), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("feature_name", "symbol", "event_ts", name="pk_features"),
    )

    op.create_table(
        "labels",
        sa.Column("label_name", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("event_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("value", sa.Numeric(30, 12), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("label_name", "symbol", "event_ts", name="pk_labels"),
    )

    op.create_table(
        "orders",
        sa.Column("order_id", sa.String(length=64), primary_key=True),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.account_id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(length=32), sa.ForeignKey("universe.symbol", ondelete="SET NULL"), nullable=True),
        sa.Column("side", sa.String(length=4), nullable=False),
        sa.Column("type", sa.String(length=16), nullable=False),
        sa.Column("price", sa.Numeric(28, 10), nullable=True),
        sa.Column("size", sa.Numeric(28, 10), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("submitted_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
    )

    op.create_table(
        "fills",
        sa.Column("fill_id", sa.String(length=64), primary_key=True),
        sa.Column("order_id", sa.String(length=64), sa.ForeignKey("orders.order_id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("fill_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("price", sa.Numeric(28, 10), nullable=False),
        sa.Column("size", sa.Numeric(28, 10), nullable=False),
        sa.Column("fee", sa.Numeric(28, 10), nullable=True),
        sa.Column("liquidity", sa.String(length=16), nullable=True),
    )

    op.create_table(
        "positions",
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.account_id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("event_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("quantity", sa.Numeric(28, 10), nullable=False),
        sa.Column("notional", sa.Numeric(28, 10), nullable=True),
        sa.Column("cost_basis", sa.Numeric(28, 10), nullable=True),
        sa.PrimaryKeyConstraint("account_id", "symbol", "event_ts", name="pk_positions"),
    )

    op.create_table(
        "pnl",
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.account_id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("event_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("realized", sa.Numeric(28, 10), nullable=False, server_default="0"),
        sa.Column("unrealized", sa.Numeric(28, 10), nullable=False, server_default="0"),
        sa.Column("fees", sa.Numeric(28, 10), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("account_id", "symbol", "event_ts", name="pk_pnl"),
    )

    op.create_table(
        "risk_events",
        sa.Column("event_id", sa.String(length=64), primary_key=True),
        sa.Column("account_id", sa.Integer(), sa.ForeignKey("accounts.account_id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=True),
        sa.Column("event_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("details", sa.JSON(), nullable=True),
    )

    op.create_table(
        "audit_logs",
        sa.Column("log_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("actor", sa.String(length=255), nullable=False),
        sa.Column("action", sa.String(length=255), nullable=False),
        sa.Column("target", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("payload", sa.JSON(), nullable=True),
    )

    op.create_table(
        "orderbook_snapshots",
        sa.Column("symbol", sa.String(length=32), sa.ForeignKey("universe.symbol", ondelete="CASCADE"), nullable=False),
        sa.Column("depth", sa.Integer(), nullable=False, server_default="10"),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bids", sa.JSON(), nullable=False),
        sa.Column("asks", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("symbol", "depth", "as_of", name="pk_orderbook_snapshots"),
    )

    for table_name, ts_col in TIMESERIES_TABLES.items():
        op.execute(
            f"SELECT create_hypertable('{table_name}', '{ts_col}', if_not_exists => TRUE)"
        )

    # snapshots also benefits from hypertable
    op.execute(
        "SELECT create_hypertable('orderbook_snapshots', 'as_of', if_not_exists => TRUE)"
    )


def downgrade() -> None:
    for table in [
        "orderbook_snapshots",
        "audit_logs",
        "risk_events",
        "pnl",
        "positions",
        "fills",
        "orders",
        "labels",
        "features",
        "orderbook_events",
        "bars",
        "config_versions",
        "universe",
        "credentials",
        "accounts",
    ]:
        op.drop_table(table)
