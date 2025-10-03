"""Create tables required by the risk service."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


NUMERIC_PRECISION = 28
NUMERIC_SCALE = 8


def upgrade() -> None:
    op.create_table(
        "account_risk_limits",
        sa.Column("account_id", sa.String(length=64), primary_key=True),
        sa.Column(
            "max_daily_loss",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=False,
        ),
        sa.Column(
            "fee_budget",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=False,
        ),
        sa.Column(
            "max_nav_pct_per_trade",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=False,
        ),
        sa.Column(
            "notional_cap",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=False,
        ),
        sa.Column("cooldown_minutes", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("instrument_whitelist", sa.Text(), nullable=True),
        sa.Column(
            "var_95_limit",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=True,
        ),
        sa.Column(
            "var_99_limit",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=True,
        ),
        sa.Column(
            "spread_threshold_bps",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=True,
        ),
        sa.Column(
            "latency_stall_seconds",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=True,
        ),
        sa.Column("exchange_outage_block", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "correlation_threshold",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=True,
        ),
        sa.Column("correlation_lookback", sa.Integer(), nullable=True),
        sa.Column("diversification_cluster_limits", sa.Text(), nullable=True),
    )

    op.create_table(
        "account_risk_usage",
        sa.Column("account_id", sa.String(length=64), primary_key=True),
        sa.Column(
            "realized_daily_loss",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "fees_paid",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "net_asset_value",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "var_95",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=True,
        ),
        sa.Column(
            "var_99",
            sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE),
            nullable=True,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "position_size_log",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("account_id", sa.String(length=64), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("volatility", sa.Float(), nullable=False),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_position_size_log_account_id", "position_size_log", ["account_id"])

    op.create_table(
        "battle_mode_log",
        sa.Column("account_id", sa.String(length=64), primary_key=True),
        sa.Column("entered_at", sa.DateTime(timezone=True), primary_key=True),
        sa.Column("exited_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reason", sa.String(length=255), nullable=False, server_default=""),
    )


def downgrade() -> None:
    op.drop_table("battle_mode_log")
    op.drop_index("ix_position_size_log_account_id", table_name="position_size_log")
    op.drop_table("position_size_log")
    op.drop_table("account_risk_usage")
    op.drop_table("account_risk_limits")
