"""Create simulation mode persistence tables."""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0007_create_sim_mode_tables"
down_revision = "0006_create_benchmark_curves"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sim_mode_state",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )

    op.create_table(
        "sim_broker_orders",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("account_id", sa.String(length=128), nullable=False),
        sa.Column("client_id", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("order_type", sa.String(length=16), nullable=False),
        sa.Column("qty", sa.Numeric(36, 18), nullable=False),
        sa.Column("filled_qty", sa.Numeric(36, 18), nullable=False, server_default=sa.text("0")),
        sa.Column("avg_price", sa.Numeric(36, 18), nullable=False, server_default=sa.text("0")),
        sa.Column("limit_px", sa.Numeric(36, 18), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'open'")),
        sa.Column("pre_trade_mid", sa.Numeric(36, 18), nullable=True),
        sa.Column("last_fill_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )
    op.create_index(
        "ix_sim_broker_orders_account_id",
        "sim_broker_orders",
        ["account_id"],
        unique=False,
    )
    op.create_index(
        "ix_sim_broker_orders_client_id",
        "sim_broker_orders",
        ["client_id"],
        unique=False,
    )

    op.create_table(
        "sim_broker_fills",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("order_id", sa.Integer(), nullable=False),
        sa.Column("account_id", sa.String(length=128), nullable=False),
        sa.Column("client_id", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("qty", sa.Numeric(36, 18), nullable=False),
        sa.Column("price", sa.Numeric(36, 18), nullable=False),
        sa.Column("liquidity", sa.String(length=16), nullable=False),
        sa.Column("fee", sa.Numeric(36, 18), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )
    op.create_index(
        "ix_sim_broker_fills_order_id",
        "sim_broker_fills",
        ["order_id"],
        unique=False,
    )
    op.create_index(
        "ix_sim_broker_fills_account_id",
        "sim_broker_fills",
        ["account_id"],
        unique=False,
    )

    op.create_table(
        "sim_price_snapshots",
        sa.Column("symbol", sa.String(length=64), primary_key=True),
        sa.Column("price", sa.Numeric(36, 18), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )


def downgrade() -> None:
    op.drop_table("sim_price_snapshots")
    op.drop_index("ix_sim_broker_fills_account_id", table_name="sim_broker_fills")
    op.drop_index("ix_sim_broker_fills_order_id", table_name="sim_broker_fills")
    op.drop_table("sim_broker_fills")
    op.drop_index("ix_sim_broker_orders_client_id", table_name="sim_broker_orders")
    op.drop_index("ix_sim_broker_orders_account_id", table_name="sim_broker_orders")
    op.drop_table("sim_broker_orders")
    op.drop_table("sim_mode_state")
