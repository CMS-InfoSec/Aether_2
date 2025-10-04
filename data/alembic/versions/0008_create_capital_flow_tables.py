"""Create tables for capital flow persistence."""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0008_create_capital_flow_tables"
down_revision = "0007_create_sim_mode_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "capital_flows",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("account_id", sa.String(length=128), nullable=False),
        sa.Column("type", sa.String(length=32), nullable=False),
        sa.Column("amount", sa.Float(), nullable=False),
        sa.Column("currency", sa.String(length=16), nullable=False),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )
    op.create_index("ix_capital_flows_account_id", "capital_flows", ["account_id"], unique=False)

    op.create_table(
        "nav_baselines",
        sa.Column("account_id", sa.String(length=128), primary_key=True),
        sa.Column("currency", sa.String(length=16), nullable=False),
        sa.Column("baseline", sa.Float(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )


def downgrade() -> None:
    op.drop_table("nav_baselines")
    op.drop_index("ix_capital_flows_account_id", table_name="capital_flows")
    op.drop_table("capital_flows")
