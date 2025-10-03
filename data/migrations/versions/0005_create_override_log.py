"""Create override_log table for manual trade decisions."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "override_log",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("intent_id", sa.String(length=128), nullable=False),
        sa.Column("account_id", sa.String(length=128), nullable=False),
        sa.Column("actor", sa.String(length=128), nullable=False),
        sa.Column("decision", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_override_log_intent_id", "override_log", ["intent_id"])
    op.create_index("ix_override_log_account_id", "override_log", ["account_id"])
    op.create_index("ix_override_log_ts", "override_log", ["ts"])


def downgrade() -> None:
    op.drop_index("ix_override_log_ts", table_name="override_log")
    op.drop_index("ix_override_log_account_id", table_name="override_log")
    op.drop_index("ix_override_log_intent_id", table_name="override_log")
    op.drop_table("override_log")

