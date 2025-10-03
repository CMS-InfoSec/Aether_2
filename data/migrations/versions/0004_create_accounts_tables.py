"""Create tables for admin profiles and risk change approvals."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "admin_profiles",
        sa.Column("admin_id", sa.String(length=64), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.Column("kraken_credentials_linked", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("last_updated", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "risk_configuration_changes",
        sa.Column("request_id", sa.String(length=64), primary_key=True),
        sa.Column("requested_by", sa.String(length=64), nullable=False),
        sa.Column(
            "payload",
            sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql"),
            nullable=False,
        ),
        sa.Column("executed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "risk_configuration_change_approvals",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("request_id", sa.String(length=64), nullable=False),
        sa.Column("admin_id", sa.String(length=64), nullable=False),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["request_id"], ["risk_configuration_changes.request_id"], ondelete="CASCADE"),
        sa.UniqueConstraint("request_id", "admin_id", name="uq_risk_change_approval"),
    )


def downgrade() -> None:
    op.drop_table("risk_configuration_change_approvals")
    op.drop_table("risk_configuration_changes")
    op.drop_table("admin_profiles")
