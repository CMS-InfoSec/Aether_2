from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "anomaly_log",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("account_id", sa.String(length=128), nullable=False),
        sa.Column("anomaly_type", sa.String(length=128), nullable=False),
        sa.Column(
            "details_json",
            sa.JSON().with_variant(postgresql.JSONB(none_as_null=True), "postgresql"),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_anomaly_log_account_id", "anomaly_log", ["account_id"])
    op.create_index("ix_anomaly_log_ts", "anomaly_log", ["ts"])


def downgrade() -> None:
    op.drop_index("ix_anomaly_log_ts", table_name="anomaly_log")
    op.drop_index("ix_anomaly_log_account_id", table_name="anomaly_log")
    op.drop_table("anomaly_log")
