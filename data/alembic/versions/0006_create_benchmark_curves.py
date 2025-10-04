"""Create benchmark_curves hypertable."""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0006_create_benchmark_curves"
down_revision = "0005_create_sentiment_scores"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "benchmark_curves",
        sa.Column("account_id", sa.String(length=128), nullable=False),
        sa.Column("benchmark", sa.String(length=64), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("pnl", sa.Float(), nullable=False, server_default=sa.text("0")),
        sa.PrimaryKeyConstraint("account_id", "benchmark", "ts", name="pk_benchmark_curves"),
    )
    op.create_index(
        "ix_benchmark_curves_account_benchmark_ts",
        "benchmark_curves",
        ["account_id", "benchmark", "ts"],
        unique=False,
    )
    op.create_index(
        "ix_benchmark_curves_ts",
        "benchmark_curves",
        ["ts"],
        unique=False,
    )
    op.execute("SELECT create_hypertable('benchmark_curves', 'ts', if_not_exists => TRUE)")


def downgrade() -> None:
    op.drop_index("ix_benchmark_curves_ts", table_name="benchmark_curves")
    op.drop_index("ix_benchmark_curves_account_benchmark_ts", table_name="benchmark_curves")
    op.drop_table("benchmark_curves")

