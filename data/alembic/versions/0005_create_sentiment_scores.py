"""Create sentiment_scores table"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0005_create_sentiment_scores"
down_revision = "0004_config_service_postgres"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sentiment_scores",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("score", sa.String(length=16), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "ix_sentiment_scores_symbol_ts",
        "sentiment_scores",
        ["symbol", "ts"],
        unique=False,
    )
    op.create_index(
        "ix_sentiment_scores_ts",
        "sentiment_scores",
        ["ts"],
        unique=False,
    )
    op.execute("SELECT create_hypertable('sentiment_scores', 'ts', if_not_exists => TRUE)")


def downgrade() -> None:
    op.drop_index("ix_sentiment_scores_ts", table_name="sentiment_scores")
    op.drop_index("ix_sentiment_scores_symbol_ts", table_name="sentiment_scores")
    op.drop_table("sentiment_scores")
