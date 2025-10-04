"""Create seasonality metrics persistence table."""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS seasonality_metrics (
    symbol TEXT NOT NULL,
    period TEXT NOT NULL,
    avg_return DOUBLE PRECISION NOT NULL,
    avg_vol DOUBLE PRECISION NOT NULL,
    avg_volume DOUBLE PRECISION NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol, period)
);
"""


def upgrade() -> None:
    op.execute(CREATE_TABLE)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS seasonality_metrics;")
