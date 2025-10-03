"""Create volatility metrics hypertable."""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS vol_metrics (
    symbol TEXT NOT NULL,
    realized_vol DOUBLE PRECISION NOT NULL,
    garch_vol DOUBLE PRECISION NOT NULL,
    jump_prob DOUBLE PRECISION NOT NULL,
    atr DOUBLE PRECISION NOT NULL,
    band_width DOUBLE PRECISION NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol, ts)
);
"""


def upgrade() -> None:
    op.execute(CREATE_TABLE)
    op.execute("SELECT create_hypertable('vol_metrics', 'ts', if_not_exists => TRUE);")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS vol_metrics;")
