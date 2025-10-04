"""Create capital allocator history tables."""

from __future__ import annotations

import os

from alembic import op

# revision identifiers, used by Alembic.
revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None

_PCT_SCALE = int(os.getenv("ALLOCATOR_PCT_SCALE", "6"))

_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS capital_allocations (
    id BIGSERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    pct NUMERIC(18, {_PCT_SCALE}) NOT NULL,
    ts TIMESTAMPTZ NOT NULL
);
"""

_CREATE_ACCOUNT_INDEX = "CREATE INDEX IF NOT EXISTS idx_capital_allocations_account_id ON capital_allocations (account_id);"
_CREATE_TS_INDEX = "CREATE INDEX IF NOT EXISTS idx_capital_allocations_ts ON capital_allocations (ts DESC);"


def upgrade() -> None:
    op.execute(_CREATE_TABLE)
    op.execute(_CREATE_ACCOUNT_INDEX)
    op.execute(_CREATE_TS_INDEX)
    op.execute("SELECT create_hypertable('capital_allocations', 'ts', if_not_exists => TRUE);")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_capital_allocations_ts;")
    op.execute("DROP INDEX IF EXISTS idx_capital_allocations_account_id;")
    op.execute("DROP TABLE IF EXISTS capital_allocations;")
