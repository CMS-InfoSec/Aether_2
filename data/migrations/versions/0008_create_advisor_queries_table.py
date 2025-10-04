"""Create advisor_queries persistence table."""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS advisor_queries (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    question TEXT NOT NULL,
    answer TEXT NOT NULL,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_advisor_queries_user_id ON advisor_queries (user_id);
"""


def upgrade() -> None:
    op.execute(CREATE_TABLE)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS advisor_queries;")

