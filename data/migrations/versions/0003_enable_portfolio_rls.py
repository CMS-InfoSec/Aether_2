"""Enable row level security for portfolio tables."""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


CREATE_CURRENT_ACCOUNT_IDS = """
CREATE OR REPLACE FUNCTION current_account_ids()
RETURNS SETOF uuid
LANGUAGE sql
STABLE
AS $$
    WITH scope_source AS (
        SELECT COALESCE(
            NULLIF(current_setting('app.account_scopes', true), ''),
            (
                SELECT (regexp_match(COALESCE(current_setting('application_name', true), ''), 'account_scopes=([^ ]+)'))[1]
            )
        ) AS scopes
    ),
    expanded AS (
        SELECT DISTINCT trim(value) AS scope
        FROM scope_source,
        LATERAL regexp_split_to_table(COALESCE(scopes, ''), ',') AS value
        WHERE trim(value) <> ''
    )
    SELECT scope::uuid
    FROM expanded
    WHERE scope ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
$$;
"""

DROP_CURRENT_ACCOUNT_IDS = "DROP FUNCTION IF EXISTS current_account_ids();"

PORTFOLIO_TABLES = ("positions", "pnl_curves", "orders", "fills")


def upgrade() -> None:
    op.execute(CREATE_CURRENT_ACCOUNT_IDS)
    for table in PORTFOLIO_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
        op.execute(f"DROP POLICY IF EXISTS rls_{table}_account_scope ON {table};")
        op.execute(
            f"""
            CREATE POLICY rls_{table}_account_scope
            ON {table}
            FOR SELECT
            USING (
                account_id = ANY (ARRAY(SELECT current_account_ids()))
            );
            """
        )


def downgrade() -> None:
    for table in PORTFOLIO_TABLES:
        op.execute(f"DROP POLICY IF EXISTS rls_{table}_account_scope ON {table};")
        op.execute(f"ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY;")
        op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY;")
    op.execute(DROP_CURRENT_ACCOUNT_IDS)
