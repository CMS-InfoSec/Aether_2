"""Add admin_slug column to accounts."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0002_add_admin_slug_to_accounts"
down_revision = "0001_create_core_tables"
branch_labels = None
depends_on = None


ADMIN_SLUGS = ("company", "director-1", "director-2")


def upgrade() -> None:
    op.add_column(
        "accounts",
        sa.Column("admin_slug", sa.String(length=255), nullable=True),
    )
    op.create_unique_constraint("uq_accounts_admin_slug", "accounts", ["admin_slug"])

    update_stmt = sa.text(
        """
        UPDATE accounts
        SET admin_slug = COALESCE(metadata->>'admin_slug', :slug)
        WHERE name = :slug
        """
    )
    connection = op.get_bind()
    for slug in ADMIN_SLUGS:
        connection.execute(update_stmt, {"slug": slug})


def downgrade() -> None:
    op.drop_constraint("uq_accounts_admin_slug", "accounts", type_="unique")
    op.drop_column("accounts", "admin_slug")
