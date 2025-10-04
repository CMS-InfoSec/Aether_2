"""Create release_manifests table"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = "0001_create_release_manifests_table"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if "release_manifests" not in inspector.get_table_names():
        op.create_table(
            "release_manifests",
            sa.Column("manifest_id", sa.String(), primary_key=True),
            sa.Column("manifest_json", sa.JSON(), nullable=False),
            sa.Column("manifest_hash", sa.String(), nullable=True),
            sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        )
        op.create_index("ix_release_manifests_ts", "release_manifests", ["ts"], unique=False)
        return

    columns = {col["name"] for col in inspector.get_columns("release_manifests")}
    if "manifest_hash" not in columns:
        op.add_column("release_manifests", sa.Column("manifest_hash", sa.String(), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if "release_manifests" in inspector.get_table_names():
        indexes = {idx["name"] for idx in inspector.get_indexes("release_manifests")}
        if "ix_release_manifests_ts" in indexes:
            op.drop_index("ix_release_manifests_ts", table_name="release_manifests")
        op.drop_table("release_manifests")
