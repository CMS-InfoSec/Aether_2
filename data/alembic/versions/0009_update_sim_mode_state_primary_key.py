"""Update sim mode state primary key to account id."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = "0009_update_sim_mode_state_primary_key"
down_revision = "0008_create_capital_flow_tables"
branch_labels = None
depends_on = None


LEGACY_ACCOUNT_PREFIX = "__legacy__"


def _sim_mode_state_columns() -> list[sa.Column]:
    return [
        sa.Column("account_id", sa.String(length=128), primary_key=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    ]


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if not inspector.has_table("sim_mode_state"):
        return

    columns = {column["name"] for column in inspector.get_columns("sim_mode_state")}
    if columns == {"account_id", "active", "reason", "ts"}:
        # Already on the desired schema.
        return

    op.rename_table("sim_mode_state", "_sim_mode_state_legacy")
    op.create_table("sim_mode_state", *_sim_mode_state_columns())

    legacy_rows = bind.execute(
        sa.text("SELECT id, active, reason, ts FROM _sim_mode_state_legacy ORDER BY id")
    ).fetchall()
    if legacy_rows:
        bind.execute(
            sa.text(
                "INSERT INTO sim_mode_state (account_id, active, reason, ts) "
                "VALUES (:account_id, :active, :reason, :ts)"
            ),
            [
                {
                    "account_id": f"{LEGACY_ACCOUNT_PREFIX}:{row._mapping['id']}",
                    "active": row._mapping["active"],
                    "reason": row._mapping["reason"],
                    "ts": row._mapping["ts"],
                }
                for row in legacy_rows
            ],
        )

    op.drop_table("_sim_mode_state_legacy")


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if not inspector.has_table("sim_mode_state"):
        return

    op.rename_table("sim_mode_state", "_sim_mode_state_new")
    op.create_table(
        "sim_mode_state",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )

    new_rows = bind.execute(
        sa.text("SELECT active, reason, ts FROM _sim_mode_state_new ORDER BY account_id")
    ).fetchall()
    if new_rows:
        bind.execute(
            sa.text(
                "INSERT INTO sim_mode_state (active, reason, ts) VALUES (:active, :reason, :ts)"
            ),
            [
                {
                    "active": row._mapping["active"],
                    "reason": row._mapping["reason"],
                    "ts": row._mapping["ts"],
                }
                for row in new_rows
            ],
        )

    op.drop_table("_sim_mode_state_new")
