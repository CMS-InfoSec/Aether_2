"""Cast risk usage and limit monetary values to numeric."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0003_update_risk_numeric"
down_revision = ("0002_add_admin_slug_to_accounts", "0002_create_ml_shap_outputs")
branch_labels = None
depends_on = None


NUMERIC_PRECISION = 28
NUMERIC_SCALE = 8

USAGE_COLUMNS = (
    ("realized_daily_loss", False),
    ("fees_paid", False),
    ("net_asset_value", False),
    ("var_95", True),
    ("var_99", True),
)

LIMIT_COLUMNS = (
    ("max_daily_loss", False),
    ("fee_budget", False),
    ("max_nav_pct_per_trade", False),
    ("notional_cap", False),
    ("var_95_limit", True),
    ("var_99_limit", True),
    ("spread_threshold_bps", True),
    ("latency_stall_seconds", True),
    ("correlation_threshold", True),
)


def _alter_numeric(table: str, columns: tuple[tuple[str, bool], ...]) -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name
    numeric_type = sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE)

    if dialect == "postgresql":
        for column, nullable in columns:
            op.alter_column(
                table,
                column,
                type_=numeric_type,
                existing_type=sa.Float(),
                existing_nullable=nullable,
                postgresql_using=f"{column}::numeric({NUMERIC_PRECISION}, {NUMERIC_SCALE})",
            )
    else:
        with op.batch_alter_table(table) as batch_op:
            for column, nullable in columns:
                batch_op.alter_column(
                    column,
                    type_=numeric_type,
                    existing_type=sa.Float(),
                    existing_nullable=nullable,
                )


def upgrade() -> None:
    _alter_numeric("account_risk_usage", USAGE_COLUMNS)
    _alter_numeric("account_risk_limits", LIMIT_COLUMNS)


def downgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name
    float_type = sa.Float()
    numeric_type = sa.Numeric(precision=NUMERIC_PRECISION, scale=NUMERIC_SCALE)

    if dialect == "postgresql":
        for column, nullable in USAGE_COLUMNS:
            op.alter_column(
                "account_risk_usage",
                column,
                type_=float_type,
                existing_type=numeric_type,
                existing_nullable=nullable,
                postgresql_using=f"{column}::double precision",
            )
        for column, nullable in LIMIT_COLUMNS:
            op.alter_column(
                "account_risk_limits",
                column,
                type_=float_type,
                existing_type=numeric_type,
                existing_nullable=nullable,
                postgresql_using=f"{column}::double precision",
            )
    else:
        with op.batch_alter_table("account_risk_usage") as batch_op:
            for column, nullable in USAGE_COLUMNS:
                batch_op.alter_column(
                    column,
                    type_=float_type,
                    existing_type=numeric_type,
                    existing_nullable=nullable,
                )
        with op.batch_alter_table("account_risk_limits") as batch_op:
            for column, nullable in LIMIT_COLUMNS:
                batch_op.alter_column(
                    column,
                    type_=float_type,
                    existing_type=numeric_type,
                    existing_nullable=nullable,
                )
