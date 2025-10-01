
"""Create hypertable for model SHAP outputs."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0002_create_ml_shap_outputs"
down_revision = "0001_create_core_tables"
branch_labels = None
depends_on = None



TABLE_NAME = "ml_shap_outputs"
TIME_COLUMN = "inference_time"


def upgrade() -> None:
    op.create_table(
        TABLE_NAME,
        sa.Column("account_id", sa.String(length=255), nullable=False),
        sa.Column("feature_name", sa.String(length=255), nullable=False),
        sa.Column("shap_value", sa.Numeric(30, 12), nullable=False),
        sa.Column(TIME_COLUMN, sa.DateTime(timezone=True), nullable=False),
        sa.Column("model_version", sa.String(length=128), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint(
            "account_id",
            "feature_name",
            TIME_COLUMN,
            "model_version",

            name="pk_ml_shap_outputs",
        ),
    )

    op.execute(

        f"SELECT create_hypertable('{TABLE_NAME}', '{TIME_COLUMN}', if_not_exists => TRUE)"
    )

    op.create_index(
        "ix_ml_shap_outputs_inference_time",
        TABLE_NAME,
        [TIME_COLUMN],
    )
    op.create_index(
        "ix_ml_shap_outputs_account_time",
        TABLE_NAME,
        ["account_id", TIME_COLUMN],

    )


def downgrade() -> None:

    op.drop_index("ix_ml_shap_outputs_account_time", table_name=TABLE_NAME)
    op.drop_index("ix_ml_shap_outputs_inference_time", table_name=TABLE_NAME)
    op.drop_table(TABLE_NAME)

