"""Create ml_shap_outputs hypertable."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0002_create_ml_shap_outputs"
down_revision = "0001_create_core_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ml_shap_outputs",
        sa.Column(
            "account_id",
            sa.Integer(),
            sa.ForeignKey("accounts.account_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("feature_name", sa.String(length=255), nullable=False),
        sa.Column("shap_value", sa.Numeric(30, 12), nullable=False),
        sa.Column("inference_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("model_version", sa.String(length=128), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint(
            "account_id", "feature_name", "inference_time", "model_version",
            name="pk_ml_shap_outputs",
        ),
    )

    op.execute(
        "SELECT create_hypertable('ml_shap_outputs', 'inference_time', if_not_exists => TRUE)"
    )

    op.create_index(
        "ix_ml_shap_outputs_account_time",
        "ml_shap_outputs",
        ["account_id", "inference_time"],
    )


def downgrade() -> None:
    op.drop_index("ix_ml_shap_outputs_account_time", table_name="ml_shap_outputs")
    op.drop_table("ml_shap_outputs")
