"""add order_executions table

Revision ID: 0005
Revises: 0004
Create Date: 2026-05-02 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "order_executions",
        sa.Column("trade_event_id", sa.Uuid(), nullable=False),
        sa.Column("wallet", sa.String(), nullable=False),
        sa.Column("condition_id", sa.String(), nullable=False),
        sa.Column("token_id", sa.String(), nullable=False),
        sa.Column("final_size_usdc", sa.Numeric(20, 6), nullable=False),
        sa.Column("mode", sa.String(), nullable=False),
        sa.Column("result", sa.String(), nullable=False),
        sa.Column("tx_hash", sa.String(), nullable=True),
        sa.Column("gas_wei", sa.Numeric(40, 0), nullable=True),
        sa.Column("failure_reason", sa.String(), nullable=True),
        sa.Column("error_message", sa.String(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "mode IN ('real', 'dry_run')",
            name="order_executions_mode_enum",
        ),
        sa.CheckConstraint(
            "result IN ('executed', 'failed', 'dry_run')",
            name="order_executions_result_enum",
        ),
        sa.CheckConstraint(
            "(mode = 'real' AND result IN ('executed', 'failed')) "
            "OR (mode = 'dry_run' AND result = 'dry_run')",
            name="order_executions_mode_result_consistency",
        ),
        sa.CheckConstraint(
            "(result = 'executed' AND tx_hash IS NOT NULL) OR result IN ('failed', 'dry_run')",
            name="order_executions_executed_has_tx",
        ),
        sa.CheckConstraint(
            "(result = 'failed' AND failure_reason IS NOT NULL AND error_message IS NOT NULL) "
            "OR result IN ('executed', 'dry_run')",
            name="order_executions_failed_has_reason",
        ),
        sa.CheckConstraint(
            "(result = 'dry_run' AND tx_hash IS NULL AND gas_wei IS NULL "
            "AND failure_reason IS NULL) "
            "OR result IN ('executed', 'failed')",
            name="order_executions_dry_run_no_tx",
        ),
        sa.CheckConstraint(
            "final_size_usdc > 0",
            name="order_executions_size_positive",
        ),
        sa.PrimaryKeyConstraint("trade_event_id"),
    )
    op.create_index(
        "idx_order_executions_wallet_decided_at",
        "order_executions",
        ["wallet", "decided_at"],
    )
    op.create_index(
        "idx_order_executions_failed_decided_at",
        "order_executions",
        ["decided_at"],
        postgresql_where=sa.text("result = 'failed'"),
    )
    op.create_index(
        "idx_order_executions_real_executed",
        "order_executions",
        ["decided_at"],
        postgresql_where=sa.text("mode = 'real' AND result = 'executed'"),
    )


def downgrade() -> None:
    op.drop_index("idx_order_executions_real_executed", table_name="order_executions")
    op.drop_index("idx_order_executions_failed_decided_at", table_name="order_executions")
    op.drop_index("idx_order_executions_wallet_decided_at", table_name="order_executions")
    op.drop_table("order_executions")
