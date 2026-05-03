"""relax order_executions mode_result_consistency to allow dry_run failed

Revision ID: 0006
Revises: 0005
Create Date: 2026-05-02 00:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint(
        "order_executions_mode_result_consistency",
        "order_executions",
        type_="check",
    )
    op.create_check_constraint(
        "order_executions_mode_result_consistency",
        "order_executions",
        "(mode = 'real' AND result IN ('executed', 'failed')) "
        "OR (mode = 'dry_run' AND result IN ('dry_run', 'failed'))",
    )


def downgrade() -> None:
    op.drop_constraint(
        "order_executions_mode_result_consistency",
        "order_executions",
        type_="check",
    )
    op.create_check_constraint(
        "order_executions_mode_result_consistency",
        "order_executions",
        "(mode = 'real' AND result IN ('executed', 'failed')) "
        "OR (mode = 'dry_run' AND result = 'dry_run')",
    )
