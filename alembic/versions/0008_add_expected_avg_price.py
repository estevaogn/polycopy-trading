"""add expected_avg_price column to order_executions

Revision ID: 0008
Revises: 0007
Create Date: 2026-05-04 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "order_executions",
        sa.Column(
            "expected_avg_price",
            sa.Numeric(20, 8),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("order_executions", "expected_avg_price")
