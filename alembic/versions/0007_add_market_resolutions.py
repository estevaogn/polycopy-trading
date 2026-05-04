"""add market_resolutions table

Revision ID: 0007
Revises: 0006
Create Date: 2026-05-04 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "market_resolutions",
        sa.Column("condition_id", sa.String(), nullable=False),
        sa.Column("resolved_outcome", sa.String(), nullable=False),
        sa.Column("winning_token_id", sa.String(), nullable=True),
        sa.Column("closed_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("outcome_prices_raw", sa.String(), nullable=False),
        sa.Column("uma_resolution_statuses_raw", sa.String(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "resolved_outcome IN ('YES', 'NO', 'INVALID')",
            name="market_resolutions_outcome_enum",
        ),
        sa.CheckConstraint(
            "(resolved_outcome IN ('YES', 'NO') AND winning_token_id IS NOT NULL) "
            "OR (resolved_outcome = 'INVALID' AND winning_token_id IS NULL)",
            name="market_resolutions_winning_token_consistency",
        ),
        sa.PrimaryKeyConstraint("condition_id"),
    )
    op.create_index(
        "idx_market_resolutions_resolved_at",
        "market_resolutions",
        ["resolved_at"],
    )
    op.create_index(
        "idx_market_resolutions_outcome",
        "market_resolutions",
        ["resolved_outcome"],
    )


def downgrade() -> None:
    op.drop_index("idx_market_resolutions_outcome", table_name="market_resolutions")
    op.drop_index("idx_market_resolutions_resolved_at", table_name="market_resolutions")
    op.drop_table("market_resolutions")
