"""add order_sizings table

Revision ID: 0004
Revises: 0003
Create Date: 2026-05-02 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "order_sizings",
        sa.Column("trade_event_id", sa.Uuid(), nullable=False),
        sa.Column("wallet", sa.String(), nullable=False),
        sa.Column("condition_id", sa.String(), nullable=False),
        sa.Column("token_id", sa.String(), nullable=False),
        sa.Column("original_size_usdc", sa.Numeric(20, 6), nullable=False),
        sa.Column("final_size_usdc", sa.Numeric(20, 6), nullable=True),
        sa.Column("decision", sa.String(), nullable=False),
        sa.Column("reason", sa.String(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "decision IN ('sized', 'skipped')",
            name="order_sizings_decision_enum",
        ),
        sa.CheckConstraint(
            "(decision = 'sized' AND final_size_usdc IS NOT NULL AND reason IS NULL) "
            "OR (decision = 'skipped' AND final_size_usdc IS NULL AND reason IS NOT NULL)",
            name="order_sizings_consistency",
        ),
        sa.CheckConstraint(
            "original_size_usdc > 0 AND (final_size_usdc IS NULL OR final_size_usdc > 0)",
            name="order_sizings_size_positive",
        ),
        sa.PrimaryKeyConstraint("trade_event_id"),
    )
    op.create_index(
        "idx_order_sizings_wallet_decided_at",
        "order_sizings",
        ["wallet", "decided_at"],
    )
    op.create_index(
        "idx_order_sizings_skipped_decided_at",
        "order_sizings",
        ["decided_at"],
        postgresql_where=sa.text("decision = 'skipped'"),
    )


def downgrade() -> None:
    op.drop_index("idx_order_sizings_skipped_decided_at", table_name="order_sizings")
    op.drop_index("idx_order_sizings_wallet_decided_at", table_name="order_sizings")
    op.drop_table("order_sizings")
