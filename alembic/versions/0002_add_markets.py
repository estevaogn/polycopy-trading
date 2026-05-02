"""add markets table

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-02 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "markets",
        sa.Column("token_id", sa.String(), nullable=False),
        sa.Column("condition_id", sa.String(), nullable=False),
        sa.Column("question", sa.String(), nullable=False),
        sa.Column("slug", sa.String(), nullable=True),
        sa.Column("outcome", sa.String(), nullable=False),
        sa.Column("end_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column(
            "is_archived",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("volume_24h_usdc", sa.Numeric(20, 6), nullable=True),
        sa.Column("liquidity_usdc", sa.Numeric(20, 6), nullable=True),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("outcome IN ('Yes', 'No')", name="markets_outcome_enum"),
        sa.CheckConstraint(
            "NOT (is_active AND is_archived)", name="markets_active_archived_exclusive"
        ),
        sa.PrimaryKeyConstraint("token_id"),
    )
    op.create_index("idx_markets_condition_id", "markets", ["condition_id"])
    op.create_index(
        "idx_markets_active_end_date",
        "markets",
        ["end_date"],
        postgresql_where=sa.text("is_active = true"),
    )
    op.create_index(
        "idx_markets_volume_24h",
        "markets",
        [sa.text("volume_24h_usdc DESC NULLS LAST")],
        postgresql_where=sa.text("is_active = true"),
        postgresql_using="btree",
    )


def downgrade() -> None:
    op.drop_index("idx_markets_volume_24h", table_name="markets")
    op.drop_index("idx_markets_active_end_date", table_name="markets")
    op.drop_index("idx_markets_condition_id", table_name="markets")
    op.drop_table("markets")
