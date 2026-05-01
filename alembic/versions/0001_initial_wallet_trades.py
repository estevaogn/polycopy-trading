"""initial wallet_trades table

Revision ID: 0001
Revises:
Create Date: 2026-05-01 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "wallet_trades",
        sa.Column("tx_hash", sa.String(), nullable=False),
        sa.Column("log_index", sa.Integer(), nullable=False),
        sa.Column("wallet", sa.String(), nullable=False),
        sa.Column("condition_id", sa.String(), nullable=False),
        sa.Column("token_id", sa.String(), nullable=False),
        sa.Column("side", sa.String(), nullable=False),
        sa.Column("price", sa.Numeric(8, 4), nullable=False),
        sa.Column("size_usdc", sa.Numeric(28, 6), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "inserted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("log_index >= 0", name="wallet_trades_log_index_nonneg"),
        sa.CheckConstraint("side IN ('BUY', 'SELL')", name="wallet_trades_side_enum"),
        sa.CheckConstraint("price >= 0 AND price <= 1", name="wallet_trades_price_range"),
        sa.PrimaryKeyConstraint("tx_hash", "log_index"),
    )
    op.create_index(
        "wallet_trades_wallet_occurred_at_idx",
        "wallet_trades",
        ["wallet", "occurred_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("wallet_trades_wallet_occurred_at_idx", table_name="wallet_trades")
    op.drop_table("wallet_trades")
