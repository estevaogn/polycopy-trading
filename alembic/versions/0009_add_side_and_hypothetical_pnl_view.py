"""add side column to order_executions and hypothetical_pnl view

Revision ID: 0009
Revises: 0008
Create Date: 2026-05-04 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_VIEW_SQL = """
CREATE OR REPLACE VIEW hypothetical_pnl AS
SELECT
    oe.trade_event_id,
    oe.wallet,
    oe.condition_id,
    oe.token_id,
    oe.side,
    oe.final_size_usdc,
    oe.expected_avg_price,
    oe.decided_at,
    oe.mode,
    oe.result,
    mr.resolved_outcome,
    mr.winning_token_id,
    mr.resolved_at,
    CASE
        WHEN oe.expected_avg_price IS NOT NULL AND oe.expected_avg_price > 0
        THEN oe.final_size_usdc / oe.expected_avg_price
        ELSE NULL
    END AS qty_tokens,
    CASE
        WHEN mr.resolved_outcome IS NULL THEN NULL
        WHEN oe.side = 'SELL' THEN NULL
        WHEN mr.resolved_outcome = 'INVALID' THEN 0.5
        WHEN mr.winning_token_id = oe.token_id THEN 1.0
        ELSE 0.0
    END AS payout_per_token,
    CASE
        WHEN mr.resolved_outcome IS NULL OR oe.side = 'SELL'
          OR oe.expected_avg_price IS NULL OR oe.expected_avg_price = 0
        THEN NULL
        WHEN mr.resolved_outcome = 'INVALID'
        THEN (oe.final_size_usdc / oe.expected_avg_price) * 0.5 - oe.final_size_usdc
        WHEN mr.winning_token_id = oe.token_id
        THEN (oe.final_size_usdc / oe.expected_avg_price) - oe.final_size_usdc
        ELSE -oe.final_size_usdc
    END AS pnl_usdc,
    CASE
        WHEN mr.resolved_outcome IS NULL THEN 'pending'
        WHEN oe.side = 'SELL' THEN 'sell_excluded'
        WHEN oe.expected_avg_price IS NULL OR oe.expected_avg_price = 0 THEN 'no_expected_price'
        WHEN mr.resolved_outcome = 'INVALID' THEN 'invalid'
        WHEN mr.winning_token_id = oe.token_id THEN 'win'
        ELSE 'lose'
    END AS status
FROM order_executions oe
LEFT JOIN market_resolutions mr ON oe.condition_id = mr.condition_id;
"""


def upgrade() -> None:
    op.add_column(
        "order_executions",
        sa.Column("side", sa.String(), nullable=False, server_default="BUY"),
    )
    op.create_check_constraint(
        "order_executions_side_enum",
        "order_executions",
        "side IN ('BUY', 'SELL')",
    )
    op.alter_column("order_executions", "side", server_default=None)

    op.execute(_VIEW_SQL)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS hypothetical_pnl;")
    op.drop_constraint("order_executions_side_enum", "order_executions", type_="check")
    op.drop_column("order_executions", "side")
