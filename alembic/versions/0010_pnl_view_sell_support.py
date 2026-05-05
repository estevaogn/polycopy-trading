"""hypothetical_pnl view with SELL side support (Plano 5C v2)

SELL trades agora têm pnl_usdc real (= size - qty*payout) em vez de NULL.
Status reflete sinal do PnL (não outcome do mercado), simétrico com BUY.

Revision ID: 0010
Revises: 0009
Create Date: 2026-05-05 00:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_VIEW_SQL_V2 = """
CREATE OR REPLACE VIEW hypothetical_pnl AS
WITH base AS (
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
            WHEN mr.resolved_outcome = 'INVALID' THEN 0.5
            WHEN mr.winning_token_id = oe.token_id THEN 1.0
            ELSE 0.0
        END AS payout_per_token
    FROM order_executions oe
    LEFT JOIN market_resolutions mr ON oe.condition_id = mr.condition_id
)
SELECT
    trade_event_id,
    wallet,
    condition_id,
    token_id,
    side,
    final_size_usdc,
    expected_avg_price,
    decided_at,
    mode,
    result,
    resolved_outcome,
    winning_token_id,
    resolved_at,
    qty_tokens,
    payout_per_token,
    CASE
        WHEN qty_tokens IS NULL OR payout_per_token IS NULL THEN NULL
        WHEN side = 'BUY' THEN qty_tokens * payout_per_token - final_size_usdc
        WHEN side = 'SELL' THEN final_size_usdc - qty_tokens * payout_per_token
        ELSE NULL
    END AS pnl_usdc,
    CASE
        WHEN resolved_outcome IS NULL THEN 'pending'
        WHEN qty_tokens IS NULL THEN 'no_expected_price'
        WHEN resolved_outcome = 'INVALID' THEN 'invalid'
        -- Resolução binária YES/NO: 'win' = pnl positivo, 'lose' = pnl negativo.
        -- BUY: win se token vencedor; SELL: win se token perdedor (sold what became worthless).
        WHEN side = 'BUY' AND winning_token_id = token_id THEN 'win'
        WHEN side = 'BUY' THEN 'lose'
        WHEN side = 'SELL' AND winning_token_id = token_id THEN 'lose'
        WHEN side = 'SELL' THEN 'win'
        ELSE 'pending'
    END AS status
FROM base;
"""


_VIEW_SQL_V1 = """
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
    op.execute(_VIEW_SQL_V2)


def downgrade() -> None:
    op.execute(_VIEW_SQL_V1)
