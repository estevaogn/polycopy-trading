"""add wallet_realized_pnl + wallet_open_positions views

Permite ver no dashboard a performance HISTÓRICA REAL da wallet (independente
da nossa cópia hipotética). Computa PnL realizado a partir de wallet_trades
(price/size que a wallet pagou) JOIN com market_resolutions.

- `wallet_realized_pnl`: pnl_usdc por trade resolvido + status (similar à
  hypothetical_pnl mas usa wallet_trades em vez de order_executions).
- `wallet_open_positions`: net qty (sum BUY - sum SELL) por token em mercados
  ainda não resolvidos. Mostra "o que a wallet detém agora".

Revision ID: 0014
Revises: 0013
Create Date: 2026-05-05 00:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_REALIZED_VIEW = """
CREATE OR REPLACE VIEW wallet_realized_pnl AS
WITH base AS (
    SELECT
        wt.tx_hash,
        wt.log_index,
        wt.wallet,
        wt.condition_id,
        wt.token_id,
        wt.side,
        wt.price,
        wt.size_usdc,
        wt.occurred_at,
        mr.resolved_outcome,
        mr.winning_token_id,
        mr.resolved_at,
        CASE
            WHEN wt.price > 0 THEN wt.size_usdc / wt.price
            ELSE NULL
        END AS qty_tokens,
        CASE
            WHEN mr.resolved_outcome IS NULL THEN NULL
            WHEN mr.resolved_outcome = 'INVALID' THEN 0.5
            WHEN mr.winning_token_id = wt.token_id THEN 1.0
            ELSE 0.0
        END AS payout_per_token
    FROM wallet_trades wt
    LEFT JOIN market_resolutions mr ON wt.condition_id = mr.condition_id
)
SELECT
    tx_hash,
    log_index,
    wallet,
    condition_id,
    token_id,
    side,
    price,
    size_usdc,
    occurred_at,
    resolved_outcome,
    winning_token_id,
    resolved_at,
    qty_tokens,
    payout_per_token,
    CASE
        WHEN qty_tokens IS NULL OR payout_per_token IS NULL THEN NULL
        WHEN side = 'BUY' THEN qty_tokens * payout_per_token - size_usdc
        WHEN side = 'SELL' THEN size_usdc - qty_tokens * payout_per_token
        ELSE NULL
    END AS pnl_usdc,
    CASE
        WHEN resolved_outcome IS NULL THEN 'pending'
        WHEN qty_tokens IS NULL THEN 'no_price'
        WHEN resolved_outcome = 'INVALID' THEN 'invalid'
        WHEN side = 'BUY' AND winning_token_id = token_id THEN 'win'
        WHEN side = 'BUY' THEN 'lose'
        WHEN side = 'SELL' AND winning_token_id = token_id THEN 'lose'
        WHEN side = 'SELL' THEN 'win'
        ELSE 'pending'
    END AS status
FROM base;
"""


_POSITIONS_VIEW = """
CREATE OR REPLACE VIEW wallet_open_positions AS
SELECT
    wt.wallet,
    wt.condition_id,
    wt.token_id,
    SUM(
        CASE
            WHEN wt.side = 'BUY' AND wt.price > 0 THEN wt.size_usdc / wt.price
            WHEN wt.side = 'SELL' AND wt.price > 0 THEN -(wt.size_usdc / wt.price)
            ELSE 0
        END
    ) AS net_qty,
    SUM(
        CASE
            WHEN wt.side = 'BUY' THEN wt.size_usdc
            WHEN wt.side = 'SELL' THEN -wt.size_usdc
            ELSE 0
        END
    ) AS net_cost_usdc,
    MIN(wt.occurred_at) AS first_trade_at,
    MAX(wt.occurred_at) AS last_trade_at
FROM wallet_trades wt
LEFT JOIN market_resolutions mr ON wt.condition_id = mr.condition_id
WHERE mr.resolved_outcome IS NULL
GROUP BY wt.wallet, wt.condition_id, wt.token_id
HAVING SUM(
    CASE
        WHEN wt.side = 'BUY' AND wt.price > 0 THEN wt.size_usdc / wt.price
        WHEN wt.side = 'SELL' AND wt.price > 0 THEN -(wt.size_usdc / wt.price)
        ELSE 0
    END
) > 0;
"""


def upgrade() -> None:
    op.execute(_REALIZED_VIEW)
    op.execute(_POSITIONS_VIEW)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS wallet_open_positions;")
    op.execute("DROP VIEW IF EXISTS wallet_realized_pnl;")
