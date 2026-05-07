"""SqlAlchemyMarketResolutionRepository: persistência idempotente de resoluções."""

from __future__ import annotations

from decimal import Decimal
from typing import cast

from sqlalchemy import CursorResult, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from polycopy.domain.pnl import PnlSummary
from polycopy.domain.resolution import MarketResolution
from polycopy.infrastructure.persistence.models import (
    MarketResolutionRow,
    WalletTradeRow,
)


class SqlAlchemyMarketResolutionRepository:
    """Persistência idempotente. PK = condition_id (1 row por mercado).

    market_resolutions é puramente append-only — sem UPDATEs.
    `insert` retorna False se já existe (PK conflict).
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(self, resolution: MarketResolution) -> bool:
        """Insere; True se nova, False se já existia (PK conflict)."""
        stmt = (
            pg_insert(MarketResolutionRow)
            .values(
                condition_id=resolution.condition_id,
                resolved_outcome=resolution.resolved_outcome.value,
                winning_token_id=resolution.winning_token_id,
                closed_time=resolution.closed_time,
                resolved_at=resolution.resolved_at,
                outcome_prices_raw=resolution.outcome_prices_raw,
                uma_resolution_statuses_raw=resolution.uma_resolution_statuses_raw,
            )
            .on_conflict_do_nothing(index_elements=["condition_id"])
        )
        result = cast(CursorResult[None], await self._session.execute(stmt))
        await self._session.flush()
        return result.rowcount == 1

    async def get_unresolved_condition_ids(self, *, limit: int) -> list[str]:
        """LEFT JOIN wallet_trades vs market_resolutions WHERE resolution IS NULL.

        Retorna ordenado por trade mais antigo primeiro (MIN(occurred_at) ASC).
        Mercados antigos têm maior probabilidade de já estarem resolvidos na
        Gamma; processá-los primeiro acelera a catalogação. Sem ORDER BY o
        Postgres pode retornar os mesmos N rows a cada cycle, deixando o
        resolver \"travado\" verificando o mesmo conjunto.
        """
        stmt = (
            select(WalletTradeRow.condition_id)
            .outerjoin(
                MarketResolutionRow,
                MarketResolutionRow.condition_id == WalletTradeRow.condition_id,
            )
            .where(MarketResolutionRow.condition_id.is_(None))
            .group_by(WalletTradeRow.condition_id)
            .order_by(func.min(WalletTradeRow.occurred_at).asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return [row[0] for row in result.all()]

    async def get_pnl_summary(self) -> PnlSummary:
        """Query agregada na view hypothetical_pnl."""
        result = await self._session.execute(
            text("""
                SELECT
                    COALESCE(SUM(pnl_usdc), 0) as total_pnl,
                    COALESCE(SUM(pnl_usdc) FILTER (
                        WHERE decided_at > now() - interval '24 hours'
                    ), 0) as pnl_24h,
                    COUNT(*) FILTER (WHERE status IN ('win','lose','invalid')) as resolved,
                    COUNT(*) FILTER (WHERE status = 'pending') as pending,
                    COUNT(*) FILTER (WHERE status = 'win') as wins,
                    COUNT(*) FILTER (WHERE status IN ('win','lose')) as decided,
                    -- Sharpe: avg(pnl/size) / stddev(pnl/size). Risk-free=0.
                    -- STDDEV_SAMP requer >= 2 trades; retorna NULL caso contrário.
                    -- NULLIF protege divisão por zero quando todos returns são iguais.
                    AVG(pnl_usdc / NULLIF(final_size_usdc, 0))
                        FILTER (WHERE status IN ('win','lose','invalid')) as ret_mean,
                    NULLIF(
                        STDDEV_SAMP(pnl_usdc / NULLIF(final_size_usdc, 0))
                            FILTER (WHERE status IN ('win','lose','invalid')),
                        0
                    ) as ret_stddev,
                    -- Avg holding: tempo entre decisão e resolução do mercado.
                    AVG(EXTRACT(EPOCH FROM (resolved_at - decided_at)) / 3600.0)
                        FILTER (WHERE status IN ('win','lose','invalid')) as avg_hold_h
                FROM hypothetical_pnl
            """)
        )
        row = result.one()
        winrate = float(row.wins) / float(row.decided) if row.decided > 0 else 0.0

        sharpe: float | None
        if row.ret_mean is not None and row.ret_stddev is not None:
            sharpe = float(row.ret_mean) / float(row.ret_stddev)
        else:
            sharpe = None

        avg_hold = float(row.avg_hold_h) if row.avg_hold_h is not None else None

        max_drawdown = await self._compute_max_drawdown()

        return PnlSummary(
            total_pnl_usdc=Decimal(str(row.total_pnl)),
            pnl_24h_usdc=Decimal(str(row.pnl_24h)),
            winrate=winrate,
            trades_resolved=int(row.resolved),
            trades_pending=int(row.pending),
            sharpe=sharpe,
            max_drawdown_usdc=max_drawdown,
            avg_holding_hours=avg_hold,
        )

    async def _compute_max_drawdown(self) -> Decimal:
        """Maior queda peak-to-trough no PnL cumulativo, ordem resolved_at.

        Retorna 0 se não houver trades resolvidos (sem séries pra computar).
        """
        result = await self._session.execute(
            text("""
                WITH resolved AS (
                    SELECT pnl_usdc, resolved_at, trade_event_id
                    FROM hypothetical_pnl
                    WHERE status IN ('win','lose','invalid')
                      AND pnl_usdc IS NOT NULL
                ),
                cum AS (
                    SELECT
                        resolved_at,
                        trade_event_id,
                        SUM(pnl_usdc) OVER (
                            ORDER BY resolved_at, trade_event_id
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS cum_pnl
                    FROM resolved
                ),
                peaks AS (
                    SELECT
                        cum_pnl,
                        MAX(cum_pnl) OVER (
                            ORDER BY resolved_at, trade_event_id
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS running_peak
                    FROM cum
                )
                SELECT COALESCE(MAX(running_peak - cum_pnl), 0) as max_dd FROM peaks
            """)
        )
        return Decimal(str(result.scalar_one()))
