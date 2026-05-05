"""PnlSummary: snapshot agregado de PnL hipotético (Plano 5C)."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class PnlSummary:
    """Snapshot dos totais de PnL retornado por get_pnl_summary."""

    total_pnl_usdc: Decimal
    pnl_24h_usdc: Decimal
    winrate: float  # 0..1
    trades_resolved: int
    trades_pending: int
    # Analytics (Plano 5C v3 — métricas analíticas).
    # None quando dataset não suporta o cálculo (ex: <2 trades pra stddev).
    sharpe: float | None
    """AVG(return) / STDDEV(return) sobre trades resolvidos. Risk-free = 0."""
    max_drawdown_usdc: Decimal
    """Maior queda peak-to-trough no PnL cumulativo (ordem resolved_at). >= 0."""
    avg_holding_hours: float | None
    """Média de (resolved_at - decided_at) em horas, sobre trades resolvidos."""
