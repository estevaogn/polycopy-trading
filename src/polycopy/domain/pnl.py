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
