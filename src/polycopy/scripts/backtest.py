"""Backtest CLI — consulta hypothetical_pnl view e formata summary.

Uso:
    uv run python -m polycopy.scripts.backtest [--since 7d] [--by wallet|none] [--format table|json]

Args:
    --since: período (ex: 7d, 24h, 1w). Default: 7d.
    --by: agrupa por wallet ou none. Default: none.
    --format: table (default) ou json.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import text

from polycopy.config import Settings
from polycopy.infrastructure.persistence.database import (
    make_engine,
    make_session_factory,
)


@dataclass(frozen=True)
class Trade:
    trade_event_id: str
    wallet: str
    condition_id: str
    token_id: str
    side: str
    final_size_usdc: Decimal
    expected_avg_price: Decimal | None
    decided_at: datetime
    resolved_at: datetime | None
    resolved_outcome: str | None
    pnl_usdc: Decimal | None
    status: str


def _parse_since(value: str) -> timedelta:
    """Parse '7d', '24h', '1w' format into timedelta."""
    match = re.fullmatch(r"(\d+)([dhwm])", value)
    if not match:
        raise ValueError(f"invalid --since format: {value!r}, expected like '7d' or '24h'")
    n, unit = int(match.group(1)), match.group(2)
    return {
        "h": timedelta(hours=n),
        "d": timedelta(days=n),
        "w": timedelta(weeks=n),
        "m": timedelta(days=n * 30),  # approx
    }[unit]


async def _query_trades(*, since: timedelta) -> list[Trade]:
    settings = Settings()
    engine = make_engine(settings)
    session_factory = make_session_factory(engine)
    cutoff = datetime.now(tz=UTC) - since

    async with session_factory() as session:
        result = await session.execute(
            text(
                """
                SELECT trade_event_id, wallet, condition_id, token_id, side,
                       final_size_usdc, expected_avg_price, decided_at, resolved_at,
                       resolved_outcome, pnl_usdc, status
                FROM hypothetical_pnl
                WHERE decided_at > :cutoff
                ORDER BY decided_at DESC
                """
            ),
            {"cutoff": cutoff},
        )
        rows = result.all()

    await engine.dispose()
    return [
        Trade(
            trade_event_id=str(r.trade_event_id),
            wallet=r.wallet,
            condition_id=r.condition_id,
            token_id=r.token_id,
            side=r.side,
            final_size_usdc=r.final_size_usdc,
            expected_avg_price=r.expected_avg_price,
            decided_at=r.decided_at,
            resolved_at=r.resolved_at,
            resolved_outcome=r.resolved_outcome,
            pnl_usdc=r.pnl_usdc,
            status=r.status,
        )
        for r in rows
    ]


@dataclass(frozen=True)
class _Analytics:
    sharpe: float | None
    max_drawdown_usdc: Decimal
    avg_holding_hours: float | None


def _compute_analytics(trades: list[Trade]) -> _Analytics:
    """Computa Sharpe, max drawdown e avg holding sobre trades resolvidos.

    Sharpe: mean/stdev de returns (pnl/size). None se <2 trades ou stdev=0.
    Max DD: maior queda peak-to-trough do PnL cumulativo, ordem resolved_at.
    Avg holding: média de (resolved_at - decided_at) em horas.
    """
    resolved = [
        t for t in trades if t.status in ("win", "lose", "invalid") and t.pnl_usdc is not None
    ]

    sharpe: float | None = None
    if len(resolved) >= 2:
        returns = [
            float(t.pnl_usdc / t.final_size_usdc)
            for t in resolved
            if t.pnl_usdc is not None and t.final_size_usdc > 0
        ]
        if len(returns) >= 2:
            stdev = statistics.stdev(returns)
            if stdev > 0:
                sharpe = statistics.mean(returns) / stdev

    max_dd = Decimal(0)
    sorted_by_time = sorted(
        (t for t in resolved if t.resolved_at is not None),
        key=lambda t: (t.resolved_at, t.trade_event_id),
    )
    cum = Decimal(0)
    peak = Decimal(0)
    for t in sorted_by_time:
        cum += t.pnl_usdc  # type: ignore[operator]
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

    holdings = [
        (t.resolved_at - t.decided_at).total_seconds() / 3600.0
        for t in resolved
        if t.resolved_at is not None
    ]
    avg_holding = sum(holdings) / len(holdings) if holdings else None

    return _Analytics(sharpe=sharpe, max_drawdown_usdc=max_dd, avg_holding_hours=avg_holding)


def _format_table(trades: list[Trade], *, since: timedelta, by: str) -> str:
    """Formata summary + tabela top trades em texto plano."""
    if not trades:
        return f"=== Backtest Summary ===\nPeriod: últimos {since}\nNo trades found in period.\n"

    n = len(trades)
    by_status: dict[str, int] = defaultdict(int)
    for t in trades:
        by_status[t.status] += 1

    resolved_pnls = [t.pnl_usdc for t in trades if t.pnl_usdc is not None]
    total_pnl = sum(resolved_pnls, Decimal(0))

    win_count = by_status.get("win", 0)
    lose_count = by_status.get("lose", 0)
    decided = win_count + lose_count
    winrate = (win_count / decided * 100) if decided > 0 else 0.0

    a = _compute_analytics(trades)
    sharpe_str = f"{a.sharpe:+.3f}" if a.sharpe is not None else "N/A"
    hold_str = f"{a.avg_holding_hours:.1f}h" if a.avg_holding_hours is not None else "N/A"

    lines = [
        "=== Backtest Summary ===",
        f"Period:        últimos {since}",
        f"Trades total:  {n}",
        f"  - Resolved:  "
        f"{by_status.get('win', 0) + by_status.get('lose', 0) + by_status.get('invalid', 0)} "
        f"(win {win_count}, lose {lose_count}, invalid {by_status.get('invalid', 0)})",
        f"  - Pending:    {by_status.get('pending', 0)}",
        f"  - Excluded:   {by_status.get('no_expected_price', 0)} "
        f"(no_price {by_status.get('no_expected_price', 0)})",
        "",
        f"PnL hipotético:  ${total_pnl:+.2f} USDC",
        f"Winrate:         {winrate:.1f}% ({win_count} / {decided} resolved excluindo invalid)",
        f"Sharpe:          {sharpe_str}",
        f"Max drawdown:    ${a.max_drawdown_usdc:.2f} USDC",
        f"Avg holding:     {hold_str}",
        "",
    ]

    if by == "wallet":
        lines.append("=== By wallet ===")
        by_wallet: dict[str, list[Trade]] = defaultdict(list)
        for t in trades:
            by_wallet[t.wallet].append(t)
        for wallet, wt in by_wallet.items():
            wpnl = sum((t.pnl_usdc for t in wt if t.pnl_usdc is not None), Decimal(0))
            ww = sum(1 for t in wt if t.status == "win")
            wd = sum(1 for t in wt if t.status in ("win", "lose"))
            wrate = (ww / wd * 100) if wd > 0 else 0.0
            lines.append(
                f"  {wallet[:10]}...:  ${wpnl:+.2f}  ({ww} wins / {wd} decided = {wrate:.1f}%)"
            )
        lines.append("")

    lines.append("=== Top 10 trades ===")
    lines.append(
        f"{'wallet':<14} {'side':<5} {'size':>8} {'expected':>10} {'status':<18} {'pnl':>10}"
    )
    lines.append("-" * 70)
    for t in trades[:10]:
        wallet_short = t.wallet[:12] + ".."
        size_str = f"{t.final_size_usdc:.2f}"
        exp_str = f"{t.expected_avg_price:.4f}" if t.expected_avg_price else "N/A"
        pnl_str = f"{t.pnl_usdc:+.2f}" if t.pnl_usdc is not None else "N/A"
        lines.append(
            f"{wallet_short:<14} {t.side:<5} {size_str:>8} {exp_str:>10} "
            f"{t.status:<18} {pnl_str:>10}"
        )

    return "\n".join(lines)


def _format_json(trades: list[Trade], *, since: timedelta, by: str) -> str:  # noqa: ARG001
    return json.dumps(
        [
            {
                "trade_event_id": t.trade_event_id,
                "wallet": t.wallet,
                "condition_id": t.condition_id,
                "token_id": t.token_id,
                "side": t.side,
                "final_size_usdc": str(t.final_size_usdc),
                "expected_avg_price": (str(t.expected_avg_price) if t.expected_avg_price else None),
                "decided_at": t.decided_at.isoformat(),
                "resolved_outcome": t.resolved_outcome,
                "pnl_usdc": str(t.pnl_usdc) if t.pnl_usdc is not None else None,
                "status": t.status,
            }
            for t in trades
        ],
        indent=2,
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Polycopy backtest CLI")
    p.add_argument("--since", default="7d", help="Período (ex: 7d, 24h, 1w). Default: 7d.")
    p.add_argument("--by", default="none", choices=["none", "wallet"], help="Group-by")
    p.add_argument("--format", default="table", choices=["table", "json"], dest="format_")
    return p


async def main_async(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        since = _parse_since(args.since)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    trades = await _query_trades(since=since)

    if args.format_ == "json":
        print(_format_json(trades, since=since, by=args.by))
    else:
        print(_format_table(trades, since=since, by=args.by))
    return 0


def main() -> None:
    sys.exit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()
