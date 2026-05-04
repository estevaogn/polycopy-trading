"""Unit tests do CLI backtest — testa parsing + formatação com inputs sintéticos."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from polycopy.scripts.backtest import (
    Trade,
    _format_json,
    _format_table,
    _parse_since,
)


def _trade(
    *,
    status: str = "win",
    side: str = "BUY",
    pnl: Decimal | None = Decimal("5"),
    wallet: str = "0xabc",
) -> Trade:
    return Trade(
        trade_event_id="00000000-0000-0000-0000-000000000001",
        wallet=wallet,
        condition_id="0x" + "ab" * 32,
        token_id="111",
        side=side,
        final_size_usdc=Decimal("10"),
        expected_avg_price=Decimal("0.5"),
        decided_at=datetime.now(tz=UTC),
        resolved_outcome="YES",
        pnl_usdc=pnl,
        status=status,
    )


def test_parse_since_days() -> None:
    assert _parse_since("7d") == timedelta(days=7)


def test_parse_since_hours() -> None:
    assert _parse_since("24h") == timedelta(hours=24)


def test_parse_since_weeks() -> None:
    assert _parse_since("2w") == timedelta(weeks=2)


def test_parse_since_invalid_raises() -> None:
    with pytest.raises(ValueError, match="invalid --since format"):
        _parse_since("xyz")


def test_format_table_empty_trades_shows_message() -> None:
    out = _format_table([], since=timedelta(days=7), by="none")
    assert "No trades found" in out


def test_format_table_all_wins() -> None:
    trades = [_trade(status="win", pnl=Decimal("5"))] * 3
    out = _format_table(trades, since=timedelta(days=7), by="none")
    assert "Trades total:  3" in out
    assert "win 3" in out
    assert "+15.00" in out  # 3 * 5
    assert "100.0%" in out


def test_format_table_mixed_outcomes() -> None:
    trades = [
        _trade(status="win", pnl=Decimal("5")),
        _trade(status="lose", pnl=Decimal("-10")),
        _trade(status="invalid", pnl=Decimal("-2")),
        _trade(status="pending", pnl=None),
        _trade(status="sell_excluded", pnl=None),
    ]
    out = _format_table(trades, since=timedelta(days=7), by="none")
    assert "Trades total:  5" in out
    assert "win 1" in out
    assert "lose 1" in out
    assert "invalid 1" in out
    assert "Pending:    1" in out
    assert "sell 1" in out
    assert "50.0%" in out  # 1 win / 2 decided


def test_format_table_by_wallet_groups() -> None:
    trades = [
        _trade(wallet="0xa", status="win", pnl=Decimal("5")),
        _trade(wallet="0xa", status="lose", pnl=Decimal("-3")),
        _trade(wallet="0xb", status="win", pnl=Decimal("10")),
    ]
    out = _format_table(trades, since=timedelta(days=7), by="wallet")
    assert "By wallet" in out
    assert "0xa" in out
    assert "0xb" in out


def test_format_json_serializes_trades() -> None:
    trades = [_trade(status="win", pnl=Decimal("5"))]
    out = _format_json(trades, since=timedelta(days=7), by="none")
    parsed = json.loads(out)
    assert len(parsed) == 1
    assert parsed[0]["status"] == "win"
    assert parsed[0]["pnl_usdc"] == "5"


def test_format_json_handles_null_pnl() -> None:
    trades = [_trade(status="pending", pnl=None)]
    out = _format_json(trades, since=timedelta(days=7), by="none")
    parsed = json.loads(out)
    assert parsed[0]["pnl_usdc"] is None
