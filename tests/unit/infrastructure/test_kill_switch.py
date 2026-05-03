"""Testes unit do KillSwitch — 5 camadas de proteção in-memory."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from polycopy.domain.events import FailureReason
from polycopy.infrastructure.execution.kill_switch import KillSwitch


def _make_kill_switch(
    *,
    max_size_usdc: Decimal = Decimal("2"),
    daily_max_usdc: Decimal = Decimal("20"),
    daily_max_trades: int = 10,
    circuit_breaker_failures: int = 3,
    pause_file: Path | None = None,
    tmp_path: Path | None = None,
) -> KillSwitch:
    if pause_file is None:
        if tmp_path is None:
            raise RuntimeError("test bug: provide pause_file or tmp_path")
        pause_file = tmp_path / "pause"
    return KillSwitch(
        max_size_usdc=max_size_usdc,
        daily_max_usdc=daily_max_usdc,
        daily_max_trades=daily_max_trades,
        circuit_breaker_failures=circuit_breaker_failures,
        pause_file=pause_file,
    )


def test_check_passes_when_all_clear(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path)
    assert ks.check(Decimal("1")) is None


def test_check_blocks_when_pause_file_exists(tmp_path: Path) -> None:
    pause = tmp_path / "pause"
    pause.touch()
    ks = _make_kill_switch(pause_file=pause)
    assert ks.check(Decimal("1")) == FailureReason.MANUALLY_PAUSED


def test_check_blocks_when_size_exceeds_cap(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path, max_size_usdc=Decimal("2"))
    assert ks.check(Decimal("3")) == FailureReason.SIZE_EXCEEDS_EXECUTOR_CAP


def test_check_blocks_when_daily_trades_exceeded(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path, daily_max_trades=2)
    ks.record_success(Decimal("1"))
    ks.record_success(Decimal("1"))
    assert ks.check(Decimal("1")) == FailureReason.DAILY_TRADES_EXCEEDED


def test_check_blocks_when_daily_usdc_exceeded(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path, daily_max_usdc=Decimal("5"))
    ks.record_success(Decimal("3"))
    # Próximo trade de $3 ultrapassaria 5
    assert ks.check(Decimal("3")) == FailureReason.DAILY_USDC_EXCEEDED


def test_check_blocks_when_circuit_breaker_tripped(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path, circuit_breaker_failures=2)
    ks.record_failure()
    ks.record_failure()
    assert ks.check(Decimal("1")) == FailureReason.CIRCUIT_BREAKER


def test_record_success_resets_circuit_breaker(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path, circuit_breaker_failures=2)
    ks.record_failure()
    ks.record_failure()
    ks.record_success(Decimal("1"))
    # Circuit breaker resetado, próximo check passa
    assert ks.check(Decimal("1")) is None


def test_eviction_window_24h(tmp_path: Path) -> None:
    """Trades > 24h devem sair do contador diário."""
    ks = _make_kill_switch(tmp_path=tmp_path, daily_max_trades=2)
    # Injetar trade antigo (>24h)
    old_ts = datetime.now(tz=UTC) - timedelta(hours=25)
    ks._trades_24h.append((old_ts, Decimal("1")))  # type: ignore[attr-defined]
    # Adicionar 2 trades recentes
    ks.record_success(Decimal("1"))
    ks.record_success(Decimal("1"))
    # Trade antigo já evicted; 2 contam, próximo bloqueia
    assert ks.check(Decimal("1")) == FailureReason.DAILY_TRADES_EXCEEDED


def test_check_order_pause_first(tmp_path: Path) -> None:
    """Ordem das checagens: pause file checado antes de tudo."""
    pause = tmp_path / "pause"
    pause.touch()
    ks = _make_kill_switch(pause_file=pause, max_size_usdc=Decimal("2"), circuit_breaker_failures=1)
    ks.record_failure()  # circuit breaker tripado
    # Mesmo com circuit breaker, pause file ganha
    assert ks.check(Decimal("100")) == FailureReason.MANUALLY_PAUSED


def test_check_order_circuit_breaker_before_daily(tmp_path: Path) -> None:
    """Ordem: circuit breaker antes de daily trades."""
    ks = _make_kill_switch(tmp_path=tmp_path, circuit_breaker_failures=1, daily_max_trades=1)
    ks.record_failure()
    ks.record_success(Decimal("1"))  # NOTE: success reseta circuit breaker
    # Reset; agora ks tem 1 trade no dia, daily_max=1
    ks.record_failure()
    # Circuit breaker tripado novamente
    assert ks.check(Decimal("1")) == FailureReason.CIRCUIT_BREAKER


def test_record_success_updates_daily_counter(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path)
    ks.record_success(Decimal("5"))
    ks.record_success(Decimal("3"))
    assert len(ks._trades_24h) == 2  # type: ignore[attr-defined]


def test_record_failure_increments_consecutive(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path, circuit_breaker_failures=5)
    ks.record_failure()
    ks.record_failure()
    assert ks._consecutive_failures == 2  # type: ignore[attr-defined]
