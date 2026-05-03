"""KillSwitch: 5 camadas de proteção in-memory pra Web3CLOBExecutor.

Pausa file checado a cada execute (controle externo via filesystem).
Daily caps via deque com janela rolante 24h. Circuit breaker conta
falhas consecutivas, reseta em sucesso.

State é in-memory — restart do agente reseta. Operadores que reiniciam
container "burlam" daily caps. Mitigação aceita pra MVP.
"""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from polycopy.domain.events import FailureReason


class KillSwitch:
    """5 camadas de proteção in-memory.

    Ordem das checagens (fail-fast):
    1. Pause file (controle operacional zero-restart)
    2. Circuit breaker (N falhas consecutivas)
    3. Daily trades cap (rolling 24h)
    4. Daily USDC cap (rolling 24h)
    5. Per-trade size cap

    State:
    - _trades_24h: deque[(timestamp, size_usdc)] com janela rolante 24h
    - _consecutive_failures: int, reset em record_success()
    """

    def __init__(
        self,
        *,
        max_size_usdc: Decimal,
        daily_max_usdc: Decimal,
        daily_max_trades: int,
        circuit_breaker_failures: int,
        pause_file: Path,
    ) -> None:
        self._max_size_usdc = max_size_usdc
        self._daily_max_usdc = daily_max_usdc
        self._daily_max_trades = daily_max_trades
        self._circuit_breaker_failures = circuit_breaker_failures
        self._pause_file = pause_file
        self._trades_24h: deque[tuple[datetime, Decimal]] = deque()
        self._consecutive_failures: int = 0

    def check(self, size_usdc: Decimal) -> FailureReason | None:
        """Retorna razão de bloqueio ou None se passou todas as camadas."""
        self._evict_old_trades()

        if self._pause_file.exists():
            return FailureReason.MANUALLY_PAUSED

        if self._consecutive_failures >= self._circuit_breaker_failures:
            return FailureReason.CIRCUIT_BREAKER

        if len(self._trades_24h) >= self._daily_max_trades:
            return FailureReason.DAILY_TRADES_EXCEEDED

        sum_24h = sum((s for _, s in self._trades_24h), Decimal("0"))
        if sum_24h + size_usdc > self._daily_max_usdc:
            return FailureReason.DAILY_USDC_EXCEEDED

        if size_usdc > self._max_size_usdc:
            return FailureReason.SIZE_EXCEEDS_EXECUTOR_CAP

        return None

    def record_success(self, size_usdc: Decimal) -> None:
        """Registra trade bem-sucedido. Reseta circuit breaker."""
        self._trades_24h.append((datetime.now(tz=UTC), size_usdc))
        self._consecutive_failures = 0

    def record_failure(self) -> None:
        """Incrementa contador de falhas consecutivas (circuit breaker)."""
        self._consecutive_failures += 1

    @property
    def consecutive_failures(self) -> int:
        """Exposto pra métrica Gauge."""
        return self._consecutive_failures

    def _evict_old_trades(self) -> None:
        """Remove trades > 24h da deque (popleft repeatedly)."""
        cutoff = datetime.now(tz=UTC) - timedelta(hours=24)
        while self._trades_24h and self._trades_24h[0][0] < cutoff:
            self._trades_24h.popleft()
