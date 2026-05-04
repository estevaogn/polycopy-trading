"""OrderExecution: value object interno de uma decisão de execução persistida.
ExecutionResult: dataclass intermediário retornado por OrderExecutor.execute().
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from polycopy.domain.events import ExecutionMode, FailureReason


@dataclass(frozen=True)
class ExecutionResult:
    """Retorno de OrderExecutor.execute(). Convertido em OrderExecution pelo agente."""

    mode: ExecutionMode
    success: bool
    tx_hash: str | None = None
    gas_wei: int | None = None
    failure_reason: FailureReason | None = None
    error_message: str | None = None
    expected_avg_price: Decimal | None = None  # NOVO — Plano 5B


@dataclass(frozen=True)
class OrderExecution:
    """Snapshot imutável de uma decisão de execução.

    Persistido em order_executions; PK = trade_event_id.

    Invariantes:
    - mode == REAL ↔ result ∈ {executed, failed}
    - mode == DRY_RUN ↔ result IN {"dry_run", "failed"}
        (failed só quando executor stub lança exception)
    - result == "executed" → tx_hash IS NOT NULL
    - result == "failed" → failure_reason IS NOT NULL AND error_message IS NOT NULL
    - result == "dry_run" → tx_hash IS NULL AND gas_wei IS NULL AND failure_reason IS NULL
    """

    trade_event_id: UUID
    wallet: str
    condition_id: str
    token_id: str
    side: Literal["BUY", "SELL"]  # Plano 5C
    final_size_usdc: Decimal
    mode: ExecutionMode
    result: Literal["executed", "failed", "dry_run"]
    tx_hash: str | None
    gas_wei: int | None
    failure_reason: FailureReason | None
    error_message: str | None
    decided_at: datetime
    expected_avg_price: Decimal | None = None  # NOVO — Plano 5B

    def __post_init__(self) -> None:
        if self.mode == ExecutionMode.REAL:
            if self.result not in ("executed", "failed"):
                raise ValueError("real mode must produce executed or failed")
        else:  # DRY_RUN
            if self.result not in ("dry_run", "failed"):
                raise ValueError("dry_run mode must produce result='dry_run' or 'failed'")

        if self.result == "executed" and self.tx_hash is None:
            raise ValueError("executed result must have tx_hash")
        if self.result == "failed":
            if self.failure_reason is None:
                raise ValueError("failed result must have failure_reason")
            if self.error_message is None:
                raise ValueError("failed result must have error_message")
        if self.result == "dry_run":
            if self.tx_hash is not None:
                raise ValueError("dry_run must have tx_hash=None")
            if self.gas_wei is not None:
                raise ValueError("dry_run must have gas_wei=None")
            if self.failure_reason is not None:
                raise ValueError("dry_run must have failure_reason=None")
        if self.final_size_usdc <= 0:
            raise ValueError("final_size_usdc must be positive")
        if self.gas_wei is not None and self.gas_wei < 0:
            raise ValueError("gas_wei must be non-negative")
        if self.decided_at.tzinfo is None:
            raise ValueError("decided_at must be timezone-aware")
