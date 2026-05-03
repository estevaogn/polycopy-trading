"""OrderExecutor: strategy pattern pra execução de ordens (real ou dry-run)."""

from __future__ import annotations

from decimal import Decimal
from typing import Protocol

from polycopy.domain.execution import ExecutionResult
from polycopy.domain.models import Trade


class OrderExecutor(Protocol):
    """Strategy pra executar uma ordem. Implementações: DryRunExecutor (MVP),
    Web3CLOBExecutor (Fase 4)."""

    async def execute(self, trade: Trade, final_size_usdc: Decimal) -> ExecutionResult:
        """Executa (ou simula) a ordem. Retorna ExecutionResult com mode + outcome."""
        ...
