"""DryRunExecutor: implementação MVP de OrderExecutor.

Sempre retorna ExecutionResult(mode=DRY_RUN, success=True). Não chama
blockchain. Real-mode (Web3CLOBExecutor) entra na Fase 4.
"""

from __future__ import annotations

from decimal import Decimal

from polycopy.domain.events import ExecutionMode
from polycopy.domain.execution import ExecutionResult
from polycopy.domain.models import Trade


class DryRunExecutor:
    """Executor que apenas simula — não chama blockchain."""

    async def execute(
        self,
        trade: Trade,  # noqa: ARG002 — assinatura imposta pelo OrderExecutor Protocol
        final_size_usdc: Decimal,  # noqa: ARG002 — idem
    ) -> ExecutionResult:
        return ExecutionResult(
            mode=ExecutionMode.DRY_RUN,
            success=True,
            tx_hash=None,
            gas_wei=None,
            failure_reason=None,
            error_message=None,
        )
