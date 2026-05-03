"""OrderExecutionRepository: contrato de persistência pra decisões de execução."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.execution import OrderExecution


class OrderExecutionRepository(Protocol):
    """Persistência idempotente. Plano 3."""

    async def insert(self, execution: OrderExecution) -> bool:
        """Insere; True se nova, False se duplicate (PK trade_event_id)."""
        ...
