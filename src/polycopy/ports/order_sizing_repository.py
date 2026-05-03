"""OrderSizingRepository: contrato de persistência pra decisões de sizing."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.sizing import OrderSizing


class OrderSizingRepository(Protocol):
    """Persistência idempotente de decisões de sizing. Plano 2C."""

    async def insert(self, sizing: OrderSizing) -> bool:
        """Insere sizing; retorna True se nova, False se já existia.

        Idempotência via PK `trade_event_id`.
        """
        ...
