"""Base class para agents: loop assíncrono com graceful shutdown e heartbeat."""

from __future__ import annotations

import asyncio
import signal
from abc import ABC, abstractmethod
from typing import ClassVar

from polycopy.infrastructure.observability.logging import get_logger


class AgentBase(ABC):
    """Loop padrão: roda `run_once()` até `stopping` ser setado.

    Subclasses devem definir `name` (ClassVar) e implementar `run_once()`.
    """

    name: ClassVar[str] = "agent"

    def __init__(
        self,
        *,
        stopping: asyncio.Event,
        interval_s: float,
        heartbeat_every_n: int = 10,
    ) -> None:
        self._stopping = stopping
        self._interval_s = interval_s
        self._heartbeat_every_n = heartbeat_every_n
        self._log = get_logger(self.name)

    @abstractmethod
    async def run_once(self) -> None:
        """Uma iteração de trabalho. Subclasses implementam."""

    async def run(self) -> None:
        """Loop principal. Sai quando `stopping` é setado."""
        iteration = 0
        self._log.info("agent_started", interval_s=self._interval_s)
        try:
            while not self._stopping.is_set():
                await self.run_once()
                iteration += 1
                if iteration % self._heartbeat_every_n == 0:
                    self._log.info("agent_heartbeat", iteration=iteration)
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=self._interval_s)
                except TimeoutError:
                    continue
        finally:
            self._log.info("agent_stopped", iterations=iteration)


def setup_signal_handlers(stopping: asyncio.Event) -> None:
    """Registra handlers SIGTERM/SIGINT que setam `stopping`. Chamar no main()."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stopping.set)
