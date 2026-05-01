"""Unit tests for AgentBase."""

from __future__ import annotations

import asyncio

import pytest

from polycopy.agents._base import AgentBase


class _CountingAgent(AgentBase):
    name = "counting"

    def __init__(self, *, stopping: asyncio.Event, interval_s: float) -> None:
        super().__init__(stopping=stopping, interval_s=interval_s)
        self.count = 0

    async def run_once(self) -> None:
        self.count += 1


class _FailingAgent(AgentBase):
    name = "failing"

    async def run_once(self) -> None:
        raise RuntimeError("boom")


async def test_run_loop_invokes_run_once_until_stopped() -> None:
    stopping = asyncio.Event()
    agent = _CountingAgent(stopping=stopping, interval_s=0.01)

    task = asyncio.create_task(agent.run())
    await asyncio.sleep(0.05)
    stopping.set()
    await task

    assert agent.count >= 2


async def test_run_loop_exits_immediately_if_stopping_already_set() -> None:
    stopping = asyncio.Event()
    stopping.set()
    agent = _CountingAgent(stopping=stopping, interval_s=0.01)
    await agent.run()
    assert agent.count == 0


async def test_run_loop_propagates_run_once_exception_after_stopping() -> None:
    stopping = asyncio.Event()
    agent = _FailingAgent(stopping=stopping, interval_s=0.01)
    with pytest.raises(RuntimeError, match="boom"):
        await agent.run()


async def test_setup_signal_handlers_sets_event_on_signal() -> None:
    """Smoke test: o helper registra handlers e o event é setado quando SIGTERM dispara."""
    import signal

    from polycopy.agents._base import setup_signal_handlers

    stopping = asyncio.Event()
    setup_signal_handlers(stopping)

    loop = asyncio.get_running_loop()
    loop.call_soon(lambda: signal.raise_signal(signal.SIGTERM))
    try:
        await asyncio.wait_for(stopping.wait(), timeout=0.5)
    finally:
        # Limpa handlers pra não vazar pra outros testes
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
    assert stopping.is_set()
