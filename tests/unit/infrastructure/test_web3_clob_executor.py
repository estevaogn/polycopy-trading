"""Testes unit do Web3CLOBExecutor — CLOB client mockado, sem rede."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from prometheus_client import CollectorRegistry

from polycopy.config import Settings
from polycopy.domain.events import ExecutionMode, FailureReason
from polycopy.domain.market import OrderBook, OrderBookLevel
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.execution.kill_switch import KillSwitch
from polycopy.infrastructure.execution.web3_clob_executor import (
    Web3CLOBExecutor,
    _classify_clob_error,
    build_clob_client,
    verify_allowance,
)
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics


class _StubCLOB:
    """Stub que satisfaz PolymarketClobPort para testes do Web3CLOBExecutor."""

    def __init__(
        self,
        book: OrderBook | None = None,
        raise_exc: Exception | None = None,
    ) -> None:
        self._book = book
        self._raise = raise_exc

    async def get_book(self, token_id: TokenId) -> OrderBook:
        if self._raise is not None:
            raise self._raise
        if self._book is None:
            raise RuntimeError("book not configured")
        return self._book


def _level(price: str, size: str) -> OrderBookLevel:
    return OrderBookLevel(
        price=Price(value=Decimal(price)),
        size=Money(amount=Decimal(size)),
    )


def _book(
    *,
    asks: list[OrderBookLevel] | None = None,
    bids: list[OrderBookLevel] | None = None,
) -> OrderBook:
    return OrderBook(
        token_id=TokenId(value="42"),
        asks=asks or [],
        bids=bids or [],
        captured_at=datetime.now(tz=UTC),
    )


def _trivial_book() -> OrderBook:
    """Book trivial com liquidez suficiente para qualquer trade de teste."""
    return _book(asks=[_level("0.5", "1000")], bids=[_level("0.5", "1000")])


def _trade() -> Trade:
    return Trade(
        tx_hash="0x" + "ab" * 32,
        log_index=0,
        wallet=WalletAddress(value="0x" + "1" * 40),
        condition_id=ConditionId(value="0x" + "cd" * 32),
        token_id=TokenId(value="42"),
        side=Side.BUY,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=UTC),
    )


@pytest.fixture
def metrics() -> Metrics:
    return make_metrics(registry=CollectorRegistry())


@pytest.fixture
def kill_switch(tmp_path: Path) -> KillSwitch:
    return KillSwitch(
        max_size_usdc=Decimal("100"),
        daily_max_usdc=Decimal("1000"),
        daily_max_trades=100,
        circuit_breaker_failures=3,
        pause_file=tmp_path / "pause",
    )


def _make_executor(
    *,
    metrics: Metrics,
    kill_switch: KillSwitch,
    clob_client: Any,
    clob: _StubCLOB | None = None,
    max_size_usdc: Decimal = Decimal("100"),
) -> Web3CLOBExecutor:
    return Web3CLOBExecutor(
        clob_client=clob_client,
        clob=clob if clob is not None else _StubCLOB(book=_trivial_book()),
        kill_switch=kill_switch,
        max_size_usdc=max_size_usdc,
        metrics=metrics,
    )


# ----- Happy path -----


async def test_execute_happy_path_returns_executed(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """CLOB sucesso -> ExecutionResult(mode=REAL, success=True, tx_hash, gas_wei)."""
    clob = MagicMock()
    clob.create_order.return_value = "signed_order"
    clob.post_order.return_value = {
        "success": True,
        "transactionHash": "0xabcdef" + "00" * 29,
        "gasUsed": 150000,
    }

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.mode == ExecutionMode.REAL
    assert result.success is True
    assert result.tx_hash == "0xabcdef" + "00" * 29
    assert result.gas_wei == 150000
    assert kill_switch.consecutive_failures == 0


# ----- Kill-switch bloqueio (CLOB nunca chamado) -----


async def test_execute_blocked_by_kill_switch_does_not_call_clob(
    metrics: Metrics, kill_switch: KillSwitch, tmp_path: Path
) -> None:
    """Pause file existe -> CLOB nunca é chamado."""
    (tmp_path / "pause").touch()
    clob = MagicMock()
    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)

    result = await executor.execute(_trade(), Decimal("1"))

    assert result.success is False
    assert result.failure_reason == FailureReason.MANUALLY_PAUSED
    clob.create_order.assert_not_called()
    clob.post_order.assert_not_called()


async def test_execute_size_exceeds_executor_cap_blocked(metrics: Metrics, tmp_path: Path) -> None:
    """size > max_size_usdc -> SIZE_EXCEEDS_EXECUTOR_CAP."""
    ks = KillSwitch(
        max_size_usdc=Decimal("2"),
        daily_max_usdc=Decimal("1000"),
        daily_max_trades=100,
        circuit_breaker_failures=3,
        pause_file=tmp_path / "pause",
    )
    clob = MagicMock()
    executor = Web3CLOBExecutor(
        clob_client=clob,
        clob=_StubCLOB(book=_trivial_book()),
        kill_switch=ks,
        max_size_usdc=Decimal("2"),
        metrics=metrics,
    )

    result = await executor.execute(_trade(), Decimal("3"))

    assert result.failure_reason == FailureReason.SIZE_EXCEEDS_EXECUTOR_CAP
    clob.create_order.assert_not_called()


# ----- CLOB exception classification -----


async def test_clob_rpc_error_returns_rpc_error(metrics: Metrics, kill_switch: KillSwitch) -> None:
    """Exception com 'rpc' no msg -> RPC_ERROR + record_failure()."""
    clob = MagicMock()
    clob.create_order.side_effect = ConnectionError("rpc node down")

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.failure_reason == FailureReason.RPC_ERROR
    assert result.error_message == "rpc node down"
    assert kill_switch.consecutive_failures == 1


async def test_clob_signature_error_returns_signature_error(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """Exception com 'signature' -> SIGNATURE_ERROR."""
    clob = MagicMock()
    clob.create_order.side_effect = ValueError("invalid signature")

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.failure_reason == FailureReason.SIGNATURE_ERROR


async def test_clob_insufficient_balance_returns_balance_error(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """Exception com 'balance' -> INSUFFICIENT_USDC_BALANCE."""
    clob = MagicMock()
    clob.create_order.return_value = "signed"
    clob.post_order.side_effect = RuntimeError("insufficient balance")

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.failure_reason == FailureReason.INSUFFICIENT_USDC_BALANCE


async def test_clob_insufficient_allowance_returns_allowance_error(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """Exception com 'allowance' -> INSUFFICIENT_USDC_ALLOWANCE."""
    clob = MagicMock()
    clob.create_order.return_value = "signed"
    clob.post_order.side_effect = RuntimeError("not enough allowance")

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.failure_reason == FailureReason.INSUFFICIENT_USDC_ALLOWANCE


async def test_clob_generic_error_returns_clob_rejected(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """Exception genérica -> CLOB_REJECTED_ORDER."""
    clob = MagicMock()
    clob.create_order.return_value = "signed"
    clob.post_order.side_effect = RuntimeError("market closed")

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.failure_reason == FailureReason.CLOB_REJECTED_ORDER


async def test_clob_post_returns_success_false(metrics: Metrics, kill_switch: KillSwitch) -> None:
    """response.success=False -> CLOB_REJECTED_ORDER."""
    clob = MagicMock()
    clob.create_order.return_value = "signed"
    clob.post_order.return_value = {"success": False, "errorMsg": "rejected"}

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.failure_reason == FailureReason.CLOB_REJECTED_ORDER
    assert result.error_message == "rejected"


# ----- ExecutionResult sempre mode=REAL -----


async def test_execute_always_mode_real(metrics: Metrics, kill_switch: KillSwitch) -> None:
    """Web3CLOBExecutor sempre retorna mode=REAL (nunca DRY_RUN)."""
    clob = MagicMock()
    clob.create_order.return_value = "signed"
    clob.post_order.return_value = {
        "success": True,
        "transactionHash": "0xab",
        "gasUsed": 100,
    }

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))
    assert result.mode == ExecutionMode.REAL


# ----- Métricas observadas -----


async def test_metric_kill_switch_blocks_incremented(metrics: Metrics, tmp_path: Path) -> None:
    """kill_switch_blocks_total{reason} incrementa quando bloqueia."""
    pause = tmp_path / "pause"
    pause.touch()
    ks = KillSwitch(
        max_size_usdc=Decimal("100"),
        daily_max_usdc=Decimal("1000"),
        daily_max_trades=100,
        circuit_breaker_failures=3,
        pause_file=pause,
    )
    clob = MagicMock()
    executor = Web3CLOBExecutor(
        clob_client=clob,
        clob=_StubCLOB(book=_trivial_book()),
        kill_switch=ks,
        max_size_usdc=Decimal("100"),
        metrics=metrics,
    )

    await executor.execute(_trade(), Decimal("1"))

    counter = metrics.executor_kill_switch_blocks_total.labels(reason="manually_paused")
    assert counter._value.get() == 1.0


async def test_metric_clob_request_duration_observed_on_success(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """clob_request_duration_seconds{result=success} observa."""
    clob = MagicMock()
    clob.create_order.return_value = "signed"
    clob.post_order.return_value = {
        "success": True,
        "transactionHash": "0xab",
        "gasUsed": 100,
    }
    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    await executor.execute(_trade(), Decimal("1"))

    samples = list(metrics.executor_clob_request_duration_seconds.collect())[0].samples
    success_count = next(
        s.value
        for s in samples
        if s.name.endswith("_count") and s.labels.get("result") == "success"
    )
    assert success_count == 1.0


async def test_metric_clob_request_duration_observed_on_error(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """clob_request_duration_seconds{result=error} observa em failure."""
    clob = MagicMock()
    clob.create_order.side_effect = RuntimeError("oops")
    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    await executor.execute(_trade(), Decimal("1"))

    samples = list(metrics.executor_clob_request_duration_seconds.collect())[0].samples
    error_count = next(
        s.value for s in samples if s.name.endswith("_count") and s.labels.get("result") == "error"
    )
    assert error_count == 1.0


async def test_metric_consecutive_failures_gauge_reflects_state(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """consecutive_failures Gauge reflete state do kill_switch."""
    clob = MagicMock()
    clob.create_order.side_effect = RuntimeError("fail1")
    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    await executor.execute(_trade(), Decimal("1"))

    assert metrics.executor_consecutive_failures._value.get() == 1.0


# ----- _classify_clob_error -----


def test_classify_clob_error_rpc_keyword() -> None:
    assert _classify_clob_error(ConnectionError("rpc timeout")) == FailureReason.RPC_ERROR


def test_classify_clob_error_signature_keyword() -> None:
    assert _classify_clob_error(ValueError("bad signature")) == FailureReason.SIGNATURE_ERROR


def test_classify_clob_error_balance_keyword() -> None:
    assert (
        _classify_clob_error(RuntimeError("not enough balance"))
        == FailureReason.INSUFFICIENT_USDC_BALANCE
    )


def test_classify_clob_error_allowance_keyword() -> None:
    assert (
        _classify_clob_error(RuntimeError("low allowance"))
        == FailureReason.INSUFFICIENT_USDC_ALLOWANCE
    )


def test_classify_clob_error_generic_falls_back_to_rejected() -> None:
    assert _classify_clob_error(RuntimeError("market closed")) == FailureReason.CLOB_REJECTED_ORDER


def test_classify_clob_error_multiple_keywords_first_wins() -> None:
    """Documenta semântica: rpc tem prioridade sobre balance em msgs ambíguas."""
    assert _classify_clob_error(RuntimeError("rpc balance error")) == FailureReason.RPC_ERROR


def test_classify_clob_error_empty_message_falls_back() -> None:
    """Exception sem mensagem cai no fallback CLOB_REJECTED_ORDER."""
    assert _classify_clob_error(RuntimeError()) == FailureReason.CLOB_REJECTED_ORDER


# ----- build_clob_client -----


def test_build_clob_client_raises_when_wallet_private_key_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """build_clob_client raise se WALLET_PRIVATE_KEY ausente."""
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.delenv("WALLET_PRIVATE_KEY", raising=False)

    settings = Settings()  # type: ignore[call-arg]
    with pytest.raises(RuntimeError, match="WALLET_PRIVATE_KEY"):
        build_clob_client(settings)


def test_build_clob_client_uses_derived_funder_address(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """build_clob_client deriva funder address da private key via eth_account.Account."""
    # Private key de teste (NUNCA usar em produção): determinística pra testes
    test_pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("WALLET_PRIVATE_KEY", test_pk)

    settings = Settings()  # type: ignore[call-arg]

    # Mockar ClobClient pra evitar request HTTP de auth real
    from unittest.mock import patch as _patch

    with _patch(
        "polycopy.infrastructure.execution.web3_clob_executor.ClobClient"
    ) as clob_client_mock:
        instance = MagicMock()
        clob_client_mock.return_value = instance
        instance.create_or_derive_api_creds.return_value = MagicMock()

        client = build_clob_client(settings)

        assert client is instance
        # Confirmar funder derivado da PK
        from eth_account import Account

        expected_funder = Account.from_key(test_pk).address
        clob_client_mock.assert_called_once()
        call_kwargs = clob_client_mock.call_args.kwargs
        assert call_kwargs["funder"] == expected_funder
        assert call_kwargs["signature_type"] == 0  # EOA


# ----- verify_allowance -----


async def test_verify_allowance_raises_when_wallet_private_key_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """verify_allowance raise se WALLET_PRIVATE_KEY ausente."""
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.delenv("WALLET_PRIVATE_KEY", raising=False)

    settings = Settings()  # type: ignore[call-arg]
    with pytest.raises(RuntimeError, match="WALLET_PRIVATE_KEY"):
        await verify_allowance(settings, Decimal("1"))


async def test_verify_allowance_passes_when_sufficient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """verify_allowance no-op (sem raise) quando allowance >= min."""
    test_pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("WALLET_PRIVATE_KEY", test_pk)

    settings = Settings()  # type: ignore[call-arg]

    from unittest.mock import patch as _patch

    with _patch("polycopy.infrastructure.execution.web3_clob_executor.Web3") as web3_mock:
        # allowance retorna 100 USDC em micro-USDC (100 * 10^6)
        contract_mock = MagicMock()
        contract_mock.functions.allowance.return_value.call.return_value = 100 * 10**6
        web3_mock.return_value.eth.contract.return_value = contract_mock
        web3_mock.HTTPProvider = MagicMock()
        web3_mock.to_checksum_address = lambda addr: addr  # passthrough

        # Não raise = passou
        await verify_allowance(settings, Decimal("50"))


async def test_verify_allowance_raises_when_insufficient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """verify_allowance raise com mensagem instrutiva quando allowance < min."""
    test_pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("WALLET_PRIVATE_KEY", test_pk)

    settings = Settings()  # type: ignore[call-arg]

    from unittest.mock import patch as _patch

    with _patch("polycopy.infrastructure.execution.web3_clob_executor.Web3") as web3_mock:
        # allowance retorna 5 USDC em micro-USDC (insuficiente pra min=10)
        contract_mock = MagicMock()
        contract_mock.functions.allowance.return_value.call.return_value = 5 * 10**6
        web3_mock.return_value.eth.contract.return_value = contract_mock
        web3_mock.HTTPProvider = MagicMock()
        web3_mock.to_checksum_address = lambda addr: addr

        with pytest.raises(RuntimeError, match="allowance insufficient"):
            await verify_allowance(settings, Decimal("10"))


# ----- expected_avg_price parity -----


async def test_web3_clob_executor_populates_expected_avg_price_on_success(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """Happy path: expected_avg_price calculado e propagado no ExecutionResult."""
    # Book com ask único a 0.5, size 1000 USDC — suficiente para target de 1 USDC
    book = _book(asks=[_level("0.5", "1000")])
    clob = MagicMock()
    clob.create_order.return_value = "signed_order"
    clob.post_order.return_value = {
        "success": True,
        "transactionHash": "0xabcdef" + "00" * 29,
        "gasUsed": 150000,
    }

    executor = Web3CLOBExecutor(
        clob_client=clob,
        clob=_StubCLOB(book=book),
        kill_switch=kill_switch,
        max_size_usdc=Decimal("100"),
        metrics=metrics,
    )
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.mode == ExecutionMode.REAL
    assert result.success is True
    assert result.expected_avg_price is not None


async def test_web3_clob_executor_expected_avg_price_propagated_on_clob_error(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """Falha na submissão: expected_avg_price ainda é propagado no resultado de erro."""
    book = _book(asks=[_level("0.6", "1000")])
    clob_client = MagicMock()
    clob_client.create_order.side_effect = RuntimeError("rpc node down")

    executor = Web3CLOBExecutor(
        clob_client=clob_client,
        clob=_StubCLOB(book=book),
        kill_switch=kill_switch,
        max_size_usdc=Decimal("100"),
        metrics=metrics,
    )
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.success is False
    assert result.failure_reason == FailureReason.RPC_ERROR
    assert result.expected_avg_price is not None


async def test_web3_clob_executor_expected_avg_price_none_when_book_fetch_fails(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """get_book lança exception: expected_avg_price=None, mas execução prossegue."""
    clob_client = MagicMock()
    clob_client.create_order.return_value = "signed"
    clob_client.post_order.return_value = {
        "success": True,
        "transactionHash": "0xabc",
        "gasUsed": 100,
    }

    executor = Web3CLOBExecutor(
        clob_client=clob_client,
        clob=_StubCLOB(raise_exc=ConnectionError("book fetch failed")),
        kill_switch=kill_switch,
        max_size_usdc=Decimal("100"),
        metrics=metrics,
    )
    result = await executor.execute(_trade(), Decimal("1"))

    # Execução prosseguiu com sucesso, mas expected_avg_price indisponível
    assert result.success is True
    assert result.expected_avg_price is None
