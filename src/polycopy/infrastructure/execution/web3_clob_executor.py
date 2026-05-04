"""Web3CLOBExecutor: implementação real de OrderExecutor via py-clob-client.

Polygon mainnet only. EOA SIGNATURE_TYPE=0. Usa py-clob-client (oficial
Polymarket) que encapsula EIP-712 signing + submissão pro operator
off-chain + settlement on-chain.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Any

import structlog
from eth_account import Account
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderType
from web3 import Web3

from polycopy.config import Settings
from polycopy.domain.events import ExecutionMode, FailureReason
from polycopy.domain.execution import ExecutionResult
from polycopy.domain.models import Trade
from polycopy.domain.slippage import calculate_expected_avg_price
from polycopy.infrastructure.execution.kill_switch import KillSwitch
from polycopy.infrastructure.execution.order_mapper import to_order_args
from polycopy.infrastructure.observability.metrics import Metrics
from polycopy.ports import PolymarketClobPort


class Web3CLOBExecutor:
    """Implementação real do OrderExecutor via py-clob-client.

    Strategy injetado no ExecutorAgent quando EXECUTOR_DRY_RUN=false.
    Sempre retorna ExecutionResult(mode=REAL, ...).
    """

    def __init__(
        self,
        *,
        clob_client: ClobClient,
        clob: PolymarketClobPort,
        kill_switch: KillSwitch,
        max_size_usdc: Decimal,
        metrics: Metrics,
    ) -> None:
        self._clob_client = clob_client
        self._clob = clob
        self._kill_switch = kill_switch
        self._max_size_usdc = max_size_usdc
        self._metrics = metrics
        self._log = structlog.get_logger("web3_clob_executor")

    async def _compute_expected_price(
        self, trade: Trade, final_size_usdc: Decimal
    ) -> Decimal | None:
        try:
            book = await self._clob.get_book(trade.token_id)
        except Exception as exc:  # noqa: BLE001
            self._metrics.executor_expected_price_unavailable_total.labels(
                reason="fetch_failed"
            ).inc()
            self._log.warning(
                "expected_price_fetch_failed",
                error=str(exc),
                error_type=type(exc).__name__,
                token_id=trade.token_id.value,
            )
            return None

        if not book.asks and not book.bids:
            self._metrics.executor_expected_price_unavailable_total.labels(
                reason="empty_book"
            ).inc()
            return None

        result = calculate_expected_avg_price(
            book=book, side=trade.side, target_usdc=final_size_usdc
        )
        if result is None:
            self._metrics.executor_expected_price_unavailable_total.labels(
                reason="insufficient_volume"
            ).inc()
        return result

    async def execute(self, trade: Trade, final_size_usdc: Decimal) -> ExecutionResult:
        # 0. Computar expected_avg_price antes de submeter (parity com DryRunExecutor)
        expected = await self._compute_expected_price(trade, final_size_usdc)

        # 1. Kill-switch (5 camadas, fail-fast)
        block_reason = self._kill_switch.check(final_size_usdc)
        if block_reason is not None:
            self._metrics.executor_kill_switch_blocks_total.labels(reason=block_reason.value).inc()
            return ExecutionResult(
                mode=ExecutionMode.REAL,
                success=False,
                failure_reason=block_reason,
                error_message=f"kill_switch blocked: {block_reason.value}",
                expected_avg_price=expected,
            )

        # 2. Mapear Trade -> OrderArgs
        args = to_order_args(trade, final_size_usdc)

        # 3. Submeter via py-clob-client (sync API -> asyncio.to_thread)
        clob_start = time.perf_counter()
        try:
            signed = await asyncio.to_thread(self._clob_client.create_order, args)
            # py-clob-client retorna dict serializável da response JSON do CLOB API;
            # verificar empiricamente em T8 (smoke test) se o shape bate.
            response: dict[str, Any] = await asyncio.to_thread(
                self._clob_client.post_order, signed, OrderType.GTC
            )
        except Exception as exc:  # noqa: BLE001 — vira OrderFailed
            self._metrics.executor_clob_request_duration_seconds.labels(result="error").observe(
                time.perf_counter() - clob_start
            )
            self._kill_switch.record_failure()
            self._metrics.executor_consecutive_failures.set(self._kill_switch.consecutive_failures)
            reason = _classify_clob_error(exc)
            return ExecutionResult(
                mode=ExecutionMode.REAL,
                success=False,
                failure_reason=reason,
                error_message=str(exc),
                expected_avg_price=expected,
            )

        self._metrics.executor_clob_request_duration_seconds.labels(result="success").observe(
            time.perf_counter() - clob_start
        )

        # 4. Verificar response do CLOB
        if not response.get("success", False):
            self._kill_switch.record_failure()
            self._metrics.executor_consecutive_failures.set(self._kill_switch.consecutive_failures)
            return ExecutionResult(
                mode=ExecutionMode.REAL,
                success=False,
                failure_reason=FailureReason.CLOB_REJECTED_ORDER,
                error_message=str(response.get("errorMsg", "unknown")),
                expected_avg_price=expected,
            )

        # 5. Sucesso
        self._kill_switch.record_success(final_size_usdc)
        self._metrics.executor_consecutive_failures.set(0)
        return ExecutionResult(
            mode=ExecutionMode.REAL,
            success=True,
            tx_hash=str(response["transactionHash"]),
            # TODO(T8 smoke test): py-clob-client post_order é submissão off-chain (matching).
            # Settlement on-chain pode vir em batch — verificar empiricamente se
            # response["gasUsed"] reflete gas real ou se chega 0/ausente. Métrica pode
            # precisar leitura on-chain pós-tx_hash em hardening.
            gas_wei=int(response.get("gasUsed", 0)),
            expected_avg_price=expected,
        )


def _classify_clob_error(exc: Exception) -> FailureReason:
    """Mapeia exception do py-clob-client pra FailureReason específica.

    Heurística por keyword no mensagem (py-clob-client não tem hierarchy
    rica de exceptions; usa RuntimeError genérico em geral).

    Ordem de prioridade (primeiro match ganha): rpc -> signature ->
    balance -> allowance -> fallback CLOB_REJECTED_ORDER.

    Em mensagens com múltiplas keywords (ex: "rpc balance error"), RPC
    ganha porque é tratado como causa upstream — falha de rede precede
    falha de protocolo. Mensagens vazias caem no fallback.
    """
    msg = str(exc).lower()
    if "rpc" in msg:
        return FailureReason.RPC_ERROR
    if "signature" in msg:
        return FailureReason.SIGNATURE_ERROR
    if "balance" in msg:
        return FailureReason.INSUFFICIENT_USDC_BALANCE
    if "allowance" in msg:
        return FailureReason.INSUFFICIENT_USDC_ALLOWANCE
    return FailureReason.CLOB_REJECTED_ORDER


def build_clob_client(settings: Settings) -> ClobClient:
    """Factory que monta ClobClient a partir de Settings.

    Usado no main() do executor agent quando real-mode ativo.
    Requer wallet_private_key set (raise se None).
    """
    if settings.wallet_private_key is None:
        raise RuntimeError("WALLET_PRIVATE_KEY required for real-mode")

    pk = settings.wallet_private_key.get_secret_value()
    # Em EOA SIGNATURE_TYPE=0, funder = address derivada da private key.
    # py-clob-client requer funder address explícito.
    funder_address = Account.from_key(pk).address

    client = ClobClient(
        host=settings.polymarket_clob_api_url,
        key=pk,
        chain_id=settings.polygon_chain_id,
        signature_type=0,  # EOA
        funder=funder_address,
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


async def verify_allowance(settings: Settings, min_required_usdc: Decimal) -> None:
    """Verifica que wallet tem allowance >= min_required_usdc pra Exchange.

    Raise RuntimeError se baixa — operador precisa rodar setup_wallet.
    Lê on-chain via web3.py (py-clob-client não expõe método de allowance).
    """
    if settings.wallet_private_key is None:
        raise RuntimeError("WALLET_PRIVATE_KEY required for verify_allowance")

    pk = settings.wallet_private_key.get_secret_value()
    wallet_address = Account.from_key(pk).address

    w3 = Web3(Web3.HTTPProvider(settings.polygon_rpc_url))
    # USDC contract on Polygon
    usdc_address = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
    usdc_abi = [
        {
            "constant": True,
            "inputs": [
                {"name": "_owner", "type": "address"},
                {"name": "_spender", "type": "address"},
            ],
            "name": "allowance",
            "outputs": [{"name": "", "type": "uint256"}],
            "type": "function",
        }
    ]
    usdc = w3.eth.contract(address=Web3.to_checksum_address(usdc_address), abi=usdc_abi)
    allowance_raw = await asyncio.to_thread(
        usdc.functions.allowance(
            Web3.to_checksum_address(wallet_address),
            Web3.to_checksum_address(settings.polymarket_exchange_address),
        ).call
    )
    allowance_usdc = Decimal(allowance_raw) / Decimal(10**6)  # USDC has 6 decimals
    if allowance_usdc < min_required_usdc:
        raise RuntimeError(
            f"USDC allowance insufficient: have ${allowance_usdc}, "
            f"need >= ${min_required_usdc}. "
            f"Run: uv run python -m polycopy.scripts.setup_wallet"
        )
