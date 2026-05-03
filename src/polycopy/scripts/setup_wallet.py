"""setup_wallet: script CLI manual one-shot pra approve USDC pro Exchange.

Roda uma vez após criar EOA + fundar com USDC + MATIC. NÃO roda no agent.

Uso:
    uv run python -m polycopy.scripts.setup_wallet

Comportamento:
    1. Carrega Settings.
    2. Imprime address da wallet, balances MATIC + USDC, allowance atual.
    3. Pergunta confirmação interativa.
    4. Se sim: chama usdc.approve(EXCHANGE, MAX_APPROVAL_USDC * 10^6).
    5. Imprime tx_hash + URL Polygonscan.
"""

from __future__ import annotations

import asyncio
import sys
from decimal import Decimal
from typing import Any

from eth_account import Account
from web3 import Web3

from polycopy.config import Settings

_USDC_ADDRESS_POLYGON = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
_USDC_ABI = [
    {
        "constant": False,
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_value", "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
]


def _print_status(
    *,
    wallet_address: str,
    matic_balance: Decimal,
    usdc_balance: Decimal,
    allowance: Decimal,
    max_approval_usdc: int,
    exchange_address: str,
) -> None:
    """Imprime status atual da wallet (balances + allowance)."""
    print("=" * 60)
    print("WALLET SETUP — Polymarket CLOB approval")
    print("=" * 60)
    print(f"Wallet address:    {wallet_address}")
    print(f"MATIC balance:     {matic_balance}")
    print(f"USDC balance:      {usdc_balance}")
    print(f"Current allowance: {allowance} USDC")
    print(f"Exchange address:  {exchange_address}")
    print(f"Approval cap:      {max_approval_usdc} USDC")
    print("=" * 60)


def _approve_usdc(
    *,
    web3: Web3,
    usdc_contract: Any,
    wallet_address: str,
    wallet_private_key: str,
    exchange_address: str,
    max_approval_usdc: int,
) -> str | None:
    """Pergunta confirmação interativa e faz approve. Retorna tx_hash hex ou None."""
    confirm = input(f"\nApprove ${max_approval_usdc} USDC for Exchange? (yes/no): ")
    if confirm.lower() != "yes":
        print("Aborted.")
        return None

    nonce = web3.eth.get_transaction_count(Web3.to_checksum_address(wallet_address))
    gas_price = web3.eth.gas_price
    cap_micro = max_approval_usdc * 10**6
    tx = usdc_contract.functions.approve(exchange_address, cap_micro).build_transaction(
        {
            "from": Web3.to_checksum_address(wallet_address),
            "nonce": nonce,
            "gas": 100_000,
            "gasPrice": gas_price,
        }
    )
    signed = Account.from_key(wallet_private_key).sign_transaction(tx)
    tx_hash_bytes = web3.eth.send_raw_transaction(signed.raw_transaction)
    tx_hash = tx_hash_bytes.hex()
    print(f"\nTransaction submitted: 0x{tx_hash}")
    print(f"Polygonscan: https://polygonscan.com/tx/0x{tx_hash}")
    print("Waiting for 1 block confirmation...")
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash_bytes, timeout=60)
    if receipt["status"] == 1:
        print("✅ Approval confirmed on-chain.")
    else:
        print("❌ Transaction reverted!")
        return None
    return tx_hash


async def main() -> None:
    """Entrypoint."""
    settings = Settings()

    if settings.wallet_private_key is None:
        print("ERROR: WALLET_PRIVATE_KEY not set in .env", file=sys.stderr)
        sys.exit(1)

    pk = settings.wallet_private_key.get_secret_value()
    wallet_address = Account.from_key(pk).address

    web3 = Web3(Web3.HTTPProvider(settings.polygon_rpc_url))
    if not web3.is_connected():
        print(f"ERROR: cannot connect to {settings.polygon_rpc_url}", file=sys.stderr)
        sys.exit(1)

    matic_balance_wei = web3.eth.get_balance(Web3.to_checksum_address(wallet_address))
    matic_balance = Decimal(matic_balance_wei) / Decimal(10**18)

    usdc_contract = web3.eth.contract(
        address=Web3.to_checksum_address(_USDC_ADDRESS_POLYGON), abi=_USDC_ABI
    )
    usdc_balance_micro = usdc_contract.functions.balanceOf(
        Web3.to_checksum_address(wallet_address)
    ).call()
    usdc_balance = Decimal(usdc_balance_micro) / Decimal(10**6)

    allowance_micro = usdc_contract.functions.allowance(
        Web3.to_checksum_address(wallet_address),
        Web3.to_checksum_address(settings.polymarket_exchange_address),
    ).call()
    allowance = Decimal(allowance_micro) / Decimal(10**6)

    _print_status(
        wallet_address=wallet_address,
        matic_balance=matic_balance,
        usdc_balance=usdc_balance,
        allowance=allowance,
        max_approval_usdc=settings.max_approval_usdc,
        exchange_address=settings.polymarket_exchange_address,
    )

    if matic_balance < Decimal("0.1"):
        print(
            "\n⚠️  WARNING: MATIC balance < 0.1 — may not have enough gas. "
            "Fund wallet with at least $1 worth of MATIC.",
            file=sys.stderr,
        )

    if usdc_balance == 0:
        print(
            "\n⚠️  WARNING: USDC balance is 0 — fund wallet with USDC first "
            "(or skip approval if you'll fund later).",
            file=sys.stderr,
        )

    _approve_usdc(
        web3=web3,
        usdc_contract=usdc_contract,
        wallet_address=wallet_address,
        wallet_private_key=pk,
        exchange_address=settings.polymarket_exchange_address,
        max_approval_usdc=settings.max_approval_usdc,
    )


if __name__ == "__main__":
    asyncio.run(main())
