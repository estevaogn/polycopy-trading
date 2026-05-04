"""Testes unit do setup_wallet script — Web3 mockado, sem rede."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from polycopy.scripts.setup_wallet import _approve_usdc, _print_status


def test_print_status_shows_balances_and_allowance(capsys: pytest.CaptureFixture) -> None:
    """Imprime address, MATIC balance, USDC balance, allowance."""
    _print_status(
        wallet_address="0x" + "1" * 40,
        matic_balance=Decimal("5.0"),
        usdc_balance=Decimal("20.0"),
        allowance=Decimal("0"),
        max_approval_usdc=100,
        exchange_address="0x2222222222222222222222222222222222222222",
    )
    captured = capsys.readouterr()
    assert "0x" + "1" * 40 in captured.out
    assert "5.0" in captured.out  # MATIC
    assert "20.0" in captured.out  # USDC
    assert "0" in captured.out  # allowance


def test_approve_usdc_requires_yes_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Confirmação 'no' → não chama approve."""
    monkeypatch.setattr("builtins.input", lambda _: "no")
    web3 = MagicMock()
    usdc_contract = MagicMock()

    result = _approve_usdc(
        web3=web3,
        usdc_contract=usdc_contract,
        wallet_address="0x" + "1" * 40,
        wallet_private_key="0x" + "ab" * 32,
        exchange_address="0x2222222222222222222222222222222222222222",
        max_approval_usdc=100,
    )
    assert result is None  # Não fez approve
    usdc_contract.functions.approve.assert_not_called()


def test_approve_usdc_yes_calls_approve_with_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirmação 'yes' → chama approve com cap em micro-USDC."""
    monkeypatch.setattr("builtins.input", lambda _: "yes")
    web3 = MagicMock()
    web3.eth.gas_price = 30 * 10**9
    web3.eth.get_transaction_count.return_value = 0
    web3.eth.send_raw_transaction.return_value = b"\xab" * 32
    web3.eth.wait_for_transaction_receipt.return_value = {"status": 1}

    usdc_contract = MagicMock()
    tx = {"to": "0xusdc", "data": "0x..."}
    usdc_contract.functions.approve.return_value.build_transaction.return_value = tx

    with patch("polycopy.scripts.setup_wallet.Account") as account_mock:
        signed_tx = MagicMock()
        signed_tx.raw_transaction = b"\xcd" * 32
        account_mock.from_key.return_value.sign_transaction.return_value = signed_tx

        result = _approve_usdc(
            web3=web3,
            usdc_contract=usdc_contract,
            wallet_address="0x" + "1" * 40,
            wallet_private_key="0x" + "ab" * 32,
            exchange_address="0x2222222222222222222222222222222222222222",
            max_approval_usdc=100,
        )
        # Verifica approve chamado com 100 USDC * 10^6 micro-USDC
        usdc_contract.functions.approve.assert_called_once_with(
            "0x2222222222222222222222222222222222222222", 100 * 10**6
        )
        assert result is not None  # tx_hash retornado


def test_approve_usdc_prints_polygonscan_url(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """Após approve sucesso, imprime URL Polygonscan."""
    monkeypatch.setattr("builtins.input", lambda _: "yes")
    web3 = MagicMock()
    web3.eth.gas_price = 30 * 10**9
    web3.eth.get_transaction_count.return_value = 0
    web3.eth.send_raw_transaction.return_value = b"\xab" * 32
    web3.eth.wait_for_transaction_receipt.return_value = {"status": 1}

    usdc_contract = MagicMock()
    tx = {"to": "0xusdc", "data": "0x..."}
    usdc_contract.functions.approve.return_value.build_transaction.return_value = tx

    with patch("polycopy.scripts.setup_wallet.Account") as account_mock:
        signed_tx = MagicMock()
        signed_tx.raw_transaction = b"\xcd" * 32
        account_mock.from_key.return_value.sign_transaction.return_value = signed_tx

        _approve_usdc(
            web3=web3,
            usdc_contract=usdc_contract,
            wallet_address="0x" + "1" * 40,
            wallet_private_key="0x" + "ab" * 32,
            exchange_address="0x2222222222222222222222222222222222222222",
            max_approval_usdc=100,
        )
    captured = capsys.readouterr()
    assert "polygonscan.com" in captured.out
