"""Tests for wallets_seed YAML parser."""

from __future__ import annotations

from pathlib import Path

import pytest

from polycopy.infrastructure.wallets_seed import (
    TrackedWallet,
    load_wallets_seed,
)


def _write(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def test_load_returns_tracked_wallets(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "w.yaml",
        """
wallets:
  - address: "0x1234567890abcdef1234567890abcdef12345678"
    label: "Whale 1"
  - address: "0x9999999999999999999999999999999999999999"
    label: "Whale 2"
""",
    )
    wallets = load_wallets_seed(path)
    assert len(wallets) == 2
    assert all(isinstance(w, TrackedWallet) for w in wallets)
    assert wallets[0].label == "Whale 1"
    assert wallets[0].address.value == "0x1234567890abcdef1234567890abcdef12345678"


def test_load_invalid_address_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "w.yaml",
        """
wallets:
  - address: "not-an-address"
    label: "Bad"
""",
    )
    with pytest.raises(ValueError):
        load_wallets_seed(path)


def test_load_missing_label_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "w.yaml",
        """
wallets:
  - address: "0x1234567890abcdef1234567890abcdef12345678"
""",
    )
    with pytest.raises(ValueError, match="label"):
        load_wallets_seed(path)


def test_load_missing_address_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "w.yaml",
        """
wallets:
  - label: "Solo"
""",
    )
    with pytest.raises(ValueError, match="address"):
        load_wallets_seed(path)


def test_load_missing_file_raises(tmp_path: Path) -> None:
    path = tmp_path / "nope.yaml"
    with pytest.raises(FileNotFoundError):
        load_wallets_seed(path)


def test_load_empty_wallets_list_returns_empty(tmp_path: Path) -> None:
    path = _write(tmp_path / "w.yaml", "wallets: []\n")
    assert load_wallets_seed(path) == []


def test_load_missing_wallets_key_raises(tmp_path: Path) -> None:
    path = _write(tmp_path / "w.yaml", "other: value\n")
    with pytest.raises(ValueError, match="wallets"):
        load_wallets_seed(path)
