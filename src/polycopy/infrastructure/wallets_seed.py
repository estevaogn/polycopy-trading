"""Parser e validacao do `wallets_seed.yaml`.

Formato esperado:
    wallets:
      - address: "0x..."
        label: "Whale 1"

Validacao:
- `address` deve ser endereco Ethereum valido (passa por `WalletAddress`).
- `label` e string nao-vazia.
- Lista pode ser vazia (`wallets: []` ou `wallets: null` - ambos retornam []).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from polycopy.domain.value_objects import WalletAddress


@dataclass(frozen=True)
class TrackedWallet:
    address: WalletAddress
    label: str


def load_wallets_seed(path: Path) -> list[TrackedWallet]:
    """Carrega e valida wallets do YAML.

    Raises:
        FileNotFoundError: se o caminho nao existe.
        ValueError: se schema invalido (address ausente/invalido, label ausente, etc).
    """
    if not path.exists():
        raise FileNotFoundError(f"wallets seed file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict) or "wallets" not in data:
        raise ValueError(
            f"wallets seed must have top-level 'wallets' key; got: {type(data).__name__}"
        )

    raw = data["wallets"]
    if raw is None:
        raw = []
    if not isinstance(raw, list):
        raise ValueError(f"'wallets' must be a list; got {type(raw).__name__}")

    wallets: list[TrackedWallet] = []
    seen_addrs: dict[str, int] = {}  # lowercase address → first-seen index
    seen_labels: dict[str, int] = {}  # label → first-seen index
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"wallet[{i}] must be a mapping; got {type(item).__name__}")
        if "address" not in item:
            raise ValueError(f"wallet[{i}] missing 'address'")
        if "label" not in item:
            raise ValueError(f"wallet[{i}] missing 'label'")
        addr = WalletAddress(value=str(item["address"]))
        label = str(item["label"]).strip()
        if not label:
            raise ValueError(f"wallet[{i}] 'label' must be non-empty")

        # Deteção de duplicatas — falha loud em vez de carregar silenciosamente.
        if addr.value in seen_addrs:
            first = seen_addrs[addr.value]
            raise ValueError(
                f"wallet[{i}] address {addr.value} duplicada (já visto em wallet[{first}])"
            )
        if label in seen_labels:
            first = seen_labels[label]
            raise ValueError(f"wallet[{i}] label {label!r} duplicada (já visto em wallet[{first}])")

        seen_addrs[addr.value] = i
        seen_labels[label] = i
        wallets.append(TrackedWallet(address=addr, label=label))

    return wallets
