"""Domain types and pure functions for wallet discovery (Fase 6).

Types correspond to Polymarket leaderboard API:
https://data-api.polymarket.com/v1/leaderboard
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from polycopy.domain.value_objects import WalletAddress


class TimePeriod(StrEnum):
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    ALL = "ALL"


class Category(StrEnum):
    OVERALL = "OVERALL"
    POLITICS = "POLITICS"
    SPORTS = "SPORTS"
    CRYPTO = "CRYPTO"
    CULTURE = "CULTURE"
    MENTIONS = "MENTIONS"
    WEATHER = "WEATHER"
    ECONOMICS = "ECONOMICS"
    TECH = "TECH"
    FINANCE = "FINANCE"


class OrderBy(StrEnum):
    PNL = "PNL"
    VOL = "VOL"


@dataclass(frozen=True)
class LeaderboardEntry:
    rank: int
    address: WalletAddress
    user_name: str | None
    volume_usdc: Decimal
    pnl_usdc: Decimal
    verified_badge: bool


@dataclass(frozen=True)
class CandidateWallet:
    address: WalletAddress
    label: str
    rank: int
    volume_usdc: Decimal
    pnl_usdc: Decimal
    verified_badge: bool


_LABEL_MAX_LEN = 32
_FALLBACK_ADDR_PREFIX_CHARS = 10


def derive_label(entry: LeaderboardEntry) -> str:
    """Return a sanitized label for a leaderboard entry.

    Sanitization rules:
    - trim leading/trailing whitespace
    - replace runs of whitespace with single '_'
    - drop non-printable characters
    - cap at 32 chars
    - fall back to '0x<8-hex>…' when user_name is empty after sanitization
    """
    raw = (entry.user_name or "").strip()
    collapsed = re.sub(r"\s+", "_", raw)
    printable = "".join(ch for ch in collapsed if ch.isprintable())
    if not printable:
        return f"{entry.address.value[:_FALLBACK_ADDR_PREFIX_CHARS]}…"
    return printable[:_LABEL_MAX_LEN]


def filter_and_rank(
    entries: list[LeaderboardEntry],
    *,
    min_volume_usdc: Decimal,
    exclude: set[WalletAddress],
    top_n: int,
) -> list[CandidateWallet]:
    """Filter, dedup, and convert entries to candidates.

    - Drops entries whose address is in `exclude`.
    - Drops entries whose `volume_usdc < min_volume_usdc`.
    - Dedups by address (first occurrence wins).
    - Preserves input order (caller should pass entries already sorted by PNL desc).
    - Truncates result to `top_n`.
    """
    seen: set[WalletAddress] = set()
    out: list[CandidateWallet] = []
    for e in entries:
        if e.address in exclude:
            continue
        if e.volume_usdc < min_volume_usdc:
            continue
        if e.address in seen:
            continue
        seen.add(e.address)
        out.append(
            CandidateWallet(
                address=e.address,
                label=derive_label(e),
                rank=e.rank,
                volume_usdc=e.volume_usdc,
                pnl_usdc=e.pnl_usdc,
                verified_badge=e.verified_badge,
            )
        )
        if len(out) >= top_n:
            break
    return out


def render_candidates_yaml(candidates: list[CandidateWallet]) -> str:
    """Render candidates as YAML matching wallets_seed.yaml schema."""
    if not candidates:
        return "wallets: []\n"
    lines = ["wallets:"]
    for c in candidates:
        lines.append(f'  - address: "{c.address.value}"')
        lines.append(f'    label: "{c.label}"')
    return "\n".join(lines) + "\n"
