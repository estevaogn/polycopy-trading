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
