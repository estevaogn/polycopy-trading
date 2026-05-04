"""Domain types and pure functions for wallet discovery (Fase 6).

Types correspond to Polymarket leaderboard API:
https://data-api.polymarket.com/v1/leaderboard
"""

from __future__ import annotations

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
