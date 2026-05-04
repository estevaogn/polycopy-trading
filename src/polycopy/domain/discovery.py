"""Domain types and pure functions for wallet discovery (Fase 6).

Types correspond to Polymarket leaderboard API:
https://data-api.polymarket.com/v1/leaderboard
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
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


def _escape_yaml_double_quoted(value: str) -> str:
    """Escape a string for use inside a YAML double-quoted scalar.

    YAML double-quoted strings recognize C-style escapes; we only need to
    escape backslash (must come first) and double quote.
    """
    return value.replace("\\", "\\\\").replace('"', '\\"')


def render_candidates_yaml(candidates: list[CandidateWallet]) -> str:
    """Render candidates as YAML matching wallets_seed.yaml schema."""
    if not candidates:
        return "wallets: []\n"
    lines = ["wallets:"]
    for c in candidates:
        lines.append(f'  - address: "{c.address.value}"')
        lines.append(f'    label: "{_escape_yaml_double_quoted(c.label)}"')
    return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class ReportMetadata:
    generated_at: datetime
    time_period: TimePeriod
    category: Category
    order_by: OrderBy
    min_volume_usdc: Decimal
    top_requested: int
    seed_path: str
    seed_size: int
    total_fetched: int
    total_excluded_existing: int
    total_excluded_min_volume: int
    total_candidates: int


def _escape_md_cell(text: str) -> str:
    """Escape pipe chars so they don't break a markdown table row."""
    return text.replace("|", "\\|")


def render_report_md(
    candidates: list[CandidateWallet],
    *,
    metadata: ReportMetadata,
) -> str:
    """Render the human-readable run report (frontmatter + markdown table)."""
    m = metadata
    fm = [
        "---",
        f"generated_at: {m.generated_at.isoformat()}",
        f"time_period: {m.time_period.value}",
        f"category: {m.category.value}",
        f"order_by: {m.order_by.value}",
        f"min_volume_usdc: {m.min_volume_usdc}",
        f"top: {m.top_requested}",
        f"seed_path: {m.seed_path}",
        f"seed_size: {m.seed_size}",
        f"total_fetched: {m.total_fetched}",
        f"total_excluded_existing: {m.total_excluded_existing}",
        f"total_excluded_min_volume: {m.total_excluded_min_volume}",
        f"total_candidates: {m.total_candidates}",
        "---",
        "",
        f"# Wallet candidates — {m.time_period.value}/{m.category.value} "
        f"(run {m.generated_at:%Y-%m-%d %H:%M UTC})",
        "",
        "| Rank | userName | Address | Volume (USDC) | PnL (USDC) | Verified | Polymarket |",
        "|---:|:--|:--|--:|--:|:--:|--|",
    ]
    rows: list[str] = []
    for c in candidates:
        addr = c.address.value
        addr_short = f"{addr[:10]}…{addr[-4:]}"
        verified = "yes" if c.verified_badge else "no"
        link = f"https://polymarket.com/profile/{addr}"
        rows.append(
            f"| {c.rank} | {_escape_md_cell(c.label)} | {addr_short} | "
            f"{c.volume_usdc:,.2f} | {c.pnl_usdc:+,.2f} | {verified} | "
            f"[link]({link}) |"
        )
    return "\n".join(fm + rows) + "\n"
