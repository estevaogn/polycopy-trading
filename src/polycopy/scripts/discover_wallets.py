"""Discover wallets CLI — queries Polymarket leaderboard and emits candidates.

Usage:
    uv run python -m polycopy.scripts.discover_wallets [flags]

Flags (all override-able):
    --time-period {DAY,WEEK,MONTH,ALL}     default: MONTH
    --category {OVERALL,POLITICS,...}      default: OVERALL
    --top N                                default: 50  (clamped to 1050)
    --min-volume USDC                      default: 5000
    --seed-path PATH                       default: config/wallets_seed.yaml
    --candidates-out PATH                  default: config/wallets_candidates.yaml
    --report-out PATH                      default: docs/discover_wallets_report.md
    --dry-run                              prints table only, no files

Exit codes:
    0  success
    1  fatal error (API/IO failure)
    2  no candidates after filtering (no files written)
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import yaml

from polycopy.domain.discovery import (
    CandidateWallet,
    Category,
    LeaderboardEntry,
    OrderBy,
    ReportMetadata,
    TimePeriod,
    filter_and_rank,
    render_candidates_yaml,
    render_report_md,
)
from polycopy.domain.value_objects import WalletAddress
from polycopy.infrastructure.observability.logging import get_logger
from polycopy.infrastructure.wallets_seed import load_wallets_seed
from polycopy.ports.discovery_repository import DiscoveryRepository
from polycopy.ports.polymarket_leaderboard import PolymarketLeaderboardPort

_log = get_logger(__name__)

DEFAULT_SEED_PATH = Path("config/wallets_seed.yaml")
DEFAULT_CANDIDATES_OUT = Path("config/wallets_candidates.yaml")
DEFAULT_REPORT_OUT = Path("docs/discover_wallets_report.md")
PAGE_SIZE = 50
MAX_TOP = 1050


@dataclass(frozen=True)
class DiscoverArgs:
    time_period: TimePeriod
    category: Category
    top: int
    min_volume_usdc: Decimal
    seed_path: Path
    candidates_out: Path
    report_out: Path
    dry_run: bool


def parse_args(argv: list[str] | None = None) -> DiscoverArgs:
    parser = argparse.ArgumentParser(prog="discover_wallets")
    parser.add_argument("--time-period", default="MONTH", choices=[tp.value for tp in TimePeriod])
    parser.add_argument("--category", default="OVERALL", choices=[c.value for c in Category])
    parser.add_argument("--top", type=int, default=50)
    parser.add_argument(
        "--min-volume", type=Decimal, default=Decimal("5000"), dest="min_volume_usdc"
    )
    parser.add_argument("--seed-path", type=Path, default=DEFAULT_SEED_PATH)
    parser.add_argument("--candidates-out", type=Path, default=DEFAULT_CANDIDATES_OUT)
    parser.add_argument("--report-out", type=Path, default=DEFAULT_REPORT_OUT)
    parser.add_argument("--dry-run", action="store_true")
    ns = parser.parse_args(argv)

    top = ns.top
    if top > MAX_TOP:
        print(
            f"warning: --top {top} clamped to {MAX_TOP} (API offset cap)",
            file=sys.stderr,
        )
        top = MAX_TOP
    if top < 1:
        parser.error("--top must be >= 1")

    return DiscoverArgs(
        time_period=TimePeriod(ns.time_period),
        category=Category(ns.category),
        top=top,
        min_volume_usdc=ns.min_volume_usdc,
        seed_path=ns.seed_path,
        candidates_out=ns.candidates_out,
        report_out=ns.report_out,
        dry_run=ns.dry_run,
    )


async def run_discover(
    args: DiscoverArgs,
    leaderboard: PolymarketLeaderboardPort,
    discovery_repo: DiscoveryRepository | None = None,
) -> int:
    try:
        seed = load_wallets_seed(args.seed_path)
    except FileNotFoundError as exc:
        print(f"error: seed file not found: {exc}", file=sys.stderr)
        return 1
    except (yaml.YAMLError, ValueError) as exc:
        print(f"error: cannot parse seed file {args.seed_path}: {exc}", file=sys.stderr)
        return 1
    seed_addrs: set[WalletAddress] = {w.address for w in seed}

    _log.info(
        "discover_run_started",
        time_period=args.time_period.value,
        category=args.category.value,
        top=args.top,
        min_volume_usdc=str(args.min_volume_usdc),
        seed_size=len(seed),
    )

    fetched: list[LeaderboardEntry] = []
    offset = 0
    try:
        while len(fetched) < args.top and offset <= 1000:
            page = await leaderboard.fetch_leaderboard(
                time_period=args.time_period,
                category=args.category,
                order_by=OrderBy.PNL,
                limit=PAGE_SIZE,
                offset=offset,
            )
            _log.info("leaderboard_page_fetched", offset=offset, count=len(page))
            fetched.extend(page)
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
    except httpx.HTTPStatusError as exc:
        print(
            f"error: leaderboard API failed (HTTP {exc.response.status_code}): {exc}",
            file=sys.stderr,
        )
        return 1
    except httpx.RequestError as exc:
        print(f"error: network failure to leaderboard API: {exc}", file=sys.stderr)
        return 1

    excluded_existing = sum(1 for e in fetched if e.address in seed_addrs)
    excluded_min_vol = sum(
        1 for e in fetched if e.address not in seed_addrs and e.volume_usdc < args.min_volume_usdc
    )

    candidates = filter_and_rank(
        fetched,
        min_volume_usdc=args.min_volume_usdc,
        exclude=seed_addrs,
        top_n=args.top,
    )

    _log.info(
        "discover_run_filtered",
        total_fetched=len(fetched),
        excluded_existing=excluded_existing,
        excluded_min_volume=excluded_min_vol,
        total_candidates=len(candidates),
    )

    if not candidates:
        if not fetched:
            print(
                f"error: no rows from API for time_period={args.time_period.value} "
                f"category={args.category.value}",
                file=sys.stderr,
            )
        else:
            print(
                f"error: all {len(fetched)} fetched rows were excluded "
                f"(by seed: {excluded_existing}, by min_volume: {excluded_min_vol})",
                file=sys.stderr,
            )
        return 2

    _print_table(candidates)

    if args.dry_run:
        _log.info("discover_run_completed", dry_run=True)
        return 0

    metadata = ReportMetadata(
        generated_at=datetime.now(tz=UTC),
        time_period=args.time_period,
        category=args.category,
        order_by=OrderBy.PNL,
        min_volume_usdc=args.min_volume_usdc,
        top_requested=args.top,
        seed_path=str(args.seed_path),
        seed_size=len(seed),
        total_fetched=len(fetched),
        total_excluded_existing=excluded_existing,
        total_excluded_min_volume=excluded_min_vol,
        total_candidates=len(candidates),
    )

    args.candidates_out.write_text(
        render_candidates_yaml(candidates),
        encoding="utf-8",
    )
    args.report_out.write_text(
        render_report_md(candidates, metadata=metadata),
        encoding="utf-8",
    )
    if discovery_repo is not None:
        await discovery_repo.insert_run(metadata, candidates)
    _log.info(
        "discover_run_completed",
        dry_run=False,
        candidates_path=str(args.candidates_out),
        report_path=str(args.report_out),
        persisted_to_db=discovery_repo is not None,
    )
    return 0


def _print_table(candidates: list[CandidateWallet]) -> None:
    print(f"{'rank':>4}  {'label':<24}  {'address':<44}  {'volume':>14}  {'pnl':>12}")
    print("-" * 110)
    for c in candidates:
        print(
            f"{c.rank:>4}  {c.label[:24]:<24}  {c.address.value:<44}  "
            f"{c.volume_usdc:>14,.2f}  {c.pnl_usdc:>+12,.2f}"
        )


async def _async_main(argv: list[str] | None = None) -> int:
    from polycopy.config import Settings
    from polycopy.infrastructure.observability.logging import configure_logging
    from polycopy.infrastructure.observability.metrics import make_metrics
    from polycopy.infrastructure.persistence.database import (
        make_engine,
        make_session_factory,
    )
    from polycopy.infrastructure.persistence.discovery_repository import (
        SqlAlchemyDiscoveryRepository,
    )
    from polycopy.infrastructure.polymarket.leaderboard_client import (
        PolymarketLeaderboardClient,
    )

    args = parse_args(argv)
    settings = Settings()
    configure_logging(env=settings.env, level=settings.log_level)
    metrics = make_metrics()
    client = PolymarketLeaderboardClient(
        base_url=settings.polymarket_base_url,
        metrics=metrics,
    )

    # Dry-run não persiste no DB (mesma semantica de não escrever arquivos).
    if args.dry_run:
        return await run_discover(args, client)

    engine = make_engine(settings)
    session_factory = make_session_factory(engine)
    try:
        async with session_factory() as session:
            repo = SqlAlchemyDiscoveryRepository(session)
            exit_code = await run_discover(args, client, discovery_repo=repo)
            await session.commit()
        return exit_code
    finally:
        await engine.dispose()


def main() -> None:
    sys.exit(asyncio.run(_async_main()))


if __name__ == "__main__":
    main()
