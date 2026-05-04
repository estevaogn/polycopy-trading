"""Port for Polymarket leaderboard endpoint (/v1/leaderboard)."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.discovery import (
    Category,
    LeaderboardEntry,
    OrderBy,
    TimePeriod,
)


class PolymarketLeaderboardPort(Protocol):
    """Read-only access to Polymarket's public trader leaderboard."""

    async def fetch_leaderboard(
        self,
        *,
        time_period: TimePeriod,
        category: Category,
        order_by: OrderBy = OrderBy.PNL,
        limit: int = 50,
        offset: int = 0,
    ) -> list[LeaderboardEntry]: ...
