"""SQLAlchemy ORM models. Não vazam pra fora do package `persistence/`."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Boolean, CheckConstraint, DateTime, Index, Integer, Numeric, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func
from sqlalchemy.sql import text as _sql_text


def sa_text_false() -> Any:
    """Server default 'false' compatível com SQLAlchemy 2.x."""
    return _sql_text("false")


class Base(DeclarativeBase):
    pass


class WalletTradeRow(Base):
    __tablename__ = "wallet_trades"

    tx_hash: Mapped[str] = mapped_column(String, primary_key=True)
    log_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    wallet: Mapped[str] = mapped_column(String, nullable=False)
    condition_id: Mapped[str] = mapped_column(String, nullable=False)
    token_id: Mapped[str] = mapped_column(String, nullable=False)
    side: Mapped[str] = mapped_column(String, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    size_usdc: Mapped[Decimal] = mapped_column(Numeric(28, 6), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    inserted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint("log_index >= 0", name="wallet_trades_log_index_nonneg"),
        CheckConstraint("side IN ('BUY', 'SELL')", name="wallet_trades_side_enum"),
        CheckConstraint("price >= 0 AND price <= 1", name="wallet_trades_price_range"),
        Index(
            "wallet_trades_wallet_occurred_at_idx",
            "wallet",
            "occurred_at",
            postgresql_using="btree",
        ),
    )


class MarketRow(Base):
    __tablename__ = "markets"

    token_id: Mapped[str] = mapped_column(String, primary_key=True)
    condition_id: Mapped[str] = mapped_column(String, nullable=False)
    question: Mapped[str] = mapped_column(String, nullable=False)
    slug: Mapped[str | None] = mapped_column(String, nullable=True)
    outcome: Mapped[str] = mapped_column(String, nullable=False)
    end_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_archived: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=sa_text_false()
    )
    volume_24h_usdc: Mapped[Decimal | None] = mapped_column(Numeric(20, 6), nullable=True)
    liquidity_usdc: Mapped[Decimal | None] = mapped_column(Numeric(20, 6), nullable=True)
    last_synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("outcome IN ('Yes', 'No')", name="markets_outcome_enum"),
        CheckConstraint(
            "NOT (is_active AND is_archived)", name="markets_active_archived_exclusive"
        ),
        Index("idx_markets_condition_id", "condition_id"),
        Index(
            "idx_markets_active_end_date",
            "end_date",
            postgresql_where="is_active = true",
        ),
        Index(
            "idx_markets_volume_24h",
            _sql_text("volume_24h_usdc DESC NULLS LAST"),
            postgresql_where="is_active = true",
            postgresql_using="btree",
        ),
    )
