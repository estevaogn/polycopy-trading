"""SQLAlchemy ORM models. Não vazam pra fora do package `persistence/`."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, Numeric, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


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
