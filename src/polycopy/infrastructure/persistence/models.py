"""SQLAlchemy ORM models. Não vazam pra fora do package `persistence/`."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    Uuid,
)
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


class RiskDecisionRow(Base):
    __tablename__ = "risk_decisions"

    trade_event_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    wallet: Mapped[str] = mapped_column(String, nullable=False)
    condition_id: Mapped[str] = mapped_column(String, nullable=False)
    token_id: Mapped[str] = mapped_column(String, nullable=False)
    decision: Mapped[str] = mapped_column(String, nullable=False)
    reason: Mapped[str | None] = mapped_column(String, nullable=True)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "decision IN ('approved', 'rejected')",
            name="risk_decisions_decision_enum",
        ),
        CheckConstraint(
            "(decision = 'approved' AND reason IS NULL) "
            "OR (decision = 'rejected' AND reason IS NOT NULL)",
            name="risk_decisions_reason_consistency",
        ),
        Index(
            "idx_risk_decisions_wallet_decided_at",
            "wallet",
            "decided_at",
            postgresql_using="btree",
        ),
        Index(
            "idx_risk_decisions_rejected_decided_at",
            "decided_at",
            postgresql_where="decision = 'rejected'",
            postgresql_using="btree",
        ),
    )


class OrderSizingRow(Base):
    __tablename__ = "order_sizings"

    trade_event_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    wallet: Mapped[str] = mapped_column(String, nullable=False)
    condition_id: Mapped[str] = mapped_column(String, nullable=False)
    token_id: Mapped[str] = mapped_column(String, nullable=False)
    original_size_usdc: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    final_size_usdc: Mapped[Decimal | None] = mapped_column(Numeric(20, 6), nullable=True)
    decision: Mapped[str] = mapped_column(String, nullable=False)
    reason: Mapped[str | None] = mapped_column(String, nullable=True)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "decision IN ('sized', 'skipped')",
            name="order_sizings_decision_enum",
        ),
        CheckConstraint(
            "(decision = 'sized' AND final_size_usdc IS NOT NULL AND reason IS NULL) "
            "OR (decision = 'skipped' AND final_size_usdc IS NULL AND reason IS NOT NULL)",
            name="order_sizings_consistency",
        ),
        CheckConstraint(
            "original_size_usdc > 0 AND (final_size_usdc IS NULL OR final_size_usdc > 0)",
            name="order_sizings_size_positive",
        ),
        Index(
            "idx_order_sizings_wallet_decided_at",
            "wallet",
            "decided_at",
            postgresql_using="btree",
        ),
        Index(
            "idx_order_sizings_skipped_decided_at",
            "decided_at",
            postgresql_where="decision = 'skipped'",
            postgresql_using="btree",
        ),
    )


class OrderExecutionRow(Base):
    __tablename__ = "order_executions"

    trade_event_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    wallet: Mapped[str] = mapped_column(String, nullable=False)
    condition_id: Mapped[str] = mapped_column(String, nullable=False)
    token_id: Mapped[str] = mapped_column(String, nullable=False)
    final_size_usdc: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    mode: Mapped[str] = mapped_column(String, nullable=False)
    result: Mapped[str] = mapped_column(String, nullable=False)
    tx_hash: Mapped[str | None] = mapped_column(String, nullable=True)
    gas_wei: Mapped[Decimal | None] = mapped_column(Numeric(40, 0), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    error_message: Mapped[str | None] = mapped_column(String, nullable=True)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "mode IN ('real', 'dry_run')",
            name="order_executions_mode_enum",
        ),
        CheckConstraint(
            "result IN ('executed', 'failed', 'dry_run')",
            name="order_executions_result_enum",
        ),
        CheckConstraint(
            "(mode = 'real' AND result IN ('executed', 'failed')) "
            "OR (mode = 'dry_run' AND result IN ('dry_run', 'failed'))",
            name="order_executions_mode_result_consistency",
        ),
        CheckConstraint(
            "(result = 'executed' AND tx_hash IS NOT NULL) OR result IN ('failed', 'dry_run')",
            name="order_executions_executed_has_tx",
        ),
        CheckConstraint(
            "(result = 'failed' AND failure_reason IS NOT NULL AND error_message IS NOT NULL) "
            "OR result IN ('executed', 'dry_run')",
            name="order_executions_failed_has_reason",
        ),
        CheckConstraint(
            "(result = 'dry_run' AND tx_hash IS NULL AND gas_wei IS NULL "
            "AND failure_reason IS NULL) "
            "OR result IN ('executed', 'failed')",
            name="order_executions_dry_run_no_tx",
        ),
        CheckConstraint(
            "final_size_usdc > 0",
            name="order_executions_size_positive",
        ),
        Index(
            "idx_order_executions_wallet_decided_at",
            "wallet",
            "decided_at",
            postgresql_using="btree",
        ),
        Index(
            "idx_order_executions_failed_decided_at",
            "decided_at",
            postgresql_where="result = 'failed'",
            postgresql_using="btree",
        ),
        Index(
            "idx_order_executions_real_executed",
            "decided_at",
            postgresql_where="mode = 'real' AND result = 'executed'",
            postgresql_using="btree",
        ),
    )
