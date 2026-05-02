"""add risk_decisions table

Revision ID: 0003
Revises: 0002
Create Date: 2026-05-02 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "risk_decisions",
        sa.Column("trade_event_id", sa.Uuid(), nullable=False),
        sa.Column("wallet", sa.String(), nullable=False),
        sa.Column("condition_id", sa.String(), nullable=False),
        sa.Column("token_id", sa.String(), nullable=False),
        sa.Column("decision", sa.String(), nullable=False),
        sa.Column("reason", sa.String(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "decision IN ('approved', 'rejected')",
            name="risk_decisions_decision_enum",
        ),
        sa.CheckConstraint(
            "(decision = 'approved' AND reason IS NULL) "
            "OR (decision = 'rejected' AND reason IS NOT NULL)",
            name="risk_decisions_reason_consistency",
        ),
        sa.PrimaryKeyConstraint("trade_event_id"),
    )
    op.create_index(
        "idx_risk_decisions_wallet_decided_at",
        "risk_decisions",
        ["wallet", "decided_at"],
    )
    op.create_index(
        "idx_risk_decisions_rejected_decided_at",
        "risk_decisions",
        ["decided_at"],
        postgresql_where=sa.text("decision = 'rejected'"),
    )


def downgrade() -> None:
    op.drop_index("idx_risk_decisions_rejected_decided_at", table_name="risk_decisions")
    op.drop_index("idx_risk_decisions_wallet_decided_at", table_name="risk_decisions")
    op.drop_table("risk_decisions")
