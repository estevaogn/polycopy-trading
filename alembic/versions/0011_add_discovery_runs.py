"""add discovery_runs + discovery_candidates tables

Persiste cada execução do `discover_wallets` CLI no DB pra que o dashboard
Grafana possa exibir candidates históricos com link pro perfil Polymarket.

Revision ID: 0011
Revises: 0010
Create Date: 2026-05-05 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "discovery_runs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("time_period", sa.String(), nullable=False),
        sa.Column("category", sa.String(), nullable=False),
        sa.Column("order_by", sa.String(), nullable=False, server_default="PNL"),
        sa.Column("top_requested", sa.Integer(), nullable=False),
        sa.Column("min_volume_usdc", sa.Numeric(20, 8), nullable=False),
        sa.Column("seed_path", sa.String(), nullable=False),
        sa.Column("seed_size", sa.Integer(), nullable=False),
        sa.Column("total_fetched", sa.Integer(), nullable=False),
        sa.Column("total_excluded_existing", sa.Integer(), nullable=False),
        sa.Column("total_excluded_min_volume", sa.Integer(), nullable=False),
        sa.Column("total_candidates", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "idx_discovery_runs_generated_at",
        "discovery_runs",
        ["generated_at"],
    )

    op.create_table(
        "discovery_candidates",
        sa.Column(
            "run_id",
            sa.BigInteger(),
            sa.ForeignKey("discovery_runs.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("rank", sa.Integer(), primary_key=True),
        sa.Column("address", sa.String(), nullable=False),
        sa.Column("label", sa.String(), nullable=False),
        sa.Column("volume_usdc", sa.Numeric(20, 8), nullable=False),
        sa.Column("pnl_usdc", sa.Numeric(20, 8), nullable=False),
        sa.Column(
            "verified_badge",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.CheckConstraint("rank >= 1", name="discovery_candidates_rank_positive"),
    )
    op.create_index(
        "idx_discovery_candidates_address",
        "discovery_candidates",
        ["address"],
    )


def downgrade() -> None:
    op.drop_index("idx_discovery_candidates_address", table_name="discovery_candidates")
    op.drop_table("discovery_candidates")
    op.drop_index("idx_discovery_runs_generated_at", table_name="discovery_runs")
    op.drop_table("discovery_runs")
