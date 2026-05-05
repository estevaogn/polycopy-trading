"""add tracked_wallets table — espelho persistido do wallets_seed.yaml

Watcher sincroniza o seed pro DB no startup pra que o dashboard Grafana
possa mostrar lista completa de wallets monitoradas (incluindo as sem
trades ainda) com os labels humanos do YAML.

Revision ID: 0013
Revises: 0012
Create Date: 2026-05-05 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "tracked_wallets",
        sa.Column("address", sa.String(), primary_key=True),
        sa.Column("label", sa.String(), nullable=False),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "last_synced_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )


def downgrade() -> None:
    op.drop_table("tracked_wallets")
