"""add notifier_config key/value table for hot-reload notifier filter

Tabela genérica de KV pra config do notifier que pode ser editada sem
restart. Notifier pollla a cada 30s e aplica imediatamente.

Default `min_size_usdc=50` (filtra trades < $50 USDC).

Revision ID: 0012
Revises: 0011
Create Date: 2026-05-05 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "notifier_config",
        sa.Column("key", sa.String(), primary_key=True),
        sa.Column("value", sa.String(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("updated_by", sa.String(), nullable=True),
    )
    # Default: filtra trades < $50 USDC. Operador ajusta via CLI.
    op.execute(
        sa.text(
            "INSERT INTO notifier_config (key, value, updated_by) "
            "VALUES ('min_size_usdc', '50', 'migration_0012')"
        )
    )


def downgrade() -> None:
    op.drop_table("notifier_config")
