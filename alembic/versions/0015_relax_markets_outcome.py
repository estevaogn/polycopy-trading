"""drop markets.outcome CHECK constraint to support non-binary markets

Polymarket tem mercados não-binários (ex: sport teams "Phillies vs Marlins"
com outcomes ["Phillies","Marlins"], multi-option, etc). Antes filtrávamos
esses no `_row_to_markets` porque o schema CHECK constraint exigia
outcome IN ('Yes', 'No'). Agora aceitamos qualquer string non-empty
pra cobrir descrições de mercados que apareceram em wallet_trades.

Revision ID: 0015
Revises: 0014
Create Date: 2026-05-05 00:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint("markets_outcome_enum", "markets", type_="check")
    op.create_check_constraint(
        "markets_outcome_nonempty",
        "markets",
        "length(outcome) > 0",
    )


def downgrade() -> None:
    # Restaura constraint original. Pode falhar se houver rows com outcome
    # diferente de Yes/No — operador deve limpar primeiro com:
    #   DELETE FROM markets WHERE outcome NOT IN ('Yes','No');
    op.drop_constraint("markets_outcome_nonempty", "markets", type_="check")
    op.create_check_constraint(
        "markets_outcome_enum",
        "markets",
        "outcome IN ('Yes', 'No')",
    )
