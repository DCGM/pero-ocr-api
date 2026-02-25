"""Add processing credits: balance, pending_cost, cost_per_page, page cost, credit_transaction table.

Revision ID: 002_add_credits
Revises: 001_initial
Create Date: 2026-02-25
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "002_add_credits"
down_revision: Union[str, None] = "001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ApiKey: credit_balance and pending_cost
    # Use server_default="5000" so existing API keys are seeded with 5000 credits,
    # then drop the server default — new keys will use the ORM default of 0.0
    # (admins top-up explicitly).
    op.add_column(
        "api_key",
        sa.Column("credit_balance", sa.Float(), nullable=False, server_default="5000"),
    )
    op.alter_column("api_key", "credit_balance", server_default=None)

    op.add_column(
        "api_key",
        sa.Column("pending_cost", sa.Float(), nullable=False, server_default="0"),
    )
    op.alter_column("api_key", "pending_cost", server_default=None)

    # Engine: cost_per_page
    op.add_column(
        "engine",
        sa.Column("cost_per_page", sa.Float(), nullable=False, server_default="0"),
    )
    op.alter_column("engine", "cost_per_page", server_default=None)

    # Page: cost snapshot
    op.add_column(
        "page",
        sa.Column("cost", sa.Float(), nullable=False, server_default="0"),
    )
    op.alter_column("page", "cost", server_default=None)

    # credit_transaction table
    op.create_table(
        "credit_transaction",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "api_key_id",
            sa.Integer(),
            sa.ForeignKey("api_key.id"),
            nullable=False,
        ),
        sa.Column("amount", sa.Float(), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column(
            "admin_api_key_id",
            sa.Integer(),
            sa.ForeignKey("api_key.id"),
            nullable=True,
        ),
        sa.Column("note", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("credit_transaction")
    op.drop_column("page", "cost")
    op.drop_column("engine", "cost_per_page")
    op.drop_column("api_key", "pending_cost")
    op.drop_column("api_key", "credit_balance")
