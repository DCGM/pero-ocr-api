"""Initial schema matching the existing database.

Revision ID: 001_initial
Revises:
Create Date: 2026-02-25

This migration creates the full PERO-OCR-API schema from scratch.
For existing databases that already have these tables, run:

    alembic stamp 001_initial

to mark the database as current without executing the migration.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- api_key ---
    op.create_table(
        "api_key",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("api_string", sa.String(), nullable=False, index=True),
        sa.Column("owner", sa.String(), nullable=False),
        sa.Column(
            "permission",
            sa.Enum("SUPER_USER", "USER", name="permission"),
            nullable=False,
        ),
        sa.Column("suspension", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="1"),
    )

    # --- engine ---
    op.create_table(
        "engine",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
    )

    # --- engine_version ---
    op.create_table(
        "engine_version",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("version", sa.String(), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column(
            "engine_id",
            sa.Integer(),
            sa.ForeignKey("engine.id"),
            nullable=False,
        ),
    )

    # --- model ---
    op.create_table(
        "model",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("config", sa.String(), nullable=False),
    )

    # --- engine_version_model ---
    op.create_table(
        "engine_version_model",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "engine_version_id",
            sa.Integer(),
            sa.ForeignKey("engine_version.id"),
            nullable=False,
        ),
        sa.Column(
            "model_id",
            sa.Integer(),
            sa.ForeignKey("model.id"),
            nullable=False,
        ),
    )

    # --- request ---
    op.create_table(
        "request",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "creation_timestamp",
            sa.DateTime(),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "modification_timestamp",
            sa.DateTime(),
            nullable=False,
            index=True,
        ),
        sa.Column("finish_timestamp", sa.DateTime(), nullable=True, index=True),
        sa.Column(
            "engine_id",
            sa.Integer(),
            sa.ForeignKey("engine.id"),
            nullable=False,
        ),
        sa.Column(
            "api_key_id",
            sa.Integer(),
            sa.ForeignKey("api_key.id"),
            nullable=False,
        ),
    )

    # --- page ---
    op.create_table(
        "page",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False, index=True),
        sa.Column("url", sa.String(), nullable=True),
        sa.Column(
            "state",
            sa.Enum(
                "CREATED", "WAITING", "PROCESSING", "NOT_FOUND",
                "INVALID_FILE", "PROCESSING_FAILED", "PROCESSED",
                "CANCELED", "EXPIRED",
                name="pagestate",
            ),
            nullable=False,
            index=True,
        ),
        sa.Column("score", sa.Float(), nullable=True, index=True),
        sa.Column("traceback", sa.String(), nullable=True),
        sa.Column(
            "waiting_timestamp", sa.DateTime(), nullable=True, index=True,
        ),
        sa.Column("processing_timestamp", sa.DateTime(), nullable=True),
        sa.Column(
            "finish_timestamp", sa.DateTime(), nullable=True, index=True,
        ),
        sa.Column(
            "request_id",
            sa.Uuid(),
            sa.ForeignKey("request.id"),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "engine_version_id",
            sa.Integer(),
            sa.ForeignKey("engine_version.id"),
            nullable=True,
            index=True,
        ),
    )

    # --- notification ---
    op.create_table(
        "notification",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("last_notification", sa.DateTime(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("page")
    op.drop_table("request")
    op.drop_table("engine_version_model")
    op.drop_table("model")
    op.drop_table("engine_version")
    op.drop_table("engine")
    op.drop_table("notification")
    op.drop_table("api_key")
