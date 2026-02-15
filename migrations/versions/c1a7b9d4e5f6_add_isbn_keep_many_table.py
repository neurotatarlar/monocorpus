"""Add isbn_keep_many table for persisted keep-many ISBN dedup decisions

Revision ID: c1a7b9d4e5f6
Revises: 5b2f6c3a1d4e
Create Date: 2026-02-14 00:00:00.000000

"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c1a7b9d4e5f6"
down_revision: Union[str, Sequence[str], None] = "5b2f6c3a1d4e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "isbn_keep_many",
        sa.Column("isbn_key", sa.String(), nullable=False),
        sa.Column("md5", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("isbn_key", "md5"),
    )
    op.create_index("ix_isbn_keep_many_isbn_key", "isbn_keep_many", ["isbn_key"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_isbn_keep_many_isbn_key", table_name="isbn_keep_many")
    op.drop_table("isbn_keep_many")
