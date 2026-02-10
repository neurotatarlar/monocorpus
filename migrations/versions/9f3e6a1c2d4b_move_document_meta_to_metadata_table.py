"""Move document.meta/lib into dedicated metadata table

Revision ID: 9f3e6a1c2d4b
Revises: 6b91f1f5f4df
Create Date: 2026-02-10 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "9f3e6a1c2d4b"
down_revision: Union[str, Sequence[str], None] = "6b91f1f5f4df"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "metadata",
        sa.Column("md5", sa.String(), nullable=False),
        sa.Column("schema_org", sa.JSON(), nullable=True),
        sa.Column("lib", sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(["md5"], ["document.md5"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("md5"),
    )

    op.execute(
        sa.text(
            """
            INSERT INTO metadata (md5, schema_org)
            SELECT md5, meta
            FROM document
            WHERE meta IS NOT NULL
            """
        )
    )

    op.drop_column("document", "meta")
    op.drop_column("document", "lib")


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column("document", sa.Column("meta", sa.JSON(), nullable=True))
    op.add_column("document", sa.Column("lib", sa.JSON(), nullable=True))

    op.execute(
        sa.text(
            """
            UPDATE document AS d
            SET meta = m.schema_org
            FROM metadata AS m
            WHERE d.md5 = m.md5
            """
        )
    )

    op.execute(
        sa.text(
            """
            UPDATE document AS d
            SET lib = '{"applicable": true}'::json
            FROM metadata AS m
            WHERE d.md5 = m.md5 AND m.lib IS TRUE
            """
        )
    )

    op.drop_table("metadata")
