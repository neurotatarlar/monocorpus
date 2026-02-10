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
        sa.PrimaryKeyConstraint("md5"),
    )

    bind = op.get_bind()
    has_unique_md5 = bool(
        bind.execute(
            sa.text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_constraint c
                    JOIN pg_class t ON t.oid = c.conrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    WHERE t.relname = 'document'
                      AND n.nspname = current_schema()
                      AND c.contype IN ('p', 'u')
                      AND c.conkey = ARRAY[
                          (
                              SELECT a.attnum
                              FROM pg_attribute a
                              WHERE a.attrelid = t.oid
                                AND a.attname = 'md5'
                                AND a.attnum > 0
                                AND NOT a.attisdropped
                              LIMIT 1
                          )
                      ]::smallint[]
                )
                """
            )
        ).scalar()
    )
    if has_unique_md5:
        op.create_foreign_key(
            "fk_metadata_md5_document_md5",
            "metadata",
            "document",
            ["md5"],
            ["md5"],
            ondelete="CASCADE",
        )

    op.execute(
        sa.text(
            """
            INSERT INTO metadata (md5, schema_org)
            SELECT DISTINCT ON (md5) md5, meta
            FROM document
            WHERE meta IS NOT NULL
            ORDER BY md5
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
