"""Drop page_count and translated from document

Revision ID: e1f2a3b4c5d6
Revises: 9f3e6a1c2d4b
Create Date: 2026-02-10 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e1f2a3b4c5d6"
down_revision: Union[str, Sequence[str], None] = "9f3e6a1c2d4b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_column("document", "page_count")
    op.drop_column("document", "translated")


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column("document", sa.Column("translated", sa.Boolean(), nullable=True))
    op.add_column("document", sa.Column("page_count", sa.Integer(), nullable=True))

    op.execute(
        sa.text(
            """
            UPDATE document AS d
            SET page_count = CASE
                WHEN (m.schema_org::jsonb ? 'numberOfPages')
                 AND (m.schema_org::jsonb->>'numberOfPages') ~ '^\\d+$'
                THEN (m.schema_org::jsonb->>'numberOfPages')::integer
                ELSE NULL
            END
            FROM metadata AS m
            WHERE d.md5 = m.md5
            """
        )
    )

    op.execute(
        sa.text(
            """
            UPDATE document AS d
            SET translated = CASE
                WHEN m.schema_org IS NULL THEN NULL
                WHEN EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(COALESCE(m.schema_org::jsonb->'contributor', '[]'::jsonb)) AS c
                    WHERE lower(COALESCE(c->>'role', '')) = 'translator'
                ) THEN TRUE
                ELSE FALSE
            END
            FROM metadata AS m
            WHERE d.md5 = m.md5
            """
        )
    )
