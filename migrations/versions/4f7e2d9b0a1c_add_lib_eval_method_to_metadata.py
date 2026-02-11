"""Add metadata evaluation method and normalized classification table

Revision ID: 4f7e2d9b0a1c
Revises: e1f2a3b4c5d6
Create Date: 2026-02-11 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = "4f7e2d9b0a1c"
down_revision: Union[str, Sequence[str], None] = "e1f2a3b4c5d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("metadata", sa.Column("lib_eval_method", sa.String(), nullable=True))
    op.create_table(
        "classification",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("ddc", sa.String(), nullable=False),
        sa.Column("path_en", sa.JSON(), nullable=False),
        sa.Column("path_en_key", sa.String(), nullable=False),
        sa.Column("path_tt", sa.JSON(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("created_by", sa.String(), nullable=False, server_default="gemini"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ddc", "path_en_key", name="uq_classification_ddc_path_en_key"),
    )
    op.create_index("ix_classification_ddc", "classification", ["ddc"])

    op.add_column("metadata", sa.Column("classification_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_metadata_classification_id",
        "metadata",
        "classification",
        ["classification_id"],
        ["id"],
        ondelete="SET NULL",
    )

    # Backward-compatibility: if an older experimental column exists, migrate and remove it.
    bind = op.get_bind()
    metadata_columns = {c["name"] for c in inspect(bind).get_columns("metadata")}
    if "library_classification" in metadata_columns:
        bind.execute(
            sa.text(
                """
                INSERT INTO classification (ddc, path_en, path_en_key, status, created_by)
                SELECT DISTINCT
                    TRIM(m.library_classification::jsonb->>'ddc') AS ddc,
                    m.library_classification::jsonb->'path' AS path_en,
                    (
                        SELECT array_to_string(
                            ARRAY(
                                SELECT lower(trim(v))
                                FROM jsonb_array_elements_text(m.library_classification::jsonb->'path') AS p(v)
                                WHERE trim(v) <> ''
                            ),
                            '|'
                        )
                    ) AS path_en_key,
                    'pending' AS status,
                    'migration' AS created_by
                FROM metadata AS m
                WHERE m.library_classification IS NOT NULL
                  AND jsonb_typeof(m.library_classification::jsonb) = 'object'
                  AND (m.library_classification::jsonb ? 'ddc')
                  AND (m.library_classification::jsonb ? 'path')
                  AND jsonb_typeof(m.library_classification::jsonb->'path') = 'array'
                  AND jsonb_array_length(m.library_classification::jsonb->'path') >= 2
                  AND TRIM(m.library_classification::jsonb->>'ddc') ~ '^[0-9]{3}(\\.[0-9]+)?$'
                ON CONFLICT (ddc, path_en_key) DO NOTHING
                """
            )
        )

        bind.execute(
            sa.text(
                """
                UPDATE metadata AS m
                SET classification_id = c.id
                FROM classification AS c
                WHERE m.library_classification IS NOT NULL
                  AND jsonb_typeof(m.library_classification::jsonb) = 'object'
                  AND c.ddc = TRIM(m.library_classification::jsonb->>'ddc')
                  AND c.path_en_key = (
                        SELECT array_to_string(
                            ARRAY(
                                SELECT lower(trim(v))
                                FROM jsonb_array_elements_text(m.library_classification::jsonb->'path') AS p(v)
                                WHERE trim(v) <> ''
                            ),
                            '|'
                        )
                    )
                """
            )
        )

        op.drop_column("metadata", "library_classification")


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint("fk_metadata_classification_id", "metadata", type_="foreignkey")
    op.drop_column("metadata", "classification_id")
    op.drop_index("ix_classification_ddc", table_name="classification")
    op.drop_table("classification")
    op.drop_column("metadata", "lib_eval_method")
