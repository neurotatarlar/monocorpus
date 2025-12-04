"""Rename metadata column to meta

Revision ID: b3a52b39c8cc
Revises: a2b4d9c0d7f9
Create Date: 2025-12-03 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3a52b39c8cc'
down_revision: Union[str, Sequence[str], None] = 'a2b4d9c0d7f9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _rename_column(table_name: str, old: str, new: str, type_) -> None:
    op.alter_column(table_name, old, new_column_name=new, existing_type=type_)


def upgrade() -> None:
    """Upgrade schema."""
    _rename_column('document', 'metadata', 'meta', sa.JSON())
    _rename_column('document_crh', 'metadata', 'meta', sa.JSON())
    _rename_column('document', 'metadata_extraction_method', 'meta_extraction_method', sa.String())
    _rename_column('document_crh', 'metadata_extraction_method', 'meta_extraction_method', sa.String())
    _rename_column('document', 'upstream_metadata_url', 'upstream_meta_url', sa.String())
    _rename_column('document_crh', 'upstream_metadata_url', 'upstream_meta_url', sa.String())


def downgrade() -> None:
    """Downgrade schema."""
    _rename_column('document_crh', 'upstream_meta_url', 'upstream_metadata_url', sa.String())
    _rename_column('document', 'upstream_meta_url', 'upstream_metadata_url', sa.String())
    _rename_column('document_crh', 'meta_extraction_method', 'metadata_extraction_method', sa.String())
    _rename_column('document', 'meta_extraction_method', 'metadata_extraction_method', sa.String())
    _rename_column('document_crh', 'meta', 'metadata', sa.JSON())
    _rename_column('document', 'meta', 'metadata', sa.JSON())
