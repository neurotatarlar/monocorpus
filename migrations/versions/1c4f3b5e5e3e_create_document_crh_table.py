"""create document_crh table

Revision ID: 1c4f3b5e5e3e
Revises: 8ddd9201287a
Create Date: 2025-11-27 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1c4f3b5e5e3e'
down_revision: Union[str, Sequence[str], None] = '8ddd9201287a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'document_crh',
        sa.Column('md5', sa.String(), nullable=False, primary_key=True),
        sa.Column('mime_type', sa.String(), nullable=True),
        sa.Column('ya_path', sa.String(), nullable=True),
        sa.Column('ya_public_url', sa.String(), nullable=True),
        sa.Column('ya_public_key', sa.String(), nullable=True),
        sa.Column('ya_resource_id', sa.String(), nullable=True),
        sa.Column('publisher', sa.String(), nullable=True),
        sa.Column('author', sa.String(), nullable=True),
        sa.Column('title', sa.String(), nullable=True),
        sa.Column('isbn', sa.String(), nullable=True),
        sa.Column('publish_date', sa.String(), nullable=True),
        sa.Column('language', sa.String(), nullable=True),
        sa.Column('genre', sa.String(), nullable=True),
        sa.Column('translated', sa.Boolean(), nullable=True),
        sa.Column('page_count', sa.Integer(), nullable=True),
        sa.Column('content_extraction_method', sa.String(), nullable=True),
        sa.Column('metadata_extraction_method', sa.String(), nullable=True),
        sa.Column('full', sa.Boolean(), nullable=True),
        sa.Column('sharing_restricted', sa.Boolean(), nullable=True),
        sa.Column('document_url', sa.String(), nullable=True),
        sa.Column('content_url', sa.String(), nullable=True),
        sa.Column('metadata_json', sa.JSON(), nullable=True),
        sa.Column('upstream_metadata_url', sa.String(), nullable=True),
        sa.Column('in_library', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.PrimaryKeyConstraint('md5')
    )
    op.create_index(op.f('ix_document_crh_md5'), 'document_crh', ['md5'], unique=True)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_document_crh_md5'), table_name='document_crh')
    op.drop_table('document_crh')
