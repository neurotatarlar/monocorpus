"""Rename metadata_json, drop in_library, add lib columns

Revision ID: a2b4d9c0d7f9
Revises: 1c4f3b5e5e3e
Create Date: 2025-12-03 15:55:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a2b4d9c0d7f9'
down_revision: Union[str, Sequence[str], None] = '1c4f3b5e5e3e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _upgrade_table(table_name: str) -> None:
    op.alter_column(table_name, 'metadata_json', new_column_name='metadata')
    op.alter_column(
        table_name,
        'metadata',
        type_=sa.JSON(),
        postgresql_using='metadata::json'
    )
    op.drop_column(table_name, 'in_library')
    op.add_column(table_name, sa.Column('lib', sa.JSON(), nullable=True))


def _downgrade_table(table_name: str) -> None:
    op.drop_column(table_name, 'lib')
    op.add_column(
        table_name,
        sa.Column('in_library', sa.Boolean(), nullable=False, server_default=sa.false())
    )
    op.alter_column(
        table_name,
        'metadata',
        type_=sa.Text(),
        postgresql_using='metadata::text'
    )
    op.alter_column(table_name, 'metadata', new_column_name='metadata_json')


def upgrade() -> None:
    """Upgrade schema."""
    _upgrade_table('document')
    _upgrade_table('document_crh')


def downgrade() -> None:
    """Downgrade schema."""
    _downgrade_table('document_crh')
    _downgrade_table('document')
