"""Add include_in_library column to Document

Revision ID: 8ddd9201287a
Revises: cd919af6109f
Create Date: 2025-11-05 16:01:10.117789

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8ddd9201287a'
down_revision: Union[str, Sequence[str], None] = 'cd919af6109f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'document',
        sa.Column('in_library', sa.Boolean(), nullable=False, server_default=sa.false())
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('document', 'in_library')

