"""No-op legacy revision

Revision ID: 1c4f3b5e5e3e
Revises: 8ddd9201287a
Create Date: 2025-11-27 00:00:00.000000

"""
from typing import Sequence, Union

# revision identifiers, used by Alembic.
revision: str = '1c4f3b5e5e3e'
down_revision: Union[str, Sequence[str], None] = '8ddd9201287a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
