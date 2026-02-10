"""Drop legacy scalar metadata columns from document

Revision ID: 6b91f1f5f4df
Revises: b3a52b39c8cc
Create Date: 2026-02-10 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "6b91f1f5f4df"
down_revision: Union[str, Sequence[str], None] = "b3a52b39c8cc"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_column("document", "publisher")
    op.drop_column("document", "author")
    op.drop_column("document", "title")
    op.drop_column("document", "isbn")
    op.drop_column("document", "publish_date")
    op.drop_column("document", "genre")


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column("document", sa.Column("genre", sa.String(), nullable=True))
    op.add_column("document", sa.Column("publish_date", sa.String(), nullable=True))
    op.add_column("document", sa.Column("isbn", sa.String(), nullable=True))
    op.add_column("document", sa.Column("title", sa.String(), nullable=True))
    op.add_column("document", sa.Column("author", sa.String(), nullable=True))
    op.add_column("document", sa.Column("publisher", sa.String(), nullable=True))
