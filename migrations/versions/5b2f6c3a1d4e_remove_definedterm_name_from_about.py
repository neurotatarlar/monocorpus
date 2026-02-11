"""Remove `name` from schema_org.about DefinedTerm entries

Revision ID: 5b2f6c3a1d4e
Revises: 2dfcb8f9a741
Create Date: 2026-02-11 00:00:00.000000

"""

from __future__ import annotations

from typing import Any, Sequence, Union
import json

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5b2f6c3a1d4e"
down_revision: Union[str, Sequence[str], None] = "2dfcb8f9a741"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _parse_schema_org(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    rows = bind.execute(sa.text("SELECT md5, schema_org FROM metadata WHERE schema_org IS NOT NULL")).fetchall()
    update_stmt = sa.text("UPDATE metadata SET schema_org = CAST(:schema_org AS jsonb) WHERE md5 = :md5")

    for row in rows:
        schema = _parse_schema_org(row.schema_org)
        if not schema:
            continue
        about = schema.get("about")
        if not isinstance(about, list):
            continue

        changed = False
        for item in about:
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("@type") or "").strip().casefold()
            if item_type != "definedterm":
                continue
            if "name" in item:
                item.pop("name", None)
                changed = True

        if changed:
            bind.execute(
                update_stmt,
                {
                    "md5": row.md5,
                    "schema_org": json.dumps(schema, ensure_ascii=False),
                },
            )


def downgrade() -> None:
    """Downgrade schema.

    Irreversible: removed names cannot be restored reliably.
    """
    pass

