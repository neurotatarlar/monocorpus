"""Backfill readable captions for DDC/CategoryPath terms in schema_org.about

Revision ID: 7d9e9ab4f1c2
Revises: 0a6be8f8d183
Create Date: 2026-02-11 00:00:00.000000

"""

from __future__ import annotations

from typing import Any, Sequence, Union
import json

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7d9e9ab4f1c2"
down_revision: Union[str, Sequence[str], None] = "0a6be8f8d183"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


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


def _path_leaf(path_en: Any) -> str | None:
    if isinstance(path_en, list):
        for item in reversed(path_en):
            leaf = _clean(item)
            if leaf:
                return leaf
    return None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            """
            SELECT m.md5, m.schema_org, c.path_en
            FROM metadata AS m
            LEFT JOIN classification AS c
              ON c.id = m.classification_id
            WHERE m.schema_org IS NOT NULL
            """
        )
    ).fetchall()
    update_stmt = sa.text("UPDATE metadata SET schema_org = CAST(:schema_org AS jsonb) WHERE md5 = :md5")

    for row in rows:
        schema = _parse_schema_org(row.schema_org)
        if not schema:
            continue
        about = schema.get("about")
        if not isinstance(about, list):
            continue

        leaf = _path_leaf(row.path_en)
        changed = False

        for item in about:
            if not isinstance(item, dict):
                continue
            term_set = (_clean(item.get("inDefinedTermSet")) or "").casefold()
            if term_set not in {"ddc", "categorypath", "librarypathen"}:
                continue

            term_code = _clean(item.get("termCode")) or _clean(item.get("name"))
            if not term_code:
                continue

            current_name = _clean(item.get("name"))
            needs_caption = current_name is None or current_name.casefold() == term_code.casefold()
            if not needs_caption:
                continue

            if term_set == "ddc":
                caption = leaf
            else:
                caption = leaf
                if not caption:
                    parts = [part.strip() for part in term_code.split(">") if part.strip()]
                    caption = parts[-1] if parts else None

                # Canonicalize legacy termset name.
                if term_set == "librarypathen":
                    item["inDefinedTermSet"] = "CategoryPath"

            if caption and caption != current_name:
                item["name"] = caption
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

    Irreversible content refinement.
    """
    pass
