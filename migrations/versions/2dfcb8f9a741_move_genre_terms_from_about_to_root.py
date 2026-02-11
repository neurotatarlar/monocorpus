"""Move Genre terms from schema_org.about back to top-level schema_org.genre

Revision ID: 2dfcb8f9a741
Revises: 7d9e9ab4f1c2
Create Date: 2026-02-11 00:00:00.000000

"""

from __future__ import annotations

from typing import Any, Sequence, Union
import json

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "2dfcb8f9a741"
down_revision: Union[str, Sequence[str], None] = "7d9e9ab4f1c2"
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


def _extract_genres(value: Any) -> list[str]:
    genres: list[str] = []
    seen: set[str] = set()
    for item in _as_list(value):
        if isinstance(item, dict):
            candidate = _clean(item.get("name"))
        else:
            candidate = _clean(item)
        if not candidate:
            continue
        key = candidate.casefold()
        if key in seen:
            continue
        seen.add(key)
        genres.append(candidate)
    return genres


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    rows = bind.execute(sa.text("SELECT md5, schema_org FROM metadata WHERE schema_org IS NOT NULL")).fetchall()
    update_stmt = sa.text("UPDATE metadata SET schema_org = CAST(:schema_org AS jsonb) WHERE md5 = :md5")

    for row in rows:
        schema = _parse_schema_org(row.schema_org)
        if not schema:
            continue

        current_genres = _extract_genres(schema.get("genre"))
        seen = {g.casefold() for g in current_genres}
        changed = False

        about_items = _as_list(schema.get("about"))
        retained_about: list[Any] = []
        for item in about_items:
            if not isinstance(item, dict):
                retained_about.append(item)
                continue
            term_set = (_clean(item.get("inDefinedTermSet")) or "").casefold()
            if term_set != "genre":
                retained_about.append(item)
                continue

            genre_value = _clean(item.get("termCode")) or _clean(item.get("name"))
            if genre_value:
                key = genre_value.casefold()
                if key not in seen:
                    seen.add(key)
                    current_genres.append(genre_value)
            changed = True

        if current_genres:
            schema["genre"] = current_genres
        elif "genre" in schema:
            schema.pop("genre", None)

        if retained_about:
            schema["about"] = retained_about
        elif "about" in schema:
            schema.pop("about", None)

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

    Irreversible data move from `about` to `genre`.
    """
    pass

