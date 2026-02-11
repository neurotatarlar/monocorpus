"""Migrate schema_org.additionalProperty into schema_org.about and drop additionalProperty

Revision ID: 0a6be8f8d183
Revises: 4f7e2d9b0a1c
Create Date: 2026-02-11 00:00:00.000000

"""

from __future__ import annotations

from typing import Any, Sequence, Union
import json

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0a6be8f8d183"
down_revision: Union[str, Sequence[str], None] = "4f7e2d9b0a1c"
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


def _normalize_schema_org(schema_org: Any) -> dict[str, Any] | None:
    if isinstance(schema_org, dict):
        schema = schema_org
    elif isinstance(schema_org, str):
        raw = schema_org.strip()
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, dict):
            return None
        schema = parsed
    else:
        return None

    updated: dict[str, Any] = dict(schema)
    terms: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for item in _as_list(updated.get("about")):
        if not isinstance(item, dict):
            continue
        term_set = _clean(item.get("inDefinedTermSet"))
        term_code = _clean(item.get("termCode")) or _clean(item.get("name"))
        if not term_set or not term_code:
            continue
        key = (term_set.casefold(), term_code.casefold())
        if key in seen:
            continue
        seen.add(key)
        terms.append(
            {
                "@type": "DefinedTerm",
                "name": term_code,
                "termCode": term_code,
                "inDefinedTermSet": term_set,
            }
        )

    for item in _as_list(updated.get("additionalProperty")):
        if not isinstance(item, dict):
            continue
        term_set = _clean(item.get("name"))
        term_code = _clean(item.get("value"))
        if not term_set or not term_code:
            continue
        key = (term_set.casefold(), term_code.casefold())
        if key in seen:
            continue
        seen.add(key)
        terms.append(
            {
                "@type": "DefinedTerm",
                "name": term_code,
                "termCode": term_code,
                "inDefinedTermSet": term_set,
            }
        )

    if terms:
        updated["about"] = terms
    else:
        updated.pop("about", None)
    updated.pop("additionalProperty", None)
    return updated


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    rows = bind.execute(sa.text("SELECT md5, schema_org FROM metadata WHERE schema_org IS NOT NULL")).fetchall()
    update_stmt = sa.text(
        "UPDATE metadata SET schema_org = CAST(:schema_org AS jsonb) WHERE md5 = :md5"
    )

    for row in rows:
        normalized = _normalize_schema_org(row.schema_org)
        if normalized is None:
            continue
        bind.execute(
            update_stmt,
            {
                "md5": row.md5,
                "schema_org": json.dumps(normalized, ensure_ascii=False),
            },
        )


def downgrade() -> None:
    """Downgrade schema.

    Irreversible: this migration removes `additionalProperty` and canonicalizes
    auxiliary metadata into `about` terms.
    """
    pass

