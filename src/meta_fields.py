"""Helpers for reading normalized metadata fields from JSON-LD `meta`."""

from __future__ import annotations

import json
import re
from typing import Any


YEAR_RE = re.compile(r"(1[5-9]\d{2}|20\d{2})")
UNKNOWN_VALUES = {"", "unknown", "неизвестно", "null", "none", "n/a"}


def parse_meta(meta_raw: Any) -> dict[str, Any]:
    """Parse `document.meta` value into a dictionary."""
    if isinstance(meta_raw, dict):
        return meta_raw
    if isinstance(meta_raw, str):
        value = meta_raw.strip()
        if not value:
            return {}
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def extract_title(meta: dict[str, Any]) -> str | None:
    """Extract document title from schema.org `name`."""
    return _clean_text(meta.get("name"))


def extract_author(meta: dict[str, Any]) -> str | None:
    """Extract joined authors string from schema.org `author`."""
    return _join_unique(_extract_name_list(meta.get("author")))


def extract_publisher(meta: dict[str, Any]) -> str | None:
    """Extract publisher name from schema.org `publisher`."""
    publisher = meta.get("publisher")
    if isinstance(publisher, dict):
        return _clean_text(publisher.get("name"))
    return _clean_text(publisher)


def extract_genre(meta: dict[str, Any]) -> str | None:
    """Extract joined genre string from schema.org `genre`."""
    genres: list[str] = []
    for item in _as_list(meta.get("genre")):
        if isinstance(item, dict):
            value = _clean_text(item.get("name"))
        else:
            value = _clean_text(item)
        if value and value not in genres:
            genres.append(value)
    return _join_unique(genres)


def extract_publish_date(meta: dict[str, Any]) -> str | None:
    """Extract datePublished as-is."""
    return _clean_text(meta.get("datePublished"))


def extract_publish_year(meta: dict[str, Any]) -> int | None:
    """Extract a 4-digit year from datePublished."""
    publish_date = extract_publish_date(meta)
    if not publish_date:
        return None
    match = YEAR_RE.search(publish_date)
    return int(match.group(1)) if match else None


def extract_isbn_values(meta: dict[str, Any]) -> list[str]:
    """Extract ISBN values as a de-duplicated list."""
    isbns: list[str] = []
    for item in _as_list(meta.get("isbn")):
        if isinstance(item, dict):
            value = _clean_text(item.get("value") or item.get("name"))
        else:
            value = _clean_text(item)
        if value and value not in isbns:
            isbns.append(value)
    return isbns


def extract_isbn(meta: dict[str, Any]) -> str | None:
    """Extract joined ISBN string."""
    return _join_unique(extract_isbn_values(meta))


def extract_flat_fields(meta_raw: Any) -> dict[str, str | None]:
    """Extract CSV-compatible flattened metadata fields."""
    meta = parse_meta(meta_raw)
    return {
        "publisher": extract_publisher(meta),
        "author": extract_author(meta),
        "title": extract_title(meta),
        "isbn": extract_isbn(meta),
        "publish_date": extract_publish_date(meta),
        "genre": extract_genre(meta),
    }


def _extract_name_list(raw: Any) -> list[str]:
    names: list[str] = []
    for item in _as_list(raw):
        if isinstance(item, dict):
            name = _clean_text(item.get("name"))
        else:
            name = _clean_text(item)
        if name and name not in names:
            names.append(name)
    return names


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _join_unique(values: list[str]) -> str | None:
    unique = [v for v in values if v]
    return ", ".join(unique) if unique else None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in UNKNOWN_VALUES:
        return None
    return text or None
