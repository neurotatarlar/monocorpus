"""Prompt templates for metadata evaluation and library applicability."""

from __future__ import annotations

import json
from typing import Any


LIBRARY_APPLICABILITY_TASK_TEXT = (
    "You classify if a document should be included in a public library "
    "collection for general readers. Return strict JSON with fields: "
    "applicable(bool), reason(str|null), metadata_patch(object), "
    "library_classification(object|null)."
)

METADATA_GAP_FILL_RULES_TEXT = (
    "First, fill gaps in known metadata using the same strict rules as metadata extraction flow: "
    "only use verifiable information from provided evidence; if uncertain do not guess; "
    "do not fabricate author/date/ISBN; dehyphenate broken words mentally; "
    "keep UTF-8; when multiple values are explicitly present include all as arrays. "
    "For each requested missing field, put either extracted value or null in metadata_patch."
)

LIBRARY_APPLICABILITY_RULES_TEXT = (
    "Use applicable=false for legal/regulatory/bureaucratic and utility "
    "documents: laws, decrees, orders, resolutions, court acts, statutes, "
    "budgets, reports, procurement docs, forms, blank templates, applications, "
    "notices, instructions, accounting/tax docs, schedules, meeting minutes. "
    "Use applicable=true for reader-oriented books: fiction, poetry, drama, "
    "children's literature, biographies, history, culture, popular science, "
    "dictionaries, encyclopedias, textbooks/manuals meant for broad reading. "
    "If uncertain, prefer applicable=false. Reason must be short (2-8 words)."
)

LIBRARY_CLASSIFICATION_RULES_TEXT = (
    "library_classification must be null when applicable=false. "
    "When applicable=true, library_classification is mandatory and must include "
    "ddc (string, 3 digits with optional decimal extension, e.g. 600 or 621.3) "
    "and path (array of 2-8 category labels, top->leaf). "
    "Use one of known_classifications if there is a close match; otherwise "
    "suggest a new classification with best-fit ddc and path. "
    "If upstream_metadata is provided, treat it as trustworthy external metadata "
    "and use it together with document content."
)

MISSING_FIELD_REQUESTS = {
    "isbn": "Please add `isbn` (array of ISBN values) or return null.",
    "datePublished": "Please add `datePublished` (YYYY or YYYY-MM-DD) or return null.",
    "numberOfPages": "Please add `numberOfPages` (integer) or return null.",
    "name": "Please add `name` (document title) or return null.",
    "author": "Please add `author` (schema.org Person/Organization list) or return null.",
    "publisher": "Please add `publisher` (schema.org Organization) or return null.",
    "genre": "Please add `genre` (array) or return null.",
    "description": "Please add `description` (1-3 concise sentences) or return null.",
    "additionalProperty": "Please add `additionalProperty` (schema.org PropertyValue list) or return null.",
}


def _build_missing_fields_text(missing_fields: list[str] | None) -> str:
    items = [field for field in (missing_fields or []) if field in MISSING_FIELD_REQUESTS]
    if not items:
        return "No metadata gaps are requested in this run; keep metadata_patch empty."
    lines = ["Missing metadata fields to fill (value or null):"]
    for field in items:
        lines.append(f"- {MISSING_FIELD_REQUESTS[field]}")
    return "\n".join(lines)


def build_library_applicability_prompt(payload: dict[str, Any]) -> list[dict[str, str]]:
    """Build a structured prompt: gap-fill metadata, then evaluate, then classify."""
    missing_fields_text = _build_missing_fields_text(payload.get("missing_fields"))
    return [
        {"text": LIBRARY_APPLICABILITY_TASK_TEXT},
        {"text": METADATA_GAP_FILL_RULES_TEXT},
        {"text": missing_fields_text},
        {"text": LIBRARY_APPLICABILITY_RULES_TEXT},
        {"text": LIBRARY_CLASSIFICATION_RULES_TEXT},
        {
            "text": (
                "Now use known metadata, upstream metadata (if any), and content excerpt or PDF slice "
                "to produce the required JSON response."
            )
        },
        {"text": json.dumps(payload, ensure_ascii=False)},
    ]


__all__ = [
    "LIBRARY_APPLICABILITY_TASK_TEXT",
    "METADATA_GAP_FILL_RULES_TEXT",
    "LIBRARY_APPLICABILITY_RULES_TEXT",
    "LIBRARY_CLASSIFICATION_RULES_TEXT",
    "build_library_applicability_prompt",
]
