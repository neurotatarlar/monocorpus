"""Prompt templates for metadata evaluation and library applicability."""

from __future__ import annotations

import json
from typing import Any


LIBRARY_APPLICABILITY_PROMPT_TEXT = (
    "You classify if a document should be included in a public library "
    "collection for general readers. Return strict JSON with fields: "
    "applicable(bool), reason(str|null), metadata_patch(object|null), "
    "library_classification(object|null). "
    "Use applicable=false for legal/regulatory/bureaucratic and utility "
    "documents: laws, decrees, orders, resolutions, court acts, statutes, "
    "budgets, reports, procurement docs, forms, blank templates, applications, "
    "notices, instructions, accounting/tax docs, schedules, meeting minutes. "
    "Use applicable=true for reader-oriented books: fiction, poetry, drama, "
    "children's literature, biographies, history, culture, popular science, "
    "dictionaries, encyclopedias, textbooks/manuals meant for broad reading. "
    "If uncertain, prefer applicable=false. Reason must be short (2-8 words). "
    "metadata_patch must include ONLY missing fields listed in missing_fields "
    "and ONLY if strongly supported by provided content excerpt or metadata. "
    "Allowed patch keys: isbn, datePublished, numberOfPages, name, author, "
    "publisher, genre, description, additionalProperty. "
    "library_classification must be null when applicable=false. "
    "When applicable=true, library_classification is mandatory and must include "
    "ddc (string, 3 digits with optional decimal extension, e.g. 600 or 621.3) "
    "and path (array of 2-8 category labels, top->leaf). "
    "Use one of known_classifications if there is a close match; otherwise "
    "suggest a new classification with best-fit ddc and path. "
    "If upstream_metadata is provided, treat it as trustworthy external metadata "
    "and use it together with document content."
)


def build_library_applicability_prompt(payload: dict[str, Any]) -> list[dict[str, str]]:
    """Build Gemini prompt for strict library-applicability classification."""
    return [
        {"text": LIBRARY_APPLICABILITY_PROMPT_TEXT},
        {"text": json.dumps(payload, ensure_ascii=False)},
    ]


__all__ = ["LIBRARY_APPLICABILITY_PROMPT_TEXT", "build_library_applicability_prompt"]
