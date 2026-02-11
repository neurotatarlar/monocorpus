"""Small helper utilities for PDF extraction diagnostics."""

from __future__ import annotations

import re


FIGURE_TAG_PATTERN = re.compile(r"<figure\b[^>]*>", re.IGNORECASE)


def has_figure_tag_with_missing_attributes(content):
    """Check if any <figure> tag lacks `data-bbox` and `data-page` attrs."""
    for match in FIGURE_TAG_PATTERN.finditer(content):
        tag = match.group(0)
        has_bbox = 'data-bbox=' in tag
        has_page = 'data-page=' in tag
        if not (has_bbox and has_page):
            return True
    return False


def tokens_info(usage_meta):
    """Format token usage metadata for logs."""
    if usage_meta:
        return (
            f"input tokens:{usage_meta.prompt_token_count}, "
            f"output tokens: {usage_meta.candidates_token_count}, "
            f"total tokens: {usage_meta.total_token_count}"
        )
    return ""
