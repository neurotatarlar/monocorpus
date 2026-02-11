"""Rule helpers for sync filtering."""

from __future__ import annotations

from sync_constants import NOT_DOCUMENT_TYPES


def normalize_isbn(value):
    """Normalize ISBN into compact 10/13-char form, otherwise return None."""
    cleaned = "".join(ch for ch in value.strip().upper() if ch.isdigit() or ch == "X")
    return cleaned if len(cleaned) in (10, 13) else None


def should_be_skipped(file):
    """Determine whether a file should be skipped based on MIME/path rules."""
    if file.mime_type in NOT_DOCUMENT_TYPES:
        # sometimes valid PDF docs detected as octet-stream
        if file.mime_type == 'application/octet-stream' and file.path.endswith(".pdf"):
            return False, 'application/pdf'
        elif file.mime_type == 'text/html' and file.path.endswith(".txt"):
            return False, 'text/plain'
        elif file.mime_type == 'text/html' and file.path.endswith(".doc"):
            return False, 'text/plain'
        else:
            return True, file.mime_type
    return False, file.mime_type
