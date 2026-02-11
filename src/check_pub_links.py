"""Backward-compatible shim for maintenance.check_pub_links."""

from maintenance.check_pub_links import (
    _extension_by_mime_type,
    _publish_file,
    check,
)
from yadisk.exceptions import PathNotFoundError


def get_meta(path, ya_client):
    """Fetch Yandex Disk metadata for a path, returning None when missing."""
    try:
        if not path:
            return None
        return ya_client.get_meta(path, fields=["md5"])
    except PathNotFoundError:
        return None

__all__ = [
    "check",
    "get_meta",
    "_extension_by_mime_type",
    "_publish_file",
    "PathNotFoundError",
]
