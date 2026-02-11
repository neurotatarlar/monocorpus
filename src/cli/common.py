"""Shared CLI dataclasses and validators."""

from __future__ import annotations

from dataclasses import dataclass
import string

import typer


@dataclass
class ExtractParams:
    """CLI parameters for content extraction."""

    md5: str
    path: str
    batch_size: int
    workers: int


@dataclass
class CliParams:
    """CLI parameters for path or md5 filtering."""

    md5: str
    path: str


@dataclass
class MetaCliArgs:
    """CLI parameters for library-applicability evaluation."""

    batch_size: int
    workers: int
    dry_run: bool


def md5_validator(value: str):
    """Validate and normalize an MD5 string for CLI usage."""
    if value:
        if len(value) != 32:
            raise typer.BadParameter("MD5 should be 32 characters long")
        value = value.lower()
        if not all(ch in string.hexdigits for ch in value):
            raise typer.BadParameter("MD5 should be a hex string")
    return value
