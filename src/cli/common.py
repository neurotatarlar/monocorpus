"""Shared CLI dataclasses and validators."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import string
from typing import Optional

import typer


@dataclass
class ExtractParams:
    """CLI parameters for content extraction."""

    md5: Optional[str]
    md5s: Optional[list[str]]
    path: Optional[str]
    batch_size: int
    workers: int


@dataclass
class CliParams:
    """CLI parameters for path or md5 filtering."""

    md5: Optional[str]
    path: Optional[str]


@dataclass
class MetaCliArgs:
    """CLI parameters for library-applicability evaluation."""

    batch_size: int
    workers: int
    dry_run: bool
    excerpt_chars: int


def md5_validator(value: str):
    """Validate and normalize an MD5 string for CLI usage."""
    if value:
        if len(value) != 32:
            raise typer.BadParameter("MD5 should be 32 characters long")
        value = value.lower()
        if not all(ch in string.hexdigits for ch in value):
            raise typer.BadParameter("MD5 should be a hex string")
    return value


def load_md5s_from_file(file_path: str) -> list[str]:
    """Load unique valid MD5 values from a text file (one hash per line)."""
    path = Path(file_path).expanduser()
    if not path.exists():
        raise typer.BadParameter(f"MD5 file not found: {path}")
    if not path.is_file():
        raise typer.BadParameter(f"MD5 file path is not a file: {path}")

    md5s: list[str] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for line_no, raw_line in enumerate(f, 1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                md5 = md5_validator(line)
            except typer.BadParameter as exc:
                raise typer.BadParameter(f"Invalid MD5 at line {line_no}: {line}") from exc
            if md5 not in seen:
                seen.add(md5)
                md5s.append(md5)
    return md5s
