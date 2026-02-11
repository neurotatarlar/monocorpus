"""Domain command group registration for the CLI."""

from __future__ import annotations

import typer

from .content import register as register_content
from .maintenance import register as register_maintenance
from .metadata import register as register_metadata
from .syncing import register as register_syncing


def register_all(app: typer.Typer, meta_app: typer.Typer) -> None:
    """Register all command groups against the provided Typer apps."""
    register_syncing(app)
    register_content(app)
    register_maintenance(app)
    register_metadata(meta_app)
