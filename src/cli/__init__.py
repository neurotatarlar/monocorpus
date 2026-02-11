"""Typer CLI entrypoint and command registration."""

from __future__ import annotations

import typer

from .commands import register_all
from .common import CliParams, ExtractParams, MetaCliArgs, md5_validator

app = typer.Typer(
    context_settings={"help_option_names": ["-h", "--help"]},
    rich_markup_mode=None,
)
meta_app = typer.Typer(
    help="Metadata commands.",
    invoke_without_command=True,
    no_args_is_help=False,
)
app.add_typer(meta_app, name="meta")

register_all(app, meta_app)

__all__ = [
    "app",
    "meta_app",
    "md5_validator",
    "ExtractParams",
    "CliParams",
    "MetaCliArgs",
]
