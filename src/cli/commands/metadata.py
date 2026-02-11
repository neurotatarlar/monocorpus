"""CLI commands for metadata extraction and evaluation."""

from __future__ import annotations

import click
import typer

from cli.common import MetaCliArgs


def register(meta_app: typer.Typer) -> None:
    """Register metadata subcommands."""

    @meta_app.callback(invoke_without_command=True)
    def meta():
        """Extract metadata by default when no subcommand is provided."""
        ctx = click.get_current_context(silent=True)
        if ctx and ctx.invoked_subcommand is None:
            import metadata

            metadata.extract_metadata()

    @meta_app.command("evaluate")
    def meta_evaluate(
        batch_size: int = typer.Option(300, help="Number of documents to process in one batch."),
        workers: int = typer.Option(5, help="Number of parallel workers to use."),
        dry_run: bool = typer.Option(False, "--dry-run", help="Run evaluation without persisting any state changes."),
    ):
        """Decide if books are applicable for library management and taxonomy."""
        from metadata.evaluation import evaluate

        args = MetaCliArgs(
            batch_size=batch_size,
            workers=workers,
            dry_run=dry_run,
        )
        evaluate(args)
