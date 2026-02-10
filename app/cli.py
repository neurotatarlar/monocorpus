"""Typer CLI for metadata evaluation tasks."""

import typer
from dataclasses import dataclass

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})
meta_app = typer.Typer(help="Commands for extracting metadata.")
app.add_typer(meta_app, name="meta")

@dataclass
class MetaCliArgs:
    batch_size: int
    workers: int
    dry_run: bool


@meta_app.command()
def evaluate(
    batch_size: int = typer.Option(300, help="Number of documents to process in one batch."),
    workers : int = typer.Option(5, help="Number of parallel workers to use."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Run evaluation without persisting any state changes."),
):
    """
    Decide if books is applicable for library management and create taxonomy
    """
    args = MetaCliArgs(
        batch_size=batch_size,
        workers=workers,
        dry_run=dry_run,
    )
    from meta.evaluation import evaluate
    evaluate(args)
