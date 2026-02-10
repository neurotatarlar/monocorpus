"""Typer CLI commands for running the monocorpus pipeline."""

import typer
import click
from typing_extensions import Annotated
from typing import Optional
from dataclasses import dataclass
import string

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


@app.command()
def sync():
    """
    Synchronize documents between Yandex Disk and Google Sheets.

    This command traverses files and directories in Yandex Disk, identifies new or updated entries, 
    and uploads them to Google Sheets. It ensures that the local and remote data are in sync, 
    facilitating seamless integration and data management.
    """
    import sync
    sync.sync()


@app.command()
def hf():
    """
    Assemble structured dataset from content files stored in S3.
    """
    import hf 
    hf.assemble_dataset()
    
    
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
    """
    Decide if books are applicable for library management and taxonomy.
    """
    from meta.evaluation import evaluate

    args = MetaCliArgs(
        batch_size=batch_size,
        workers=workers,
        dry_run=dry_run,
    )
    evaluate(args)
    
    
@app.command()
def extract(
    md5: Annotated[
        Optional[str],
        typer.Option(
            "--md5",
            callback=md5_validator,
            help="MD5 hash of the document. If not provided, all local documents will be processed."
        )
    ] = None,
    path: Annotated[
        Optional[str],
        typer.Option(
            "--path", "-p",
            help="Path to the document or directory in yandex disk. If not provided, all yandex disk will be processed"
        )
    ] = None,
    batch_size: Annotated[
        int,
        typer.Option(
            "--batch-size", "-b",
            help="Count of documents to process in one batch",
        )
    ] = None,
    workers: Annotated[
        int,
        typer.Option(
            "--workers", "-w",
            help="Count of parallel workers to process documents. Each worker use separate Gemini API key. Cannot be more than count of available API keys.",
        )
    ] = 8):
    """
    Extract content from documents stored in Yandex Disk.
    """
    import content
    cli_params = ExtractParams(
        md5=md5.strip() if md5 else None, 
        path=path.strip() if path else None,
        workers=workers,
        batch_size=batch_size if batch_size and batch_size > 0 else workers*3,
    )
    content.extract_content(cli_params)
    
    
@app.command()
def layouts(
        md5: Annotated[
        Optional[str],
        typer.Option(
            "--md5",
            callback=md5_validator,
            help="MD5 hash of the document. If not provided, all local documents will be processed."
        )
    ] = None,
    path: Annotated[
        Optional[str],
        typer.Option(
            "--path", "-p",
            help="Path to the document or directory in yandex disk. If not provided, all yandex disk will be processed"
        )
    ] = None,
):
    """Run layout detection for the selected documents."""
    from layout.dispatch import layouts
    cli_params = CliParams(
        md5=md5.strip() if md5 else None, 
        path=path.strip() if path else None,
    )
    layouts(cli_params)
    
    
@app.command()
def match_limited():
    """
    Match limited and full books and check unmatched 
    """
    import match_limited
    match_limited.match_limited()
    
    
@app.command()
def sharing_restricted():
    """
    Check docs in sharing restricted folder are matches to docs in gsheets
    """
    import sharing_restricted
    sharing_restricted.check()
    
    
@app.command()
def check_artifacts():
    """Run artifact validation checks."""
    import check_artifacts
    check_artifacts.check()
    
    
@app.command()
def check_pub_links():
    """
    Check public links of documents in Yandex Disk and restore if needed
    """
    import check_pub_links
    check_pub_links.check()
    

@app.command()
def dump_state():
    """
    Dump current database state into google sheets and google drive
    """
    import dump_state
    dump_state.dump()


@app.command()
def pps(
    force_download: Annotated[
        bool,
        typer.Option(
            "--force-download",
            help="Download archives even if a local copy exists.",
        ),
    ] = False,
    report_path: Annotated[
        Optional[str],
        typer.Option(
            "--report",
            help="Optional path to write the JSON report. Defaults to workdir logs.",
        ),
    ] = None,
):
    """
    Postpostprocess extracted markdown archives and upload updates to S3.
    """
    import pps
    pps.run(force_download=force_download, report_path=report_path)


@app.command()
def dedup(
    threshold: Annotated[
        float,
        typer.Option(
            "--threshold",
            help="Near-full duplicate threshold based on paragraph containment.",
        ),
    ] = 0.98,
    force_download: Annotated[
        bool,
        typer.Option(
            "--force-download",
            help="Download archives even if a local copy exists.",
        ),
    ] = False,
    max_group_size: Annotated[
        int,
        typer.Option(
            "--max-group-size",
            help="Skip candidate groups larger than this size.",
        ),
    ] = 80,
    report_path: Annotated[
        Optional[str],
        typer.Option(
            "--report",
            help="Optional path to write the JSON report. Defaults to workdir logs.",
        ),
    ] = None,
):
    """Scan for near-full duplicate documents and write a report."""
    import dedup

    dedup.run(
        threshold=threshold,
        force_download=force_download,
        max_group_size=max_group_size,
        report_path=report_path,
    )

