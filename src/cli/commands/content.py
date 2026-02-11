"""CLI commands for content extraction and quality passes."""

from __future__ import annotations

from typing import Optional

import typer
from typing_extensions import Annotated

from cli.common import CliParams, ExtractParams, md5_validator


def register(app: typer.Typer) -> None:
    """Register content-related commands."""

    @app.command()
    def extract(
        md5: Annotated[
            Optional[str],
            typer.Option(
                "--md5",
                callback=md5_validator,
                help="MD5 hash of the document. If not provided, all local documents will be processed.",
            ),
        ] = None,
        path: Annotated[
            Optional[str],
            typer.Option(
                "--path",
                "-p",
                help="Path to the document or directory in yandex disk. If not provided, all yandex disk will be processed",
            ),
        ] = None,
        batch_size: Annotated[
            int,
            typer.Option(
                "--batch-size",
                "-b",
                help="Count of documents to process in one batch",
            ),
        ] = None,
        workers: Annotated[
            int,
            typer.Option(
                "--workers",
                "-w",
                help="Count of parallel workers to process documents. Each worker use separate Gemini API key. Cannot be more than count of available API keys.",
            ),
        ] = 8,
    ):
        """Extract content from documents stored in Yandex Disk."""
        import content

        cli_params = ExtractParams(
            md5=md5.strip() if md5 else None,
            path=path.strip() if path else None,
            workers=workers,
            batch_size=batch_size if batch_size and batch_size > 0 else workers * 3,
        )
        content.extract_content(cli_params)

    @app.command()
    def layouts(
        md5: Annotated[
            Optional[str],
            typer.Option(
                "--md5",
                callback=md5_validator,
                help="MD5 hash of the document. If not provided, all local documents will be processed.",
            ),
        ] = None,
        path: Annotated[
            Optional[str],
            typer.Option(
                "--path",
                "-p",
                help="Path to the document or directory in yandex disk. If not provided, all yandex disk will be processed",
            ),
        ] = None,
    ):
        """Run layout detection for the selected documents."""
        from layout.dispatch import layouts as run_layouts

        cli_params = CliParams(
            md5=md5.strip() if md5 else None,
            path=path.strip() if path else None,
        )
        run_layouts(cli_params)

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
        """Postpostprocess extracted markdown archives and upload updates to S3."""
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
