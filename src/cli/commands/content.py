"""CLI commands for content extraction and quality passes."""

from __future__ import annotations

from typing import Optional

import typer
from typing_extensions import Annotated

from cli.common import CliParams, ExtractParams, load_md5s_from_file, md5_validator


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
        md5_file: Annotated[
            Optional[str],
            typer.Option(
                "--md5-file",
                help="Path to a text file with one MD5 per line.",
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

        if md5 and md5_file:
            raise typer.BadParameter("Use either --md5 or --md5-file, not both.")
        if path and md5_file:
            raise typer.BadParameter("Use either --path or --md5-file, not both.")

        md5s = None
        if md5_file:
            md5s = load_md5s_from_file(md5_file)
            if not md5s:
                raise typer.BadParameter(f"No MD5 values found in file: {md5_file}")

        cli_params = ExtractParams(
            md5=md5.strip() if md5 else None,
            md5s=md5s,
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
        from experimental.layout.dispatch import layouts as run_layouts

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
        from content.pps.service import run as run_pps

        run_pps(force_download=force_download, report_path=report_path)

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
        from content.dedup import run as run_dedup

        run_dedup(
            threshold=threshold,
            force_download=force_download,
            max_group_size=max_group_size,
            report_path=report_path,
        )

    @app.command("chunk-audit")
    def chunk_audit(
        md5: Annotated[
            Optional[str],
            typer.Option(
                "--md5",
                callback=md5_validator,
                help="Audit only this document MD5.",
            ),
        ] = None,
        md5_file: Annotated[
            Optional[str],
            typer.Option(
                "--md5-file",
                help="Path to a text file with one MD5 per line.",
            ),
        ] = None,
        path: Annotated[
            Optional[str],
            typer.Option(
                "--path",
                "-p",
                help="Path to the document or directory in Yandex Disk.",
            ),
        ] = None,
        reset_content_url: Annotated[
            bool,
            typer.Option(
                "--reset-content-url",
                help="Set document.content_url=NULL for docs with incomplete/invalid local chunk sets.",
            ),
        ] = False,
        report_path: Annotated[
            Optional[str],
            typer.Option(
                "--report",
                help="Optional path to write the JSON audit report. Defaults to workdir logs.",
            ),
        ] = None,
        size_anomaly_large_ratio: Annotated[
            float,
            typer.Option(
                "--size-anomaly-large-ratio",
                help="Report chunk as large anomaly if bytes/page >= this ratio to document average.",
            ),
        ] = 5.0,
        size_anomaly_small_ratio: Annotated[
            float,
            typer.Option(
                "--size-anomaly-small-ratio",
                help="Report chunk as small anomaly if bytes/page <= this ratio to document average.",
            ),
        ] = 0.2,
        size_anomaly_min_valid_chunks: Annotated[
            int,
            typer.Option(
                "--size-anomaly-min-valid-chunks",
                help="Minimum valid chunks in a document before size anomalies are evaluated.",
            ),
        ] = 4,
    ):
        """Audit local PDF chunk coverage and optionally reset docs for re-extraction."""
        from content.chunk_audit import run as run_chunk_audit

        if md5 and md5_file:
            raise typer.BadParameter("Use either --md5 or --md5-file, not both.")
        if path and md5_file:
            raise typer.BadParameter("Use either --path or --md5-file, not both.")
        if size_anomaly_large_ratio <= 1.0:
            raise typer.BadParameter("--size-anomaly-large-ratio must be > 1.0")
        if not (0.0 < size_anomaly_small_ratio < 1.0):
            raise typer.BadParameter("--size-anomaly-small-ratio must be in (0, 1)")
        if size_anomaly_min_valid_chunks < 1:
            raise typer.BadParameter("--size-anomaly-min-valid-chunks must be >= 1")

        md5s = None
        if md5_file:
            md5s = load_md5s_from_file(md5_file)
            if not md5s:
                raise typer.BadParameter(f"No MD5 values found in file: {md5_file}")

        run_chunk_audit(
            md5=md5.strip() if md5 else None,
            md5s=md5s,
            path=path.strip() if path else None,
            reset_content_url=reset_content_url,
            report_path=report_path,
            size_anomaly_large_ratio=size_anomaly_large_ratio,
            size_anomaly_small_ratio=size_anomaly_small_ratio,
            size_anomaly_min_valid_chunks=size_anomaly_min_valid_chunks,
        )
