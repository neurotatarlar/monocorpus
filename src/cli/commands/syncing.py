"""CLI commands for synchronization and dataset assembly flows."""

from __future__ import annotations

import typer


def register(app: typer.Typer) -> None:
    """Register sync-related top-level commands."""

    @app.command()
    def sync():
        """Synchronize documents between Yandex Disk and Google Sheets."""
        import sync

        sync.sync()

    @app.command()
    def hf():
        """Assemble structured dataset from content files stored in S3."""
        from dataset.hf import assemble_dataset

        assemble_dataset()
