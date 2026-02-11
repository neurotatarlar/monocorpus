"""CLI commands for maintenance and verification tasks."""

from __future__ import annotations

import typer


def register(app: typer.Typer) -> None:
    """Register maintenance commands."""

    @app.command("match-limited")
    def match_limited():
        """Match limited and full books and check unmatched."""
        from maintenance.match_limited import match_limited as run_match_limited

        run_match_limited()

    @app.command("sharing-restricted")
    def sharing_restricted():
        """Check docs in sharing restricted folder against database entries."""
        from maintenance.sharing_restricted import check as check_sharing_restricted

        check_sharing_restricted()

    @app.command("check-pub-links")
    def check_pub_links():
        """Check public links of documents in Yandex Disk and restore if needed."""
        from maintenance.check_pub_links import check

        check()

    @app.command("dump-state")
    def dump_state():
        """Dump current database state into Google Sheets and Drive."""
        from maintenance.dump_state import dump

        dump()
