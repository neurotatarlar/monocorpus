"""Focused unit tests for pps._process_doc orchestration."""

import os
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from pps import PpsStats, _backup_path, _process_doc  # noqa: E402


class PpsProcessDocTests(unittest.TestCase):
    def test_process_doc_unchanged_skips_backup_write_upload(self) -> None:
        stats = PpsStats()
        doc = types.SimpleNamespace(md5="d" * 32, content_url="https://example.com/x.zip")

        with tempfile.TemporaryDirectory() as tmp:
            local_zip = os.path.join(tmp, f"{doc.md5}.zip")
            with open(local_zip, "wb") as f:
                f.write(b"zipbytes")

            with (
                patch("pps._ensure_local_zip", return_value=(local_zip, "bucket", "key.zip")),
                patch("pps._read_markdown_from_zip", return_value=("same content", f"{doc.md5}.md")),
                patch("pps._check_repeated_paragraph_blocks", return_value={}),
                patch("pps._apply_rules", return_value=("same content", {})),
                patch("pps._format_markdown") as fmt,
                patch("pps._write_zip_with_updated_md") as write_zip,
                patch("pps.upload_file") as upload,
            ):
                _process_doc(
                    doc=doc,
                    config={},
                    s3client=Mock(),
                    content_bucket="content",
                    force_download=False,
                    stats=stats,
                )

            self.assertEqual(1, stats.processed)
            self.assertEqual(1, stats.unchanged)
            self.assertEqual(0, stats.changed)
            self.assertEqual(0, stats.backup_created)
            self.assertFalse(os.path.exists(_backup_path(local_zip)))
            fmt.assert_not_called()
            write_zip.assert_not_called()
            upload.assert_not_called()

    def test_process_doc_changed_creates_backup_and_uploads(self) -> None:
        stats = PpsStats()
        doc = types.SimpleNamespace(md5="e" * 32, content_url="https://example.com/x.zip")

        with tempfile.TemporaryDirectory() as tmp:
            local_zip = os.path.join(tmp, f"{doc.md5}.zip")
            with open(local_zip, "wb") as f:
                f.write(b"zipbytes")

            with (
                patch("pps._ensure_local_zip", return_value=(local_zip, "bucket", "key.zip")),
                patch("pps._read_markdown_from_zip", return_value=("old", f"{doc.md5}.md")),
                patch("pps._check_repeated_paragraph_blocks", return_value={}),
                patch("pps._apply_rules", return_value=("new", {"mixed_script_lookalikes_fixed": 2})),
                patch("pps._format_markdown", return_value="formatted"),
                patch("pps.truncate_underscore_runs", return_value=("formatted", 0)),
                patch("pps._write_zip_with_updated_md") as write_zip,
                patch("pps.upload_file") as upload,
            ):
                _process_doc(
                    doc=doc,
                    config={},
                    s3client=Mock(),
                    content_bucket="content",
                    force_download=False,
                    stats=stats,
                )

            self.assertEqual(1, stats.processed)
            self.assertEqual(1, stats.changed)
            self.assertEqual(0, stats.unchanged)
            self.assertEqual(1, stats.backup_created)
            self.assertTrue(os.path.exists(_backup_path(local_zip)))
            write_zip.assert_called_once()
            upload.assert_called_once()
            self.assertEqual(2, stats.issue_counts["mixed_script_lookalikes_fixed"])
            self.assertEqual(1, stats.issue_counts["mixed_script_lookalikes_fixed_docs"])
            self.assertEqual([doc.md5], stats.issue_docs["mixed_script_lookalikes_fixed"])


if __name__ == "__main__":
    unittest.main()
