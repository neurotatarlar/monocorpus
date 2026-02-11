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

from content.pps.service import PpsStats, _backup_path, _process_doc  # noqa: E402


class PpsProcessDocTests(unittest.TestCase):
    def test_process_doc_download_failure_records_error(self) -> None:
        stats = PpsStats()
        doc = types.SimpleNamespace(md5="9" * 32, content_url="https://example.com/x.zip")

        with patch("content.pps.service._ensure_local_zip", side_effect=RuntimeError("download failed")):
            _process_doc(
                doc=doc,
                config={},
                s3client=Mock(),
                content_bucket="content",
                force_download=False,
                stats=stats,
            )

        self.assertEqual(1, stats.processed)
        self.assertEqual(1, stats.download_errors)
        self.assertEqual("download", stats.errors[0]["stage"])

    def test_process_doc_read_failure_records_error(self) -> None:
        stats = PpsStats()
        doc = types.SimpleNamespace(md5="8" * 32, content_url="https://example.com/x.zip")

        with patch("content.pps.service._ensure_local_zip", return_value=("/tmp/fake.zip", "bucket", "key.zip")), patch(
            "content.pps.service._read_markdown_from_zip", side_effect=ValueError("bad zip")
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
        self.assertEqual(1, stats.read_errors)
        self.assertEqual("read", stats.errors[0]["stage"])

    def test_process_doc_unchanged_skips_backup_write_upload(self) -> None:
        stats = PpsStats()
        doc = types.SimpleNamespace(md5="d" * 32, content_url="https://example.com/x.zip")

        with tempfile.TemporaryDirectory() as tmp:
            local_zip = os.path.join(tmp, f"{doc.md5}.zip")
            with open(local_zip, "wb") as f:
                f.write(b"zipbytes")

            with (
                patch("content.pps.service._ensure_local_zip", return_value=(local_zip, "bucket", "key.zip")),
                patch("content.pps.service._read_markdown_from_zip", return_value=("same content", f"{doc.md5}.md")),
                patch("content.pps.service._check_repeated_paragraph_blocks", return_value={}),
                patch("content.pps.service._apply_rules", return_value=("same content", {})),
                patch("content.pps.service._format_markdown") as fmt,
                patch("content.pps.service._write_zip_with_updated_md") as write_zip,
                patch("content.pps.service.upload_file") as upload,
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
                patch("content.pps.service._ensure_local_zip", return_value=(local_zip, "bucket", "key.zip")),
                patch("content.pps.service._read_markdown_from_zip", return_value=("old", f"{doc.md5}.md")),
                patch("content.pps.service._check_repeated_paragraph_blocks", return_value={}),
                patch("content.pps.service._apply_rules", return_value=("new", {"mixed_script_lookalikes_fixed": 2})),
                patch("content.pps.service._format_markdown", return_value="formatted"),
                patch("content.pps.service.truncate_underscore_runs", return_value=("formatted", 0)),
                patch("content.pps.service._write_zip_with_updated_md") as write_zip,
                patch("content.pps.service.upload_file") as upload,
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

    def test_process_doc_reports_duplicate_blocks_even_when_unchanged(self) -> None:
        stats = PpsStats()
        doc = types.SimpleNamespace(md5="0" * 32, content_url="https://example.com/x.zip")

        with tempfile.TemporaryDirectory() as tmp:
            local_zip = os.path.join(tmp, f"{doc.md5}.zip")
            with open(local_zip, "wb") as f:
                f.write(b"zipbytes")

            with (
                patch("content.pps.service._ensure_local_zip", return_value=(local_zip, "bucket", "key.zip")),
                patch("content.pps.service._read_markdown_from_zip", return_value=("same", f"{doc.md5}.md")),
                patch("content.pps.service._check_repeated_paragraph_blocks", return_value={"repeated_paragraph_blocks": 1}),
                patch("content.pps.service._apply_rules", return_value=("same", {})),
                patch("content.pps.service._format_markdown") as fmt,
                patch("content.pps.service._write_zip_with_updated_md") as write_zip,
                patch("content.pps.service.upload_file") as upload,
            ):
                _process_doc(
                    doc=doc,
                    config={},
                    s3client=Mock(),
                    content_bucket="content",
                    force_download=False,
                    stats=stats,
                )

            self.assertEqual(1, stats.unchanged)
            self.assertEqual(1, stats.issue_counts["repeated_paragraph_blocks"])
            self.assertEqual([doc.md5], stats.issue_docs["repeated_paragraph_blocks"])
            fmt.assert_not_called()
            write_zip.assert_not_called()
            upload.assert_not_called()

    def test_process_doc_backup_failure_stops_pipeline(self) -> None:
        stats = PpsStats()
        doc = types.SimpleNamespace(md5="f" * 32, content_url="https://example.com/x.zip")

        with tempfile.TemporaryDirectory() as tmp:
            local_zip = os.path.join(tmp, f"{doc.md5}.zip")
            with open(local_zip, "wb") as f:
                f.write(b"zipbytes")

            with (
                patch("content.pps.service._ensure_local_zip", return_value=(local_zip, "bucket", "key.zip")),
                patch("content.pps.service._read_markdown_from_zip", return_value=("old", f"{doc.md5}.md")),
                patch("content.pps.service._check_repeated_paragraph_blocks", return_value={}),
                patch("content.pps.service._apply_rules", return_value=("new", {"mixed_script_lookalikes_fixed": 1})),
                patch("content.pps.service._format_markdown", return_value="formatted"),
                patch("content.pps.service.truncate_underscore_runs", return_value=("formatted", 0)),
                patch("content.pps.service.shutil.copy2", side_effect=OSError("backup failed")),
                patch("content.pps.service._write_zip_with_updated_md") as write_zip,
                patch("content.pps.service.upload_file") as upload,
            ):
                _process_doc(
                    doc=doc,
                    config={},
                    s3client=Mock(),
                    content_bucket="content",
                    force_download=False,
                    stats=stats,
                )

            self.assertEqual(1, stats.write_errors)
            self.assertEqual(0, stats.changed)
            self.assertEqual("backup", stats.errors[0]["stage"])
            write_zip.assert_not_called()
            upload.assert_not_called()

    def test_process_doc_write_failure_stops_before_upload(self) -> None:
        stats = PpsStats()
        doc = types.SimpleNamespace(md5="1" * 32, content_url="https://example.com/x.zip")

        with tempfile.TemporaryDirectory() as tmp:
            local_zip = os.path.join(tmp, f"{doc.md5}.zip")
            with open(local_zip, "wb") as f:
                f.write(b"zipbytes")

            with (
                patch("content.pps.service._ensure_local_zip", return_value=(local_zip, "bucket", "key.zip")),
                patch("content.pps.service._read_markdown_from_zip", return_value=("old", f"{doc.md5}.md")),
                patch("content.pps.service._check_repeated_paragraph_blocks", return_value={}),
                patch("content.pps.service._apply_rules", return_value=("new", {"mixed_script_lookalikes_fixed": 1})),
                patch("content.pps.service._format_markdown", return_value="formatted"),
                patch("content.pps.service.truncate_underscore_runs", return_value=("formatted", 0)),
                patch("content.pps.service._write_zip_with_updated_md", side_effect=OSError("write failed")) as write_zip,
                patch("content.pps.service.upload_file") as upload,
            ):
                _process_doc(
                    doc=doc,
                    config={},
                    s3client=Mock(),
                    content_bucket="content",
                    force_download=False,
                    stats=stats,
                )

            self.assertEqual(1, stats.write_errors)
            self.assertEqual(0, stats.changed)
            self.assertEqual("write", stats.errors[0]["stage"])
            write_zip.assert_called_once()
            upload.assert_not_called()

    def test_process_doc_upload_failure_records_error(self) -> None:
        stats = PpsStats()
        doc = types.SimpleNamespace(md5="2" * 32, content_url="https://example.com/x.zip")

        with tempfile.TemporaryDirectory() as tmp:
            local_zip = os.path.join(tmp, f"{doc.md5}.zip")
            with open(local_zip, "wb") as f:
                f.write(b"zipbytes")

            with (
                patch("content.pps.service._ensure_local_zip", return_value=(local_zip, "bucket", "key.zip")),
                patch("content.pps.service._read_markdown_from_zip", return_value=("old", f"{doc.md5}.md")),
                patch("content.pps.service._check_repeated_paragraph_blocks", return_value={}),
                patch("content.pps.service._apply_rules", return_value=("new", {"mixed_script_lookalikes_fixed": 1})),
                patch("content.pps.service._format_markdown", return_value="formatted"),
                patch("content.pps.service.truncate_underscore_runs", return_value=("formatted", 0)),
                patch("content.pps.service._write_zip_with_updated_md"),
                patch("content.pps.service.upload_file", side_effect=RuntimeError("upload failed")) as upload,
            ):
                _process_doc(
                    doc=doc,
                    config={},
                    s3client=Mock(),
                    content_bucket="content",
                    force_download=False,
                    stats=stats,
                )

            self.assertEqual(1, stats.upload_errors)
            self.assertEqual(0, stats.changed)
            self.assertEqual("upload", stats.errors[0]["stage"])
            upload.assert_called_once()


if __name__ == "__main__":
    unittest.main()
