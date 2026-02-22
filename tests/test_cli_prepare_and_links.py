"""Unit tests for lightweight CLI and helper modules."""

import os
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from typer.testing import CliRunner

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from cli import app, md5_validator  # noqa: E402
from maintenance.check_pub_links import _extension_by_mime_type, _publish_file, get_meta  # noqa: E402
from prompts import shots as prepare_shots  # noqa: E402


class CliAndHelperTests(unittest.TestCase):
    def test_md5_validator(self) -> None:
        self.assertEqual("a" * 32, md5_validator("A" * 32))
        with self.assertRaises(Exception):
            md5_validator("abc")
        with self.assertRaises(Exception):
            md5_validator("z" * 32)

    def test_extension_by_mime_type(self) -> None:
        self.assertEqual(".docx", _extension_by_mime_type("application/vnd.openxmlformats-officedocument.wordprocessingml.document"))
        self.assertEqual(".txt", _extension_by_mime_type("text/plain"))
        self.assertEqual(".html", _extension_by_mime_type("text/html"))
        self.assertEqual(".pdf", _extension_by_mime_type("application/pdf"))
        self.assertEqual(".djvu", _extension_by_mime_type("image/vnd.djvu"))
        with self.assertRaises(ValueError):
            _extension_by_mime_type("application/unknown")

    def test_get_meta_handles_empty_and_missing(self) -> None:
        client = Mock()
        self.assertIsNone(get_meta("", client))
        client.get_meta.assert_not_called()

    def test_get_meta_returns_none_on_not_found(self) -> None:
        client = Mock()
        with patch("maintenance.check_pub_links.PathNotFoundError", RuntimeError):
            client.get_meta.side_effect = RuntimeError("missing")
            self.assertIsNone(get_meta("/x", client))

    def test_publish_file_survives_unpublish_error(self) -> None:
        class _Meta(dict):
            def __init__(self):
                super().__init__(public_key="pk", public_url="pu")
                self.path = "disk:/x"
                self.resource_id = "rid"

        client = Mock()
        client.unpublish.side_effect = RuntimeError("cannot unpublish")
        client.get_meta.return_value = _Meta()

        pub_key, pub_url, path, resource_id = _publish_file(client, "disk:/x")
        self.assertEqual(("pk", "pu", "disk:/x", "rid"), (pub_key, pub_url, path, resource_id))
        client.publish.assert_called_once_with("disk:/x")

    def test_prepare_shots_helpers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            md = os.path.join(tmp, "abc2.md")
            img = os.path.join(tmp, "abc1.jpeg")
            with open(md, "w", encoding="utf-8") as f:
                f.write("ground truth")
            with open(img, "wb") as f:
                f.write(b"\xff\xd8\xff")

            listed = prepare_shots._list_files(tmp, ".md")
            self.assertEqual([md], listed)

            payload = prepare_shots._form_inline_shots(tmp)
            self.assertTrue(payload[0].startswith("Here are examples"))
            self.assertEqual("Example 1 Image:", payload[1])
            self.assertIn("inline_data", payload[2])
            self.assertIn("Ground Truth", payload[3])

    def test_cli_dedup_dispatch(self) -> None:
        runner = CliRunner()
        with patch("content.dedup.run") as run_dedup:
            result = runner.invoke(
                app,
                [
                    "dedup",
                    "--threshold",
                    "0.97",
                    "--force-download",
                    "--max-group-size",
                    "12",
                    "--report",
                    "report.json",
                ],
            )
        self.assertEqual(0, result.exit_code, result.output)
        run_dedup.assert_called_once_with(
            threshold=0.97,
            force_download=True,
            max_group_size=12,
            report_path="report.json",
        )

    def test_cli_chunk_audit_dispatch(self) -> None:
        runner = CliRunner()
        with patch("content.chunk_audit.run") as run_chunk_audit:
            result = runner.invoke(
                app,
                [
                    "chunk-audit",
                    "--md5",
                    "a" * 32,
                    "--reset-content-url",
                    "--report",
                    "chunk_audit.json",
                ],
            )
        self.assertEqual(0, result.exit_code, result.output)
        run_chunk_audit.assert_called_once_with(
            md5="a" * 32,
            md5s=None,
            path=None,
            reset_content_url=True,
            report_path="chunk_audit.json",
            size_anomaly_large_ratio=5.0,
            size_anomaly_small_ratio=0.2,
            size_anomaly_min_valid_chunks=4,
        )

    def test_cli_pps_dispatch(self) -> None:
        runner = CliRunner()
        with patch("content.pps.service.run") as run_pps:
            result = runner.invoke(
                app,
                ["pps", "--force-download", "--report", "pps_report.json"],
            )
        self.assertEqual(0, result.exit_code, result.output)
        run_pps.assert_called_once_with(force_download=True, report_path="pps_report.json")

    def test_cli_extract_invalid_md5(self) -> None:
        runner = CliRunner()
        result = runner.invoke(app, ["extract", "--md5", "invalid"])
        self.assertNotEqual(0, result.exit_code)
        self.assertIn("MD5 should be 32 characters long", result.output)

    def test_cli_extract_md5_file_dispatch(self) -> None:
        runner = CliRunner()
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            f.write("# comment\n")
            f.write("a" * 32 + "\n")
            f.write("A" * 32 + "\n")
            f.write("\n")
            f.write("b" * 32 + "\n")
            md5_file = f.name
        self.addCleanup(lambda: os.path.exists(md5_file) and os.remove(md5_file))

        with patch("content.extract_content") as extract_content:
            result = runner.invoke(app, ["extract", "--md5-file", md5_file, "--workers", "2"])

        self.assertEqual(0, result.exit_code, result.output)
        extract_content.assert_called_once()
        cli_params = extract_content.call_args.args[0]
        self.assertIsNone(cli_params.md5)
        self.assertEqual(["a" * 32, "b" * 32], cli_params.md5s)
        self.assertIsNone(cli_params.path)
        self.assertEqual(2, cli_params.workers)

    def test_cli_extract_md5_file_conflict_fails(self) -> None:
        runner = CliRunner()
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            f.write("a" * 32 + "\n")
            md5_file = f.name
        self.addCleanup(lambda: os.path.exists(md5_file) and os.remove(md5_file))

        result = runner.invoke(app, ["extract", "--md5", "a" * 32, "--md5-file", md5_file])
        self.assertNotEqual(0, result.exit_code)
        self.assertIn("Use either --md5 or --md5-file", result.output)

    def test_cli_dedup_bad_threshold_fails(self) -> None:
        runner = CliRunner()
        result = runner.invoke(app, ["dedup", "--threshold", "0"])
        self.assertNotEqual(0, result.exit_code)
        self.assertIsNotNone(result.exception)
        self.assertIn("threshold must be in (0, 1]", str(result.exception))

    def test_cli_dedup_unwritable_report_path_fails(self) -> None:
        runner = CliRunner()
        with patch("content.dedup.run", side_effect=PermissionError("report path is not writable")):
            result = runner.invoke(app, ["dedup", "--report", "/root/blocked/report.json"])
        self.assertNotEqual(0, result.exit_code)
        self.assertIsInstance(result.exception, PermissionError)
        self.assertIn("not writable", str(result.exception))

    def test_cli_dedup_bad_group_size_fails(self) -> None:
        runner = CliRunner()
        result = runner.invoke(app, ["dedup", "--max-group-size", "1"])
        self.assertNotEqual(0, result.exit_code)
        self.assertIsNotNone(result.exception)
        self.assertIn("max_group_size must be >= 2", str(result.exception))

    def test_cli_pps_download_exception_fails(self) -> None:
        runner = CliRunner()
        with patch("content.pps.service.run", side_effect=RuntimeError("download failed")):
            result = runner.invoke(app, ["pps"])
        self.assertNotEqual(0, result.exit_code)
        self.assertIsInstance(result.exception, RuntimeError)
        self.assertIn("download failed", str(result.exception))


if __name__ == "__main__":
    unittest.main()
