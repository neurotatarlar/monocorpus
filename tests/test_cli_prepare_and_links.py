"""Unit tests for lightweight CLI and helper modules."""

import os
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

from typer.testing import CliRunner

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from cli import app, md5_validator  # noqa: E402
from check_pub_links import _extension_by_mime_type, get_meta  # noqa: E402
import prepare_shots  # noqa: E402


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
        fake_dedup = types.SimpleNamespace(run=Mock())
        runner = CliRunner()
        with patch.dict(sys.modules, {"dedup": fake_dedup}):
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
        fake_dedup.run.assert_called_once_with(
            threshold=0.97,
            force_download=True,
            max_group_size=12,
            report_path="report.json",
        )

    def test_cli_pps_dispatch(self) -> None:
        fake_pps = types.SimpleNamespace(run=Mock())
        runner = CliRunner()
        with patch.dict(sys.modules, {"pps": fake_pps}):
            result = runner.invoke(
                app,
                ["pps", "--force-download", "--report", "pps_report.json"],
            )
        self.assertEqual(0, result.exit_code, result.output)
        fake_pps.run.assert_called_once_with(force_download=True, report_path="pps_report.json")

    def test_cli_extract_invalid_md5(self) -> None:
        runner = CliRunner()
        result = runner.invoke(app, ["extract", "--md5", "invalid"])
        self.assertNotEqual(0, result.exit_code)
        self.assertIn("MD5 should be 32 characters long", result.output)


if __name__ == "__main__":
    unittest.main()
