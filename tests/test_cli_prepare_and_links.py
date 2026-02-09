"""Unit tests for lightweight CLI and helper modules."""

import os
import sys
import tempfile
import unittest
from unittest.mock import Mock

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from cli import md5_validator  # noqa: E402
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


if __name__ == "__main__":
    unittest.main()
