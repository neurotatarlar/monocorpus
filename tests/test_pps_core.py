"""Unit tests for pure pps helper checks."""

import os
import sys
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from pps import (  # noqa: E402
    _backup_path,
    _check_missing_footnotes,
    _check_repeated_paragraph_blocks,
    _parse_s3_location,
    _remove_duplicate_toc_markers,
    _remove_replacement_chars,
)


class PpsCoreTests(unittest.TestCase):
    def test_remove_duplicate_toc_markers(self) -> None:
        text = (
            "<!-- mdformat-toc start --no-anchors -->\n"
            "a\n"
            "<!-- mdformat-toc start --no-anchors -->\n"
            "b\n"
        )
        updated, removed = _remove_duplicate_toc_markers(text)
        self.assertEqual(1, removed)
        self.assertEqual(1, updated.count("mdformat-toc start --no-anchors"))

    def test_remove_replacement_chars(self) -> None:
        updated, removed = _remove_replacement_chars("a\ufffd\ufffdb")
        self.assertEqual("ab", updated)
        self.assertEqual(2, removed)

    def test_missing_footnotes_check(self) -> None:
        text = "text [^1] [^3]\n\n[^1]: one\n\n[^3]: three\n"
        issues = _check_missing_footnotes(text)
        self.assertIn("missing_footnotes", issues)
        self.assertGreaterEqual(issues["missing_footnotes"], 1)

    def test_repeated_paragraph_blocks_check(self) -> None:
        p1 = "A" * 60
        p2 = "B" * 60
        p3 = "C" * 60
        text = f"{p1}\n\n{p2}\n\n{p3}\n\nx\n\n{p1}\n\n{p2}\n\n{p3}\n"
        issues = _check_repeated_paragraph_blocks(text, min_paragraphs=3, min_chars=100)
        self.assertEqual({"repeated_paragraph_blocks": 1}, issues)

    def test_parse_s3_location(self) -> None:
        bucket, key = _parse_s3_location(
            "https://storage.yandexcloud.net/bucket/key/file.zip", "fb", "fk"
        )
        self.assertEqual(("bucket", "key/file.zip"), (bucket, key))
        self.assertEqual(("fb", "fk"), _parse_s3_location(None, "fb", "fk"))

    def test_backup_path(self) -> None:
        self.assertEqual("/tmp/_backupa.zip", _backup_path("/tmp/a.zip"))


if __name__ == "__main__":
    unittest.main()
