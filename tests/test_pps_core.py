"""Unit tests for pure pps helper checks."""

import os
import sys
import json
import tempfile
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from pps import (  # noqa: E402
    PpsStats,
    _apply_rules,
    _backup_path,
    _check_missing_footnotes,
    _check_repeated_paragraph_blocks,
    _parse_s3_location,
    _remove_duplicate_toc_markers,
    _remove_replacement_chars,
    _write_report,
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

    def test_apply_rules_is_idempotent_for_current_enabled_fixes(self) -> None:
        original = "Тест сeлам\n"
        once, issues_once = _apply_rules(original)
        twice, issues_twice = _apply_rules(once)
        self.assertEqual("Тест селам\n", once)
        self.assertIn("mixed_script_lookalikes_fixed", issues_once)
        self.assertEqual(once, twice)
        self.assertEqual({}, issues_twice)

    def test_write_report_schema_stable_for_empty_and_non_empty_stats(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            empty_path = os.path.join(tmp, "empty.json")
            _write_report(empty_path, PpsStats())
            with open(empty_path, "r", encoding="utf-8") as f:
                empty_report = json.load(f)

            self.assertIn("summary", empty_report)
            self.assertIn("issues", empty_report)
            self.assertIn("failed", empty_report)
            self.assertIn("issue_docs", empty_report)
            self.assertEqual({}, empty_report["issues"])
            self.assertEqual({}, empty_report["failed"])
            self.assertEqual({}, empty_report["issue_docs"])

            filled = PpsStats(processed=2, changed=1, unchanged=1, download_errors=1)
            filled.add_issue("mixed_script_lookalikes_fixed", 3)
            filled.add_issue_doc("mixed_script_lookalikes_fixed", "a" * 32)
            filled.add_error("a" * 32, "download", "x")
            filled_path = os.path.join(tmp, "filled.json")
            _write_report(filled_path, filled)
            with open(filled_path, "r", encoding="utf-8") as f:
                filled_report = json.load(f)

            self.assertIn("summary", filled_report)
            self.assertIn("issues", filled_report)
            self.assertIn("failed", filled_report)
            self.assertIn("issue_docs", filled_report)
            self.assertEqual(1, filled_report["summary"]["download_errors"])
            self.assertEqual(3, filled_report["issues"]["mixed_script_lookalikes_fixed"])
            self.assertEqual(["a" * 32], filled_report["failed"]["download"])
            self.assertEqual(["a" * 32], filled_report["issue_docs"]["mixed_script_lookalikes_fixed"])


if __name__ == "__main__":
    unittest.main()
