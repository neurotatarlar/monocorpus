"""Unit tests for pps text normalization helpers."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pps_text import (  # noqa: E402
    mask_math_segments,
    normalize_mixed_script_lookalikes,
    restore_math_segments,
    truncate_underscore_runs,
)


class PpsTextTests(unittest.TestCase):
    def test_mixed_script_fixes_only_cyrillic_tokens(self) -> None:
        text = "Бүген сeлам һәм Əлифба\nzamanalif əlifba\n"
        fixed, count = normalize_mixed_script_lookalikes(text)
        self.assertEqual("Бүген селам һәм Әлифба\nzamanalif əlifba\n", fixed)
        self.assertEqual(2, count)

    def test_mixed_script_skips_inline_and_fenced_code(self) -> None:
        text = (
            "текст сeлам\n"
            "`код сeлам Əлифба`\n"
            "```\n"
            "сeлам Əлифба\n"
            "```\n"
        )
        fixed, count = normalize_mixed_script_lookalikes(text)
        expected = (
            "текст селам\n"
            "`код сeлам Əлифба`\n"
            "```\n"
            "сeлам Əлифба\n"
            "```\n"
        )
        self.assertEqual(expected, fixed)
        self.assertEqual(1, count)

    def test_truncate_underscore_runs_and_idempotent(self) -> None:
        text = "___________\nпаспорт _________ №___________\n"
        fixed, removed = truncate_underscore_runs(text)
        self.assertEqual("__________\nпаспорт _________ №\\__________\n", fixed)
        self.assertEqual(2, removed)

        fixed2, removed2 = truncate_underscore_runs(fixed)
        self.assertEqual(fixed, fixed2)
        self.assertEqual(0, removed2)

    def test_mask_restore_math_segments_roundtrip(self) -> None:
        text = "Текст $a\\_b + c$ и $$E = mc^2$$.\n"
        masked, placeholders = mask_math_segments(text)
        self.assertIn("MATHPLACEHOLDER", masked)
        self.assertNotIn("$a\\_b + c$", masked)
        self.assertNotIn("$$E = mc^2$$", masked)

        restored = restore_math_segments(masked, placeholders)
        self.assertEqual(text, restored)


if __name__ == "__main__":
    unittest.main()
