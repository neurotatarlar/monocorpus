"""Unit tests for pps text normalization helpers."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from content.pps.text import (  # noqa: E402
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

    def test_truncate_underscore_runs_handles_unicode_and_double_escape(self) -> None:
        text = "＿＿＿＿＿＿＿＿＿＿＿＿\n\\\\\\\\____________\n"
        fixed, removed = truncate_underscore_runs(text)
        self.assertEqual("__________\n\\__________\n", fixed)
        self.assertGreaterEqual(removed, 4)

    def test_mask_restore_math_segments_roundtrip(self) -> None:
        text = "Текст $a\\_b + c$ и $$E = mc^2$$.\n"
        masked, placeholders = mask_math_segments(text)
        self.assertIn("MATHPLACEHOLDER", masked)
        self.assertNotIn("$a\\_b + c$", masked)
        self.assertNotIn("$$E = mc^2$$", masked)

        restored = restore_math_segments(masked, placeholders)
        self.assertEqual(text, restored)

    def test_mask_math_ignores_escaped_dollar(self) -> None:
        text = r"Цена \$5, формула $x+y$."
        masked, placeholders = mask_math_segments(text)
        self.assertIn(r"\$5", masked)
        self.assertIn("MATHPLACEHOLDER", masked)
        restored = restore_math_segments(masked, placeholders)
        self.assertEqual(text, restored)

    def test_mask_math_handles_multiline_display(self) -> None:
        text = "До:\n$$\na = b + c\n$$\nПосле.\n"
        masked, placeholders = mask_math_segments(text)
        self.assertEqual(1, len(placeholders))
        restored = restore_math_segments(masked, placeholders)
        self.assertEqual(text, restored)

    def test_mask_math_handles_adjacent_segments(self) -> None:
        text = "$a$$b$ и $$c$$$$d$$"
        masked, placeholders = mask_math_segments(text)
        self.assertGreaterEqual(len(placeholders), 2)
        restored = restore_math_segments(masked, placeholders)
        self.assertEqual(text, restored)

    def test_mask_math_unclosed_inline_kept(self) -> None:
        text = "Неполная формула $x + y без закрытия"
        masked, placeholders = mask_math_segments(text)
        self.assertEqual(text, masked)
        self.assertEqual({}, placeholders)


if __name__ == "__main__":
    unittest.main()
