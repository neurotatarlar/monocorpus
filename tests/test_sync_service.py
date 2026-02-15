"""Tests for sync service helper logic."""

import os
import sys
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from sync.service import _is_isbn_group_marked_keep_many  # noqa: E402


class SyncServiceTests(unittest.TestCase):
    def test_isbn_group_marked_keep_many_when_subset(self) -> None:
        self.assertTrue(
            _is_isbn_group_marked_keep_many(
                "9781000000001",
                {"a", "b"},
                {"9781000000001": {"a", "b", "c"}},
            )
        )

    def test_isbn_group_not_marked_keep_many_when_missing_or_not_subset(self) -> None:
        self.assertFalse(
            _is_isbn_group_marked_keep_many(
                "9781000000001",
                {"a", "x"},
                {"9781000000001": {"a", "b", "c"}},
            )
        )
        self.assertFalse(
            _is_isbn_group_marked_keep_many(
                "9781000000001",
                {"a"},
                {},
            )
        )

