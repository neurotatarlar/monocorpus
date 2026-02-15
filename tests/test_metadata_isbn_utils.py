"""Tests for shared ISBN canonicalization helpers."""

from __future__ import annotations

import unittest

from metadata.isbn_utils import canonicalize_isbn_values


class MetadataIsbnUtilsTests(unittest.TestCase):
    """Verify isbnlib-based canonicalization used across metadata flows."""

    def test_canonicalize_hyphenated_isbns(self) -> None:
        self.assertEqual(
            ["9785298021098", "0321534964"],
            canonicalize_isbn_values(["978-5-298-02109-8", "0-321-53496-4"]),
        )

    def test_canonicalize_extracts_from_prefixed_text(self) -> None:
        self.assertEqual(
            ["9785298021098"],
            canonicalize_isbn_values("ISBN 978-5-298-02109-8"),
        )

    def test_canonicalize_returns_none_when_invalid(self) -> None:
        self.assertIsNone(canonicalize_isbn_values(["invalid", "9781234567890"]))

    def test_canonicalize_handles_cyrillic_x_in_isbn10(self) -> None:
        self.assertEqual(
            ["577611120X"],
            canonicalize_isbn_values("5-7761-1120-Х"),
        )
