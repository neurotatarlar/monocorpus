"""Tests for metadata field extraction helpers."""

from __future__ import annotations

import unittest

from metadata.fields import extract_genre


class MetadataFieldsTests(unittest.TestCase):
    """Covers extraction helpers for schema.org metadata."""

    def test_extract_genre_from_top_level_genre(self) -> None:
        meta = {"genre": ["Novel", "Historical fiction"]}
        self.assertEqual("Novel, Historical fiction", extract_genre(meta))

    def test_extract_genre_falls_back_to_about_defined_terms(self) -> None:
        meta = {
            "about": [
                {"@type": "DefinedTerm", "name": "Preserved", "inDefinedTermSet": "Other"},
                {
                    "@type": "DefinedTerm",
                    "name": "Novel",
                    "termCode": "Novel",
                    "inDefinedTermSet": "Genre",
                },
                {
                    "@type": "DefinedTerm",
                    "name": "Novel",
                    "termCode": "Novel",
                    "inDefinedTermSet": "Genre",
                },
                {
                    "@type": "DefinedTerm",
                    "name": "Historical fiction",
                    "termCode": "Historical fiction",
                    "inDefinedTermSet": "Genre",
                },
            ]
        }
        self.assertEqual("Novel, Historical fiction", extract_genre(meta))
