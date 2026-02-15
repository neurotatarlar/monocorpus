"""Tests for base metadata normalization in extraction dispatch flow."""

from __future__ import annotations

import unittest

from metadata.dispatch import _normalize_base_schema_org


class MetadataDispatchTests(unittest.TestCase):
    """Validate normalization rules applied before saving schema.org metadata."""

    def test_normalize_base_schema_org_cleans_fields(self) -> None:
        raw = {
            "@context": "https://schema.org",
            "@type": "Book",
            "name": "  unknown  ",
            "description": "  Some   text  ",
            "audience": "Неизвестно",
            "inLanguage": " tt-Cyrl, ru-Cyrl, tt-Cyrl ",
            "datePublished": "Published in 2019.",
            "numberOfPages": " 500 pages ",
            "bookEdition": "2",
            "author": [
                {"@type": "Person", "name": " Author A "},
                {"@type": "Person", "name": "unknown"},
                "Author A",
                "Author B",
            ],
            "contributor": [
                {"@type": "Person", "name": " Editor A ", "role": " editor "},
                {"@type": "Person", "name": "Editor A", "role": "editor"},
            ],
            "publisher": {"@type": "Organization", "name": "  Publisher  "},
            "isbn": ["978-5-298-02109-8", "invalid", "5-7761-1120-Х"],
            "genre": [" Novel ", "novel", "unknown"],
            "isBasedOn": {
                "@type": "CreativeWork",
                "name": " Source ",
                "url": [
                    "https://example.org/source",
                    "javascript:alert(1)",
                    "https://example.org/source",
                    "https://example.org/bad path",
                ],
            },
            "about": [
                {"@type": "DefinedTerm", "termCode": "821.512.145", "inDefinedTermSet": "UDC"},
                {"@type": "DefinedTerm", "termCode": "821.512.145", "inDefinedTermSet": "udc"},
                {"@type": "DefinedTerm", "termCode": "059", "inDefinedTermSet": "DDC"},
                {"@type": "DefinedTerm", "termCode": "Fiction", "inDefinedTermSet": "Genre"},
                {"@type": "DefinedTerm", "termCode": "X > Y", "inDefinedTermSet": "CategoryPath"},
            ],
            "additionalProperty": [{"@type": "PropertyValue", "name": "UDC", "value": "821.512.145"}],
        }

        normalized = _normalize_base_schema_org(raw)

        self.assertNotIn("name", normalized)
        self.assertEqual("Some text", normalized["description"])
        self.assertNotIn("audience", normalized)
        self.assertEqual("ru-Cyrl, tt-Cyrl", normalized["inLanguage"])
        self.assertNotIn("datePublished", normalized)
        self.assertEqual(500, normalized["numberOfPages"])
        self.assertEqual(2, normalized["bookEdition"])

        self.assertEqual(
            [{"@type": "Person", "name": "Author A"}, {"@type": "Person", "name": "Author B"}],
            normalized["author"],
        )
        self.assertEqual(
            [{"@type": "Person", "name": "Editor A", "role": "editor"}],
            normalized["contributor"],
        )
        self.assertEqual({"@type": "Organization", "name": "Publisher"}, normalized["publisher"])
        self.assertEqual(["9785298021098", "577611120X"], normalized["isbn"])
        self.assertEqual(["novel"], normalized["genre"])
        self.assertEqual(
            {
                "@type": "CreativeWork",
                "name": "Source",
                "url": ["https://example.org/source"],
            },
            normalized["isBasedOn"],
        )
        self.assertEqual(
            [{"@type": "DefinedTerm", "termCode": "821.512.145", "inDefinedTermSet": "UDC"}],
            normalized["about"],
        )
        self.assertNotIn("additionalProperty", normalized)
