"""Tests for local PDF chunk audit helpers."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from content.chunk_audit import (  # noqa: E402
    _detect_size_anomalies,
    _inspect_local_chunks,
    _load_valid_chunk_ranges,
    _missing_pages_from_ranges,
)


class ContentChunkAuditTests(unittest.TestCase):
    """Validate chunk audit completeness and local chunk validation logic."""

    def _write_chunk(self, root: str, name: str, content: object) -> None:
        with open(os.path.join(root, name), "w", encoding="utf-8") as f:
            json.dump({"content": content}, f, ensure_ascii=False)

    def test_load_valid_chunk_ranges_accepts_empty_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self._write_chunk(tmp, "chunk-0-0.json", "")
            self._write_chunk(tmp, "chunk-1-2.json", "text")

            ranges, invalid = _load_valid_chunk_ranges(tmp)

            self.assertEqual([(0, 0), (1, 2)], ranges)
            self.assertEqual([], invalid)

    def test_load_valid_chunk_ranges_reports_invalid_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self._write_chunk(tmp, "chunk-0-0.json", "ok")
            self._write_chunk(tmp, "chunk-1-1.json", 123)  # invalid type
            self._write_chunk(tmp, "chunk-2-2.json", '<figure data-page="1"></figure>')  # missing data-bbox

            ranges, invalid = _load_valid_chunk_ranges(tmp)

            self.assertEqual([(0, 0)], ranges)
            self.assertEqual(["chunk-1-1.json", "chunk-2-2.json"], invalid)

    def test_missing_pages_from_ranges_clamps_bounds_and_detects_gaps(self) -> None:
        missing = _missing_pages_from_ranges(
            pages_count=5,
            ranges=[(-10, 1), (3, 10)],  # covers 0,1 and 3,4
        )
        self.assertEqual([2], missing)

    def test_detect_size_anomalies_flags_large_chunk_per_document(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self._write_chunk(tmp, "chunk-0-0.json", "a" * 100)
            self._write_chunk(tmp, "chunk-1-1.json", "b" * 100)
            self._write_chunk(tmp, "chunk-2-2.json", "c" * 100)
            self._write_chunk(tmp, "chunk-3-3.json", "d" * 100)
            self._write_chunk(tmp, "chunk-4-4.json", "e" * 100)
            self._write_chunk(tmp, "chunk-5-5.json", "f" * 10000)

            valid_chunks, invalid = _inspect_local_chunks(tmp)
            anomalies = _detect_size_anomalies(valid_chunks)

            self.assertEqual([], invalid)
            self.assertTrue(
                any(
                    a["chunk"] == "chunk-5-5.json" and a["reason"] == "too_large_bytes_per_page"
                    for a in anomalies
                )
            )

    def test_detect_size_anomalies_flags_small_non_empty_chunk(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self._write_chunk(tmp, "chunk-0-0.json", "a" * 2000)
            self._write_chunk(tmp, "chunk-1-1.json", "b" * 2000)
            self._write_chunk(tmp, "chunk-2-2.json", "c" * 2000)
            self._write_chunk(tmp, "chunk-3-3.json", "x")

            valid_chunks, invalid = _inspect_local_chunks(tmp)
            anomalies = _detect_size_anomalies(valid_chunks)

            self.assertEqual([], invalid)
            self.assertEqual(1, len(anomalies))
            self.assertEqual("chunk-3-3.json", anomalies[0]["chunk"])
            self.assertEqual("too_small_bytes_per_page", anomalies[0]["reason"])

    def test_detect_size_anomalies_ignores_empty_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self._write_chunk(tmp, "chunk-0-0.json", "a" * 2000)
            self._write_chunk(tmp, "chunk-1-1.json", "b" * 2000)
            self._write_chunk(tmp, "chunk-2-2.json", "c" * 2000)
            self._write_chunk(tmp, "chunk-3-3.json", "")

            valid_chunks, invalid = _inspect_local_chunks(tmp)
            anomalies = _detect_size_anomalies(valid_chunks)

            self.assertEqual([], invalid)
            self.assertEqual([], anomalies)


if __name__ == "__main__":
    unittest.main()
