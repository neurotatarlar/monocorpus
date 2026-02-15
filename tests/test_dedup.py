"""Unit tests for report-only dedup helper logic."""

import json
import os
import sys
import tempfile
import unittest
import zipfile
from unittest.mock import Mock, patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from content.dedup import (  # noqa: E402
    DedupStats,
    DocMeta,
    Fingerprint,
    _build_duplicate_groups,
    _build_fingerprint,
    _candidate_keys,
    _compare_fingerprints,
    _detect_format,
    _ensure_local_zip,
    _extract_year,
    _is_duplicate,
    _normalize_isbn,
    _normalize_key,
    _normalize_text,
    _parse_s3_location,
    _pick_keeper,
    _read_markdown_from_zip,
    _write_report,
)


class DedupTests(unittest.TestCase):
    def test_normalization_helpers(self) -> None:
        self.assertEqual("hello world", _normalize_key("  Hello,   World! "))
        self.assertEqual("9781402894626", _normalize_isbn("978-1-4028-9462-6"))
        self.assertEqual("", _normalize_isbn("978-1-234-56789-0"))
        self.assertEqual("2012", _extract_year("published in 2012, reprint"))
        self.assertEqual("", _extract_year("unknown"))

    def test_candidate_keys(self) -> None:
        meta = DocMeta(
            md5="a" * 32,
            content_url="u",
            mime_type="application/epub+zip",
            ya_path="/x/book.epub",
            title="Long title of a great book",
            author="Some Author",
            isbn="978-1-4028-9462-6",
            publish_year="2001",
        )
        keys = _candidate_keys(meta)
        self.assertIn("isbn:9781402894626", keys)
        self.assertIn("title_author:long title of a great book|some author", keys)
        self.assertIn("title_year:long title of a great book|2001", keys)
        self.assertIn("title:long title of a great book", keys)

    def test_text_normalization_and_fingerprint(self) -> None:
        md = "# Header\nText [^1] `code`.\n\n```x\nskip\n```\n\nPara two.\n"
        normalized = _normalize_text(md)
        self.assertNotIn("#", normalized)
        self.assertNotIn("code", normalized)
        self.assertNotIn("[^1]", normalized)
        self.assertIn("text", normalized)

        fp = _build_fingerprint(md)
        self.assertTrue(fp.text_hash)
        self.assertGreaterEqual(fp.char_count, 1)
        self.assertGreaterEqual(len(fp.paragraph_hashes), 1)

    def test_compare_and_duplicate_decision(self) -> None:
        a = Fingerprint("h1", 1000, {"a", "b", "c"})
        b = Fingerprint("h1", 1000, {"a", "b", "c"})
        score = _compare_fingerprints(a, b)
        self.assertTrue(score.exact_hash)
        self.assertTrue(_is_duplicate(score, 0.98))

        c = Fingerprint("h2", 1000, {"a", "b", "c"})
        d = Fingerprint("h3", 950, {"a", "b", "c"})
        score2 = _compare_fingerprints(c, d)
        self.assertFalse(score2.exact_hash)
        self.assertAlmostEqual(1.0, score2.containment)
        self.assertTrue(_is_duplicate(score2, 0.98))

    def test_duplicate_decision_respects_length_ratio_gate(self) -> None:
        a = Fingerprint("h1", 1000, {"a", "b", "c"})
        b = Fingerprint("h2", 500, {"a", "b", "c"})
        score = _compare_fingerprints(a, b)
        self.assertAlmostEqual(1.0, score.containment)
        self.assertLess(score.length_ratio, 0.9)
        self.assertFalse(_is_duplicate(score, 0.98))

    def test_detect_format_and_keeper(self) -> None:
        docs = {
            "a": DocMeta("a", "u", "application/pdf", "/x/a.pdf", None, None, None, None),
            "b": DocMeta("b", "u", "application/epub+zip", "/x/b.epub", None, None, None, None),
            "c": DocMeta("c", "u", "application/pdf", "/x/c.pdf", None, None, None, None),
        }
        fps = {
            "a": Fingerprint("1", 100, {"x"}),
            "b": Fingerprint("2", 50, {"x"}),
            "c": Fingerprint("3", 200, {"x"}),
        }
        self.assertEqual("pdf", _detect_format(docs["a"]))
        self.assertEqual("epub", _detect_format(docs["b"]))
        keeper = _pick_keeper(["a", "b", "c"], docs, fps)
        self.assertEqual("b", keeper)

    def test_pick_keeper_is_deterministic_on_tie(self) -> None:
        docs = {
            "a1": DocMeta("a1", "u", "application/pdf", "/x/a.pdf", "A", "X", None, "2000"),
            "a2": DocMeta("a2", "u", "application/pdf", "/x/b.pdf", "B", "Y", None, "2001"),
        }
        fps = {
            "a1": Fingerprint("1", 100, {"x"}),
            "a2": Fingerprint("2", 100, {"y"}),
        }
        self.assertEqual("a1", _pick_keeper(["a2", "a1"], docs, fps))

    def test_pick_keeper_ignores_metadata_quality_when_format_wins(self) -> None:
        docs = {
            "pdf": DocMeta("pdf", "u", "application/pdf", "/x/a.pdf", "Very rich title", "Known Author", "9781402894626", "2000"),
            "epub": DocMeta("epub", "u", "application/epub+zip", "/x/b.epub", None, None, None, None),
        }
        fps = {
            "pdf": Fingerprint("1", 500, {"x"}),
            "epub": Fingerprint("2", 150, {"x"}),
        }
        self.assertEqual("epub", _pick_keeper(["pdf", "epub"], docs, fps))

    def test_build_duplicate_groups(self) -> None:
        docs = {
            "a": DocMeta("a", "u", "application/pdf", "/x/a.pdf", "A", "X", None, "2000"),
            "b": DocMeta("b", "u", "application/epub+zip", "/x/b.epub", "A", "X", None, "2000"),
            "c": DocMeta("c", "u", "application/pdf", "/x/c.pdf", "A", "X", None, "2000"),
        }
        fps = {
            "a": Fingerprint("1", 100, {"x"}),
            "b": Fingerprint("2", 90, {"x"}),
            "c": Fingerprint("3", 110, {"x"}),
        }
        groups = _build_duplicate_groups([("a", "b"), ("b", "c")], docs, fps)
        self.assertEqual(1, len(groups))
        self.assertEqual("b", groups[0]["keeper_md5"])
        self.assertEqual({"a", "c"}, set(groups[0]["duplicate_md5s"]))

    def test_parse_s3_location(self) -> None:
        bucket, key = _parse_s3_location(
            "https://storage.yandexcloud.net/my-bucket/path/to/file.zip",
            "fallback-bucket",
            "fallback.zip",
        )
        self.assertEqual("my-bucket", bucket)
        self.assertEqual("path/to/file.zip", key)
        bucket2, key2 = _parse_s3_location("", "fb", "fk")
        self.assertEqual(("fb", "fk"), (bucket2, key2))

    def test_ensure_local_zip_uses_existing_without_download(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            zpath = os.path.join(tmp, "x.zip")
            with open(zpath, "wb") as f:
                f.write(b"x")
            s3 = Mock()
            with patch("content.dedup.get_in_workdir", return_value=zpath):
                local, bucket, key = _ensure_local_zip(
                    "a" * 32,
                    "https://storage.yandexcloud.net/bucket/key.zip",
                    s3,
                    "fallback",
                    force_download=False,
                )
            self.assertEqual(zpath, local)
            self.assertEqual("bucket", bucket)
            self.assertEqual("key.zip", key)
            s3.download_file.assert_not_called()

    def test_ensure_local_zip_downloads_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            zpath = os.path.join(tmp, "x.zip")
            s3 = Mock()

            def _download(_bucket, _key, path):
                with open(path, "wb") as f:
                    f.write(b"x")

            s3.download_file.side_effect = _download

            with patch("content.dedup.get_in_workdir", return_value=zpath):
                local, bucket, key = _ensure_local_zip(
                    "b" * 32,
                    "https://storage.yandexcloud.net/bucket2/key2.zip",
                    s3,
                    "fallback",
                    force_download=False,
                )
            self.assertEqual(zpath, local)
            self.assertEqual("bucket2", bucket)
            self.assertEqual("key2.zip", key)
            s3.download_file.assert_called_once_with("bucket2", "key2.zip", zpath)

    def test_ensure_local_zip_raises_if_download_did_not_create_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            zpath = os.path.join(tmp, "x.zip")
            s3 = Mock()
            with patch("content.dedup.get_in_workdir", return_value=zpath):
                with self.assertRaises(FileNotFoundError):
                    _ensure_local_zip(
                        "c" * 32,
                        "https://storage.yandexcloud.net/bucket3/key3.zip",
                        s3,
                        "fallback",
                        force_download=False,
                    )

    def test_read_markdown_from_zip_with_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            zpath = os.path.join(tmp, "x.zip")
            with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("another.md", "hello")
            self.assertEqual("hello", _read_markdown_from_zip(zpath, "abc"))

    def test_write_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "report.json")
            stats = DedupStats(
                docs_total=10,
                candidate_docs=4,
                loaded_docs=4,
                duplicate_pairs=1,
                duplicate_groups=1,
            )
            groups = [{"keeper_md5": "a", "duplicate_md5s": ["b"], "members": []}]
            pairs = [{"a_md5": "a", "b_md5": "b"}]
            _write_report(path, 0.98, 80, stats, groups, pairs)
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.assertEqual(0.98, data["settings"]["near_full_threshold"])
            self.assertEqual(["b"], data["duplicate_docs"])
            self.assertEqual(1, data["summary"]["duplicate_groups"])

    def test_write_report_includes_failed_buckets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "report_failed.json")
            stats = DedupStats(docs_total=2, candidate_docs=2)
            stats.add_error("download", "a" * 32)
            stats.add_error("read", "b" * 32)
            _write_report(path, 0.98, 80, stats, [], [])
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.assertEqual(["a" * 32], data["failed"]["download"])
            self.assertEqual(["b" * 32], data["failed"]["read"])


if __name__ == "__main__":
    unittest.main()
