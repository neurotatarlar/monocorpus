"""Focused unit tests for dedup.run orchestration."""

import json
import os
import sys
import tempfile
import types
import unittest
import zipfile
from unittest.mock import Mock, patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

import dedup  # noqa: E402


class _FakeScalarResult:
    def __init__(self, docs):
        self._docs = docs

    def yield_per(self, _size):
        return self._docs


class _FakeSession:
    def __init__(self, docs):
        self._docs = docs

    def scalar(self, _stmt):
        return len(self._docs)

    def scalars(self, _stmt):
        return _FakeScalarResult(self._docs)


class _FakeSessionCtx:
    def __init__(self, docs):
        self._session = _FakeSession(docs)

    def __enter__(self):
        return self._session

    def __exit__(self, exc_type, exc, tb):
        return False


class DedupRunTests(unittest.TestCase):
    def test_run_reports_duplicate_group_and_keeper_priority(self) -> None:
        md5_pdf = "a" * 32
        md5_epub = "b" * 32
        md5_other = "c" * 32

        docs = [
            types.SimpleNamespace(
                md5=md5_pdf,
                content_url="https://storage.yandexcloud.net/cont-bucket/a.zip",
                mime_type="application/pdf",
                ya_path="/docs/book.pdf",
                title="Duplicate Book",
                author="Author One",
                isbn="978-1-4028-9462-6",
                publish_date="2002",
            ),
            types.SimpleNamespace(
                md5=md5_epub,
                content_url="https://storage.yandexcloud.net/cont-bucket/b.zip",
                mime_type="application/epub+zip",
                ya_path="/docs/book.epub",
                title="Duplicate Book",
                author="Author One",
                isbn="9781402894626",
                publish_date="2002",
            ),
            types.SimpleNamespace(
                md5=md5_other,
                content_url="https://storage.yandexcloud.net/cont-bucket/c.zip",
                mime_type="application/pdf",
                ya_path="/docs/other.pdf",
                title="Another Title",
                author="Another Author",
                isbn=None,
                publish_date="1999",
            ),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            zips = {}
            for md5, text in (
                (md5_pdf, "A paragraph " * 30),
                (md5_epub, "A paragraph " * 30),
                (md5_other, "Completely different text " * 20),
            ):
                path = os.path.join(tmp, f"{md5}.zip")
                with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(f"{md5}.md", text)
                zips[md5] = path

            report_path = os.path.join(tmp, "dedup_report.json")

            def _ensure_local(md5, content_url, s3client, fallback_bucket, force_download):
                return zips[md5], "cont-bucket", f"{md5}.zip"

            with (
                patch.object(dedup, "read_config", return_value={"yandex": {"cloud": {"bucket": {"content": "cont-bucket"}}}}),
                patch.object(dedup, "create_session", return_value=Mock()),
                patch.object(dedup, "get_session", return_value=_FakeSessionCtx(docs)),
                patch.object(dedup, "_ensure_local_zip", side_effect=_ensure_local),
            ):
                dedup.run(report_path=report_path, threshold=0.98)

            with open(report_path, "r", encoding="utf-8") as f:
                report = json.load(f)

            self.assertEqual(1, report["summary"]["duplicate_groups"])
            self.assertEqual(1, report["summary"]["duplicate_pairs"])
            self.assertEqual([md5_pdf], report["duplicate_docs"])
            self.assertEqual(1, len(report["duplicate_groups"]))
            group = report["duplicate_groups"][0]
            self.assertEqual(md5_epub, group["keeper_md5"])
            self.assertEqual([md5_pdf], group["duplicate_md5s"])
            self.assertEqual({}, report["failed"])

    def test_run_records_read_errors_for_bad_archives(self) -> None:
        md5_no_md = "d" * 32
        md5_corrupt = "e" * 32

        docs = [
            types.SimpleNamespace(
                md5=md5_no_md,
                content_url="https://storage.yandexcloud.net/cont-bucket/d.zip",
                mime_type="application/pdf",
                ya_path="/docs/bad1.pdf",
                title="Bad Archives",
                author="Author X",
                isbn="9781402894626",
                publish_date="2001",
            ),
            types.SimpleNamespace(
                md5=md5_corrupt,
                content_url="https://storage.yandexcloud.net/cont-bucket/e.zip",
                mime_type="application/pdf",
                ya_path="/docs/bad2.pdf",
                title="Bad Archives",
                author="Author X",
                isbn="9781402894626",
                publish_date="2001",
            ),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            zip_no_md = os.path.join(tmp, f"{md5_no_md}.zip")
            with zipfile.ZipFile(zip_no_md, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("notes.txt", "not markdown")

            zip_corrupt = os.path.join(tmp, f"{md5_corrupt}.zip")
            with open(zip_corrupt, "wb") as f:
                f.write(b"not-a-zip")

            report_path = os.path.join(tmp, "dedup_report_errors.json")

            def _ensure_local(md5, content_url, s3client, fallback_bucket, force_download):
                if md5 == md5_no_md:
                    return zip_no_md, "cont-bucket", f"{md5}.zip"
                return zip_corrupt, "cont-bucket", f"{md5}.zip"

            with (
                patch.object(dedup, "read_config", return_value={"yandex": {"cloud": {"bucket": {"content": "cont-bucket"}}}}),
                patch.object(dedup, "create_session", return_value=Mock()),
                patch.object(dedup, "get_session", return_value=_FakeSessionCtx(docs)),
                patch.object(dedup, "_ensure_local_zip", side_effect=_ensure_local),
            ):
                dedup.run(report_path=report_path, threshold=0.98)

            with open(report_path, "r", encoding="utf-8") as f:
                report = json.load(f)

            self.assertEqual(2, report["summary"]["read_errors"])
            self.assertEqual(0, report["summary"]["loaded_docs"])
            self.assertEqual(0, report["summary"]["duplicate_pairs"])
            self.assertEqual(sorted([md5_no_md, md5_corrupt]), sorted(report["failed"]["read"]))

    def test_run_records_download_error(self) -> None:
        md5_ok = "1" * 32
        md5_missing = "2" * 32
        docs = [
            types.SimpleNamespace(
                md5=md5_ok,
                content_url="https://storage.yandexcloud.net/cont-bucket/1.zip",
                mime_type="application/pdf",
                ya_path="/docs/a.pdf",
                title="Same title",
                author="Same author",
                isbn="9781402894626",
                publish_date="2001",
            ),
            types.SimpleNamespace(
                md5=md5_missing,
                content_url="https://storage.yandexcloud.net/cont-bucket/2.zip",
                mime_type="application/pdf",
                ya_path="/docs/b.pdf",
                title="Same title",
                author="Same author",
                isbn="9781402894626",
                publish_date="2001",
            ),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            ok_zip = os.path.join(tmp, f"{md5_ok}.zip")
            with zipfile.ZipFile(ok_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(f"{md5_ok}.md", "same content " * 20)

            report_path = os.path.join(tmp, "dedup_report_download.json")

            def _ensure_local(md5, content_url, s3client, fallback_bucket, force_download):
                if md5 == md5_missing:
                    raise FileNotFoundError("missing")
                return ok_zip, "cont-bucket", f"{md5}.zip"

            with (
                patch.object(dedup, "read_config", return_value={"yandex": {"cloud": {"bucket": {"content": "cont-bucket"}}}}),
                patch.object(dedup, "create_session", return_value=Mock()),
                patch.object(dedup, "get_session", return_value=_FakeSessionCtx(docs)),
                patch.object(dedup, "_ensure_local_zip", side_effect=_ensure_local),
            ):
                dedup.run(report_path=report_path, threshold=0.98)

            with open(report_path, "r", encoding="utf-8") as f:
                report = json.load(f)

            self.assertEqual(1, report["summary"]["download_errors"])
            self.assertEqual(1, report["summary"]["loaded_docs"])
            self.assertEqual([md5_missing], report["failed"]["download"])
            self.assertEqual(0, report["summary"]["duplicate_pairs"])

    def test_run_skips_large_candidate_groups(self) -> None:
        docs = []
        with tempfile.TemporaryDirectory() as tmp:
            zips = {}
            for idx in range(4):
                md5 = f"{idx+1:032d}"[-32:]
                docs.append(
                    types.SimpleNamespace(
                        md5=md5,
                        content_url=f"https://storage.yandexcloud.net/cont-bucket/{md5}.zip",
                        mime_type="application/pdf",
                        ya_path=f"/docs/{md5}.pdf",
                        title="Same title for all",
                        author="Same author",
                        isbn="9781402894626",
                        publish_date="2001",
                    )
                )
                path = os.path.join(tmp, f"{md5}.zip")
                with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(f"{md5}.md", "shared text " * 40)
                zips[md5] = path

            report_path = os.path.join(tmp, "dedup_report_large.json")

            def _ensure_local(md5, content_url, s3client, fallback_bucket, force_download):
                return zips[md5], "cont-bucket", f"{md5}.zip"

            with (
                patch.object(dedup, "read_config", return_value={"yandex": {"cloud": {"bucket": {"content": "cont-bucket"}}}}),
                patch.object(dedup, "create_session", return_value=Mock()),
                patch.object(dedup, "get_session", return_value=_FakeSessionCtx(docs)),
                patch.object(dedup, "_ensure_local_zip", side_effect=_ensure_local),
            ):
                dedup.run(report_path=report_path, threshold=0.98, max_group_size=2)

            with open(report_path, "r", encoding="utf-8") as f:
                report = json.load(f)

            self.assertGreaterEqual(report["summary"]["groups_skipped_large"], 1)
            self.assertEqual(0, report["summary"]["pairs_checked"])
            self.assertEqual(0, report["summary"]["duplicate_pairs"])


if __name__ == "__main__":
    unittest.main()
