"""Tests for PDF postprocess image URL/upload behavior."""

from __future__ import annotations

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from content.pdf_postprocess import _upload_to_s3


class PdfPostprocessUploadTests(TestCase):
    """Ensure image URL handling works with and without actual S3 upload."""

    def setUp(self):
        self.session = SimpleNamespace(_endpoint=SimpleNamespace(host="https://storage.yandexcloud.net"))
        self.config = {"yandex": {"cloud": {"bucket": {"image": "clips-bucket"}}}}

    def test_upload_to_s3_upload_false_sets_url_without_upload(self):
        pairs = [{"path": "/tmp/doc-1-0.png"}]

        with patch("content.pdf_postprocess.upload_file") as upload_mock:
            _upload_to_s3(pairs, self.session, self.config, upload=False)

        upload_mock.assert_not_called()
        self.assertEqual("https://storage.yandexcloud.net/clips-bucket/doc-1-0.png", pairs[0]["url"])

    def test_upload_to_s3_upload_true_calls_upload_and_uses_returned_url(self):
        pairs = [{"path": "/tmp/doc-2-3.png"}]
        returned_url = "https://storage.yandexcloud.net/clips-bucket/doc-2-3.png"

        with patch("content.pdf_postprocess.upload_file", return_value=returned_url) as upload_mock:
            _upload_to_s3(pairs, self.session, self.config, upload=True)

        upload_mock.assert_called_once_with(
            "/tmp/doc-2-3.png",
            "clips-bucket",
            "doc-2-3.png",
            self.session,
            skip_if_exists=True,
        )
        self.assertEqual(returned_url, pairs[0]["url"])

