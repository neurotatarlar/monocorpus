"""Unit tests for utility and helper modules with mocked I/O."""

import base64
import io
import json
import os
import sys
import tempfile
import unittest
import zipfile
from unittest.mock import Mock, patch

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from gemini import upload_and_wait  # noqa: E402
from s3 import upload_file  # noqa: E402
from utils import calculate_md5, decrypt, encrypt, load_upstream_metadata  # noqa: E402


class UtilsS3GeminiTests(unittest.TestCase):
    def test_calculate_md5(self) -> None:
        with tempfile.NamedTemporaryFile("wb", delete=True) as f:
            f.write(b"abc")
            f.flush()
            self.assertEqual("900150983cd24fb0d6963f7d28e17f72", calculate_md5(f.name))

    def test_encrypt_decrypt_roundtrip(self) -> None:
        key = AESGCM.generate_key(bit_length=128)
        cfg = {"encryption_key": base64.urlsafe_b64encode(key).decode("utf-8")}
        plain = "https://example.org/doc/123"
        cipher = encrypt(plain, cfg)
        self.assertTrue(cipher.startswith("enc:"))
        self.assertEqual(plain, decrypt(cipher, cfg))

    def test_s3_upload_file_skip_if_exists(self) -> None:
        session = Mock()
        session.list_objects_v2.return_value = {"Contents": [{"Key": "x"}]}
        url = upload_file("/tmp/file", "bucket", "key", session, skip_if_exists=True)
        session.upload_file.assert_not_called()
        self.assertEqual(f"{session._endpoint.host}/bucket/key", url)

    def test_s3_upload_file_uploads_when_missing(self) -> None:
        session = Mock()
        session.list_objects_v2.return_value = {}
        _ = upload_file("/tmp/file", "bucket", "key", session, skip_if_exists=True)
        session.upload_file.assert_called_once_with("/tmp/file", "bucket", "key")

    def test_upload_and_wait_success(self) -> None:
        client = Mock()
        uploaded = Mock()
        uploaded.name = "f1"
        state_processing = Mock()
        state_processing.state = "PROCESSING"
        state_active = Mock()
        state_active.state = "ACTIVE"

        client.files.upload.return_value = uploaded
        client.files.get.side_effect = [state_processing, state_active]

        with patch("gemini.time.sleep") as sleep:
            result = upload_and_wait(client, "/tmp/a.pdf", "application/pdf", poll_interval=0.1, timeout=1)
        self.assertIs(result, state_active)
        self.assertEqual(2, client.files.get.call_count)
        sleep.assert_called_once()

    def test_upload_and_wait_timeout(self) -> None:
        client = Mock()
        uploaded = Mock()
        uploaded.name = "f1"
        state_processing = Mock()
        state_processing.state = "PROCESSING"

        client.files.upload.return_value = uploaded
        client.files.get.side_effect = [state_processing, state_processing, state_processing]

        with patch("gemini.time.sleep"):
            with self.assertRaises(TimeoutError):
                upload_and_wait(client, "/tmp/a.pdf", "application/pdf", poll_interval=0.1, timeout=0.2)

    def test_load_upstream_metadata_strips_unwanted_fields(self) -> None:
        raw_meta = {
            "title": "Book",
            "available_pages": [1, 2],
            "doc_card_url": "x",
            "download_code": "x",
            "doc_url": "x",
            "access": "x",
            "lang": "tt",
        }
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("metadata.json", json.dumps(raw_meta))
        payload = buf.getvalue()

        class _Resp:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def raise_for_status(self):
                return None

            def iter_content(self, chunk_size=8192):
                for i in range(0, len(payload), chunk_size):
                    yield payload[i : i + chunk_size]

        with tempfile.TemporaryDirectory() as tmp:
            def _fake_get_in_workdir(*dir_names, file=None, prefix=None):
                parts = [getattr(p, "value", p) for p in dir_names]
                path = os.path.join(tmp, *parts)
                os.makedirs(path, exist_ok=True)
                if file:
                    return os.path.join(path, file)
                return path

            with (
                patch("utils.get_in_workdir", side_effect=_fake_get_in_workdir),
                patch("utils.requests.get", return_value=_Resp()),
            ):
                out = load_upstream_metadata("https://example.org/meta.zip", "abc")
            parsed = json.loads(out)
            self.assertEqual("Book", parsed["title"])
            self.assertNotIn("available_pages", parsed)
            self.assertNotIn("doc_card_url", parsed)
            self.assertNotIn("download_code", parsed)
            self.assertNotIn("doc_url", parsed)
            self.assertNotIn("access", parsed)
            self.assertNotIn("lang", parsed)


if __name__ == "__main__":
    unittest.main()
