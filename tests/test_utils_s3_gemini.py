"""Unit tests for utility and helper modules with mocked I/O."""

import base64
import os
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from gemini import upload_and_wait  # noqa: E402
from s3 import upload_file  # noqa: E402
from utils import calculate_md5, decrypt, encrypt  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
