"""Unit tests for utility and helper modules with mocked I/O."""

import base64
import io
import json
import os
import sys
import subprocess
import tempfile
import unittest
import zipfile
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from gemini import create_client, gemini_cli, upload_and_wait  # noqa: E402
from s3 import download, upload_file  # noqa: E402
from utils import (  # noqa: E402
    _get_bucket_id,
    calculate_md5,
    decrypt,
    download_file_locally,
    dump_expired_keys,
    encrypt,
    load_expired_keys,
    load_upstream_metadata,
)


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

    def test_s3_upload_file_without_skip_flag_always_uploads(self) -> None:
        session = Mock()
        session.list_objects_v2.return_value = {"Contents": [{"Key": "key"}]}
        _ = upload_file("/tmp/file", "bucket", "key", session, skip_if_exists=False)
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

    def test_upload_and_wait_immediate_active(self) -> None:
        client = Mock()
        uploaded = Mock()
        uploaded.name = "f1"
        state_active = Mock()
        state_active.state = "ACTIVE"

        client.files.upload.return_value = uploaded
        client.files.get.return_value = state_active

        with patch("gemini.time.sleep") as sleep:
            result = upload_and_wait(client, "/tmp/a.pdf", "application/pdf", poll_interval=0.1, timeout=1)
        self.assertIs(result, state_active)
        sleep.assert_not_called()

    def test_s3_download_skips_existing_and_downloads_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            existing = os.path.join(tmp, "a.txt")
            with open(existing, "w", encoding="utf-8") as f:
                f.write("ok")

            s3 = Mock()
            paginator = Mock()
            paginator.paginate.return_value = [
                {"Contents": [{"Key": "root/a.txt"}, {"Key": "root/sub/b.txt"}]}
            ]
            s3.get_paginator.return_value = paginator

            with patch("s3.create_session", return_value=s3):
                yielded = list(download("bucket", tmp, prefix="root/"))

            self.assertEqual([existing, os.path.join(tmp, "sub", "b.txt")], yielded)
            s3.download_file.assert_called_once_with("bucket", "root/sub/b.txt", os.path.join(tmp, "sub", "b.txt"))

    def test_s3_download_handles_empty_pages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s3 = Mock()
            paginator = Mock()
            paginator.paginate.return_value = [{"Contents": []}, {}]
            s3.get_paginator.return_value = paginator
            with patch("s3.create_session", return_value=s3):
                yielded = list(download("bucket", tmp, prefix="root/"))
            self.assertEqual([], yielded)
            s3.download_file.assert_not_called()

    def test_gemini_api_uploads_files_and_passes_config(self) -> None:
        from gemini import gemini_api

        client = Mock()
        client.models.generate_content_stream.return_value = "stream"
        uploaded = Mock()
        prompt = ["user text"]
        schema = {"type": "object"}

        with patch("gemini.upload_and_wait", return_value=uploaded) as upload_wait:
            stream, uploaded_files = gemini_api(
                prompt=prompt,
                model="gemini-model",
                client=client,
                files={"/tmp/a.pdf": "application/pdf"},
                temperature=0.3,
                schema=schema,
                timeout_sec=7,
            )

        self.assertEqual("stream", stream)
        self.assertEqual([uploaded], uploaded_files)
        upload_wait.assert_called_once_with(client, "/tmp/a.pdf", "application/pdf")
        call = client.models.generate_content_stream.call_args
        self.assertEqual("gemini-model", call.kwargs["model"])
        self.assertEqual(["user text", uploaded], call.kwargs["contents"])

    def test_gemini_cli_invokes_subprocess_with_env(self) -> None:
        config = {"google_api_key": {"free": "secret-key"}}
        proc = Mock()
        with patch("gemini.subprocess.run", return_value=proc) as run:
            result = gemini_cli(config, "hello")
        self.assertIs(proc, result)
        kwargs = run.call_args.kwargs
        self.assertEqual("secret-key", kwargs["env"]["GEMINI_API_KEY"])
        self.assertEqual("hello", run.call_args.args[0][4])
        self.assertTrue(kwargs["check"])

    def test_gemini_cli_raises_on_subprocess_error(self) -> None:
        config = {"google_api_key": {"free": "secret-key"}}
        err = subprocess.CalledProcessError(1, "cmd", stderr="boom")
        with patch("gemini.subprocess.run", side_effect=err):
            with self.assertRaises(subprocess.CalledProcessError):
                gemini_cli(config, "hello")

    def test_create_client_calls_genai_client(self) -> None:
        with patch("gemini.genai.Client", return_value="client") as client_ctor:
            client = create_client("k")
        self.assertEqual("client", client)
        client_ctor.assert_called_once_with(api_key="k")

    def test_bucket_id_cutoff_logic(self) -> None:
        with patch("utils.datetime") as dt:
            dt.now.return_value = datetime(2026, 1, 2, 8, 59, tzinfo=timezone.utc)
            self.assertEqual("20260101_1", _get_bucket_id())

        with patch("utils.datetime") as dt:
            dt.now.return_value = datetime(2026, 1, 2, 9, 0, tzinfo=timezone.utc)
            self.assertEqual("20260102_0", _get_bucket_id())

    def test_dump_and_load_expired_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch("utils._get_bucket_id", return_value="20250101_0"):
                dump_expired_keys({"k1", "k2"}, dir=tmp)
                loaded = load_expired_keys(dir=tmp)
                self.assertEqual({"k1", "k2"}, loaded)

            with patch("utils._get_bucket_id", return_value="20250101_1"):
                self.assertEqual(set(), load_expired_keys(dir=tmp))

    def test_download_file_locally_skips_download_when_hash_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            content = b"hello"
            md5 = "5d41402abc4b2a76b9719d911017c592"
            local_path = os.path.join(tmp, f"{md5}.pdf")
            with open(local_path, "wb") as f:
                f.write(content)

            doc = Mock(
                md5=md5,
                ya_path="/x/doc.pdf",
                mime_type="application/pdf",
                ya_public_url="https://public",
                sharing_restricted=False,
            )
            ya_client = Mock()
            with patch("utils.get_in_workdir", return_value=local_path):
                path = download_file_locally(ya_client, doc, config={})
            self.assertEqual(local_path, path)
            ya_client.download_public.assert_not_called()

    def test_download_file_locally_uses_decrypted_url_and_mime_extension(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            md5 = "d41d8cd98f00b204e9800998ecf8427e"
            local_path = os.path.join(tmp, f"{md5}.pdf")
            doc = Mock(
                md5=md5,
                ya_path="/x/doc_without_ext",
                mime_type="application/pdf",
                ya_public_url="enc:xxx",
                sharing_restricted=True,
            )
            ya_client = Mock()

            def _download_public(_url, fp):
                fp.write(b"")

            ya_client.download_public.side_effect = _download_public

            with (
                patch("utils.get_in_workdir", return_value=local_path),
                patch("utils.decrypt", return_value="https://decrypted"),
            ):
                path = download_file_locally(ya_client, doc, config={"encryption_key": "x"})

            self.assertEqual(local_path, path)
            ya_client.download_public.assert_called_once_with("https://decrypted", unittest.mock.ANY)

    def test_load_upstream_metadata_none_url(self) -> None:
        self.assertIsNone(load_upstream_metadata(None, "abc"))

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
