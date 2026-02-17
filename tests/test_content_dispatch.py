"""Tests for PDF dispatch predicate construction."""

from __future__ import annotations

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from content import dispatch


class _FakeChannel:
    def __init__(self):
        self.exceeded_keys_set = set()

    def get_all_unprocessable_docs(self):
        return {"deadbeefdeadbeefdeadbeefdeadbeef"}

    def dump(self):
        return None


class _DummyContext:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


class ContentDispatchTests(TestCase):
    """Validate predicate filtering used by PDF extraction workers."""

    def _capture_pdf_predicate(self, *, md5=None, md5s=None):
        cli = SimpleNamespace(md5=md5, md5s=md5s, workers=1, batch_size=10)
        captured = {}

        def _fake_obtain_documents(cli_params, ya_client, entity_cls, predicate, limit, session):
            captured["predicate"] = predicate
            return []

        config = {
            "gemini_api_keys": ["k1"],
            "proxy": None,
            "yandex": {"disk": {"oauth_token": "token"}},
        }

        with (
            patch("content.dispatch.read_config", return_value=config),
            patch("content.dispatch.Channel", _FakeChannel),
            patch("content.dispatch.YaDisk", return_value=_DummyContext()),
            patch("content.dispatch.get_session", return_value=_DummyContext()),
            patch("content.dispatch.obtain_documents", side_effect=_fake_obtain_documents),
            patch("content.dispatch.random.shuffle", side_effect=lambda _x: None),
        ):
            dispatch._process_pdf(cli)

        return captured["predicate"]

    def test_process_pdf_predicate_always_filters_missing_content_url(self):
        predicate = self._capture_pdf_predicate(md5="26b7cc14dfc7e1150f9a9595bc2bad26")
        sql = str(predicate.compile(compile_kwargs={"literal_binds": True}))

        self.assertIn("document.content_url IS NULL", sql)
        self.assertIn("document.md5 NOT IN ('deadbeefdeadbeefdeadbeefdeadbeef')", sql)
        self.assertIn("document.md5 = '26b7cc14dfc7e1150f9a9595bc2bad26'", sql)

    def test_process_pdf_predicate_applies_md5s_on_top_of_base_filters(self):
        predicate = self._capture_pdf_predicate(md5s=["a" * 32, "b" * 32])
        sql = str(predicate.compile(compile_kwargs={"literal_binds": True}))

        self.assertIn("document.content_url IS NULL", sql)
        self.assertIn("document.md5 IN ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')", sql)

