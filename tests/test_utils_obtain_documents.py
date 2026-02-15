"""Unit tests for utils.obtain_documents lazy-session behavior."""

import os
import sys
import types
import unittest
from unittest.mock import Mock, patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from utils import obtain_documents  # noqa: E402


class ObtainDocumentsTests(unittest.TestCase):
    def test_obtain_documents_uses_lazy_get_session_when_none(self) -> None:
        cli_params = types.SimpleNamespace(md5=None, path=None)
        fake_session = object()
        fake_doc = types.SimpleNamespace(md5="x")

        with (
            patch("utils.get_session", return_value=fake_session) as get_session,
            patch("utils._find", return_value=iter([fake_doc])) as find_docs,
        ):
            docs = list(
                obtain_documents(
                    cli_params=cli_params,
                    ya_client=Mock(),
                    entity_cls=object,
                    predicate=None,
                    session=None,
                )
            )

        get_session.assert_called_once()
        find_docs.assert_called_once()
        self.assertEqual([fake_doc], docs)

    def test_obtain_documents_respects_explicit_session(self) -> None:
        cli_params = types.SimpleNamespace(md5=None, path=None)
        explicit_session = object()
        fake_doc = types.SimpleNamespace(md5="y")

        with (
            patch("utils.get_session") as get_session,
            patch("utils._find", return_value=iter([fake_doc])) as find_docs,
        ):
            docs = list(
                obtain_documents(
                    cli_params=cli_params,
                    ya_client=Mock(),
                    entity_cls=object,
                    predicate=None,
                    session=explicit_session,
                )
            )

        get_session.assert_not_called()
        find_docs.assert_called_once_with(
            explicit_session,
            predicate=None,
            limit=None,
            offset=None,
            entity_cls=object,
        )
        self.assertEqual([fake_doc], docs)

    def test_obtain_documents_path_file_uses_md5_lookup(self) -> None:
        cli_params = types.SimpleNamespace(md5=None, path="/disk/file")
        explicit_session = object()
        fake_doc = types.SimpleNamespace(md5="z")
        fake_entity = types.SimpleNamespace(md5=object())
        ya_client = Mock()
        ya_client.get_meta.return_value = types.SimpleNamespace(md5="z", type="file", path="/disk/file")

        with patch("utils._find", return_value=iter([fake_doc])) as find_docs:
            docs = list(
                obtain_documents(
                    cli_params=cli_params,
                    ya_client=ya_client,
                    entity_cls=fake_entity,
                    predicate=None,
                    session=explicit_session,
                )
            )

        ya_client.get_meta.assert_called_once_with("/disk/file", fields=["md5", "type", "path"])
        find_docs.assert_called_once()
        self.assertEqual([fake_doc], docs)

    def test_obtain_documents_md5s_uses_single_in_lookup(self) -> None:
        md5s = ["a" * 32, "b" * 32]
        cli_params = types.SimpleNamespace(md5=None, md5s=md5s, path=None)
        explicit_session = object()
        fake_docs = [types.SimpleNamespace(md5=md5s[0]), types.SimpleNamespace(md5=md5s[1])]

        class _FakeCol:
            def in_(self, items):
                return ("in", tuple(items))

        fake_entity = types.SimpleNamespace(md5=_FakeCol())

        with patch("utils._find", return_value=iter(fake_docs)) as find_docs:
            docs = list(
                obtain_documents(
                    cli_params=cli_params,
                    ya_client=Mock(),
                    entity_cls=fake_entity,
                    predicate=None,
                    session=explicit_session,
                )
            )

        find_docs.assert_called_once()
        kwargs = find_docs.call_args.kwargs
        self.assertEqual(("in", tuple(md5s)), kwargs["predicate"])
        self.assertEqual(fake_docs, docs)


if __name__ == "__main__":
    unittest.main()
