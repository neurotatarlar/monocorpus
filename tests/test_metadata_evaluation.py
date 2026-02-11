"""Unit tests for metadata library-applicability evaluation."""

from __future__ import annotations

from queue import Queue
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from metadata.evaluation import (
    EvaluationTask,
    LibraryApplicabilityWorker,
    _apply_metadata_patch,
    _build_content_excerpt,
    _normalize_library_classification,
    _normalize_metadata_patch,
    _sync_auxiliary_terms_in_about,
    evaluate,
)
from prompts.metadata_evaluation import build_library_applicability_prompt


class _DummyChannel:
    """Minimal channel stub for worker construction in tests."""

    exceeded_keys_set: set[str] = set()

    def add_unprocessable_doc(self, _md5: str) -> None:
        return

    def add_exceeded_key(self, _key: str) -> None:
        return


class MetadataEvaluationTests(unittest.TestCase):
    """Covers prompt construction and response parsing."""

    def _worker(self) -> LibraryApplicabilityWorker:
        return LibraryApplicabilityWorker(
            gemini_api_key="k",
            tasks_queue=Queue(),
            config={},
            channel=_DummyChannel(),
            dry_run=True,
        )

    def test_worker_excerpt_chars_is_int(self) -> None:
        worker = self._worker()
        self.assertIsInstance(worker.excerpt_chars, int)

    def _task(self) -> EvaluationTask:
        return EvaluationTask(
            md5="0" * 32,
            ya_path="/docs/legal.pdf",
            language="tt",
            page_count=123,
            full=True,
            sharing_restricted=False,
            ya_public_url="https://storage.example/public.pdf",
            mime_type="application/pdf",
            document_url=None,
            upstream_meta_url=None,
            content_url="https://storage.example/content/0.zip",
            schema_org={"name": "Sample title", "datePublished": "2018"},
        )

    def test_build_applicability_prompt_includes_core_policy(self) -> None:
        prompt = build_library_applicability_prompt(
            {"md5": "x", "missing_fields": []},
            content_excerpt="excerpt",
        )
        self.assertGreaterEqual(len(prompt), 2)
        text = prompt[0]["text"]
        self.assertIn("applicable(bool)", text)
        self.assertIn("reason(str|null)", text)
        all_text = "\n".join([part["text"] for part in prompt[:-1]])
        self.assertIn("library_ddc", all_text)
        self.assertIn("library_path", all_text)
        self.assertIn("If uncertain, prefer applicable=false.", all_text)
        self.assertIn("If upstream_metadata is provided", all_text)
        payload_text = self._payload_text(prompt)
        self.assertIn('"md5": "x"', payload_text)
        self.assertNotIn('"content_excerpt"', payload_text)
        self.assertTrue(any("CONTENT_EXCERPT:\nexcerpt" == part["text"] for part in prompt))

    @staticmethod
    def _payload_text(prompt: list[dict[str, str]]) -> str:
        return next(part["text"] for part in prompt if part["text"].startswith("{"))

    @patch("metadata.evaluation.gemini_api")
    def test_evaluate_parses_library_decision(self, gemini_api_mock) -> None:
        gemini_api_mock.return_value = (
            [SimpleNamespace(text='{"applicable": false, "reason": "legal act"}')],
            [],
        )

        with patch.object(LibraryApplicabilityWorker, "_load_content_excerpt", return_value="sample excerpt"):
            evaluation = self._worker()._evaluate(self._task(), gemini_client=object())

        self.assertIsNotNone(evaluation)
        self.assertFalse(evaluation.applicable)
        self.assertEqual("legal act", evaluation.reason)
        prompt = gemini_api_mock.call_args.kwargs["prompt"]
        self.assertIn("public library collection for general readers", prompt[0]["text"])
        payload_text = self._payload_text(prompt)
        self.assertNotIn('"content_excerpt"', payload_text)
        self.assertTrue(any("CONTENT_EXCERPT:\nsample excerpt" == part["text"] for part in prompt))

    @patch("metadata.evaluation.gemini_api")
    def test_evaluate_returns_none_for_empty_response(self, gemini_api_mock) -> None:
        gemini_api_mock.return_value = ([SimpleNamespace(text="")], [])

        with (
            patch.object(LibraryApplicabilityWorker, "_load_content_excerpt", return_value=None),
            patch.object(LibraryApplicabilityWorker, "_prepare_pdf_slice_for_eval", return_value=None),
        ):
            evaluation = self._worker()._evaluate(self._task(), gemini_client=object())

        self.assertIsNone(evaluation)

    def test_build_content_excerpt_splits_to_three_parts(self) -> None:
        text = "A" * 6000 + "B" * 6000 + "C" * 6000
        excerpt = _build_content_excerpt(text, 10_000)
        self.assertIsNotNone(excerpt)
        assert excerpt is not None
        self.assertLessEqual(len(excerpt), 10_000)
        self.assertIn("A", excerpt)
        self.assertIn("B", excerpt)
        self.assertIn("C", excerpt)

    def test_build_content_excerpt_returns_none_when_disabled(self) -> None:
        self.assertIsNone(_build_content_excerpt("text", 0))

    def test_normalize_metadata_patch_keeps_only_missing_fields(self) -> None:
        task = EvaluationTask(
            md5="1" * 32,
            ya_path="/docs/book.pdf",
            language="tt",
            page_count=None,
            full=True,
            sharing_restricted=False,
            ya_public_url=None,
            mime_type="application/pdf",
            document_url=None,
            upstream_meta_url=None,
            content_url="https://storage.example/content/1.zip",
            schema_org={
                "name": "Existing",
                "datePublished": "2020",
                "isbn": ["9780306406157"],
            },
        )
        patch = _normalize_metadata_patch(
            {
                "name": "New title should be ignored",
                "datePublished": "1999",
                "isbn": ["9781111111111"],
                "description": "Some description",
                "publisher": "Test Publisher",
            },
            task,
            config={},
        )
        self.assertIsNotNone(patch)
        assert patch is not None
        patch_data = patch.model_dump(by_alias=True, exclude_none=True)
        self.assertNotIn("name", patch_data)
        self.assertNotIn("datePublished", patch_data)
        self.assertNotIn("isbn", patch_data)
        self.assertNotIn("genre", patch_data)
        self.assertEqual("Some description", patch_data["description"])
        self.assertEqual("Test Publisher", patch_data["publisher"]["name"])

    def test_normalize_metadata_patch_allows_genre_refresh(self) -> None:
        task = EvaluationTask(
            md5="1" * 32,
            ya_path="/docs/book.pdf",
            language="tt",
            page_count=None,
            full=True,
            sharing_restricted=False,
            ya_public_url=None,
            mime_type="application/pdf",
            document_url=None,
            upstream_meta_url=None,
            content_url="https://storage.example/content/1.zip",
            schema_org={"genre": ["Old genre"]},
        )
        patch = _normalize_metadata_patch(
            {"genre": ["Novel", "Novel", "Fiction"]},
            task,
            config={},
        )
        self.assertIsNotNone(patch)
        assert patch is not None
        patch_data = patch.model_dump(by_alias=True, exclude_none=True)
        self.assertEqual(["Novel", "Fiction"], patch_data["genre"])

    def test_apply_metadata_patch_updates_missing_values(self) -> None:
        schema = {"name": "Existing title", "publisher": {"@type": "Organization", "name": ""}}
        patch = {
            "name": "Should not overwrite",
            "description": "Desc",
            "publisher": {"@type": "Organization", "name": "Pub"},
        }
        updated, applied = _apply_metadata_patch(schema, patch)
        self.assertEqual("Existing title", updated["name"])
        self.assertEqual("Desc", updated["description"])
        self.assertEqual("Pub", updated["publisher"]["name"])
        self.assertIn("description", applied)
        self.assertIn("publisher", applied)

    def test_sync_auxiliary_terms_in_about_adds_and_removes_values(self) -> None:
        schema = {
            "about": [
                {"@type": "Thing", "name": "Preserved"},
                {"@type": "DefinedTerm", "name": "821.512.145", "termCode": "821.512.145", "inDefinedTermSet": "UDC"},
                {"@type": "DefinedTerm", "name": "Novel", "termCode": "Novel", "inDefinedTermSet": "Genre"},
                {"@type": "DefinedTerm", "name": "Caption", "termCode": "O-1", "inDefinedTermSet": "OtherSet"},
            ],
            "additionalProperty": [
                {"@type": "PropertyValue", "name": "DDC", "value": "300"},
                {"@type": "PropertyValue", "name": "CategoryPath", "value": "Old > Path"},
            ]
        }
        updated, applied = _sync_auxiliary_terms_in_about(
            schema_org=schema,
            applicable=True,
            ddc="600",
            path=["Technology", "Engineering"],
        )
        self.assertIn("about", applied)
        self.assertIn("additionalProperty", applied)
        self.assertIn("genre", applied)
        self.assertEqual(["Novel"], updated["genre"])
        about = updated["about"]
        self.assertTrue(any(item.get("name") == "Preserved" for item in about if isinstance(item, dict)))
        self.assertFalse(any(item.get("inDefinedTermSet") == "Genre" for item in about if isinstance(item, dict)))
        self.assertTrue(
            any(
                item.get("inDefinedTermSet") == "OtherSet" and item.get("termCode") == "O-1"
                for item in about
                if isinstance(item, dict)
            )
        )
        self.assertTrue(
            any(
                item.get("inDefinedTermSet") == "UDC" and item.get("termCode") == "821.512.145"
                for item in about
                if isinstance(item, dict)
            )
        )
        self.assertTrue(
            any(
                item.get("inDefinedTermSet") == "DDC" and item.get("termCode") == "600"
                for item in about
                if isinstance(item, dict)
            )
        )
        self.assertTrue(
            any(
                item.get("inDefinedTermSet") == "CategoryPath"
                and item.get("termCode") == "Technology > Engineering"
                for item in about
                if isinstance(item, dict)
            )
        )
        self.assertFalse(
            any(
                item.get("@type") == "DefinedTerm" and "name" in item
                for item in about
                if isinstance(item, dict)
            )
        )
        self.assertNotIn("additionalProperty", updated)

        cleaned, removed = _sync_auxiliary_terms_in_about(
            schema_org=updated,
            applicable=False,
            ddc=None,
            path=None,
        )
        cleaned_about = cleaned["about"]
        self.assertEqual(["Novel"], cleaned.get("genre"))
        self.assertFalse(any(item.get("inDefinedTermSet") == "DDC" for item in cleaned_about if isinstance(item, dict)))
        self.assertFalse(
            any(item.get("inDefinedTermSet") == "CategoryPath" for item in cleaned_about if isinstance(item, dict))
        )
        self.assertTrue(any(item.get("inDefinedTermSet") == "UDC" for item in cleaned_about if isinstance(item, dict)))
        self.assertTrue(any(item.get("name") == "Preserved" for item in cleaned_about if isinstance(item, dict)))
        self.assertIn("about", removed)

    def test_metadata_patch_serialization_keeps_utf8(self) -> None:
        task = EvaluationTask(
            md5="2" * 32,
            ya_path="/docs/book.pdf",
            language="tt",
            page_count=None,
            full=True,
            sharing_restricted=False,
            ya_public_url=None,
            mime_type="application/pdf",
            document_url=None,
            upstream_meta_url=None,
            content_url="https://storage.example/content/2.zip",
            schema_org={},
        )
        patch = _normalize_metadata_patch(
            {"description": "Китап"},
            task,
            config={},
        )
        self.assertIsNotNone(patch)
        assert patch is not None
        dumped_json = patch.model_dump_json(by_alias=True, exclude_none=True, ensure_ascii=False)
        self.assertIn("Китап", dumped_json)

    @patch("metadata.evaluation.gemini_api")
    def test_evaluate_attaches_pdf_slice_when_no_text_excerpt(self, gemini_api_mock) -> None:
        gemini_api_mock.return_value = (
            [SimpleNamespace(text='{"applicable": true, "reason": null, "library_ddc": "600", "library_path": ["Technology", "Engineering"]}')],
            [],
        )
        with (
            patch.object(LibraryApplicabilityWorker, "_load_content_excerpt", return_value=None),
            patch.object(LibraryApplicabilityWorker, "_prepare_pdf_slice_for_eval", return_value="/tmp/eval-slice.pdf"),
        ):
            evaluation = self._worker()._evaluate(self._task(), gemini_client=object())

        self.assertIsNotNone(evaluation)
        files = gemini_api_mock.call_args.kwargs["files"]
        self.assertEqual({"/tmp/eval-slice.pdf": "application/pdf"}, files)
        prompt = gemini_api_mock.call_args.kwargs["prompt"]
        payload_text = self._payload_text(prompt)
        self.assertNotIn('"content_excerpt": null', payload_text)
        self.assertNotIn('"content_excerpt"', payload_text)
        self.assertNotIn('"upstream_metadata"', payload_text)
        self.assertFalse(any(part["text"].startswith("CONTENT_EXCERPT:\n") for part in prompt))

    @patch("metadata.evaluation.gemini_api")
    @patch("metadata.evaluation.load_upstream_metadata")
    def test_evaluate_includes_upstream_metadata_when_available(self, load_upstream_metadata_mock, gemini_api_mock) -> None:
        load_upstream_metadata_mock.return_value = '{"name":"Upstream"}'
        gemini_api_mock.return_value = (
            [SimpleNamespace(text='{"applicable": false, "reason": "legal act"}')],
            [],
        )
        task = self._task()
        task.upstream_meta_url = "https://storage.example/upstream.zip"
        with patch.object(LibraryApplicabilityWorker, "_load_content_excerpt", return_value="sample excerpt"):
            self._worker()._evaluate(task, gemini_client=object())
        prompt = gemini_api_mock.call_args.kwargs["prompt"]
        payload_text = self._payload_text(prompt)
        self.assertIn('"upstream_metadata": "{\\"name\\":\\"Upstream\\"}"', payload_text)

    @patch("metadata.evaluation.gemini_api")
    def test_evaluate_requires_classification_for_applicable_doc(self, gemini_api_mock) -> None:
        gemini_api_mock.return_value = (
            [SimpleNamespace(text='{"applicable": true, "reason": "book"}')],
            [],
        )
        with patch.object(LibraryApplicabilityWorker, "_load_content_excerpt", return_value="sample excerpt"):
            evaluation = self._worker()._evaluate(self._task(), gemini_client=object())
        self.assertIsNone(evaluation)

    def test_normalize_library_classification_returns_normalized_tuple(self) -> None:
        ddc, path = _normalize_library_classification(
            "600",
            ["Technology", "Engineering"],
            applicable=True,
        )
        self.assertEqual("600", ddc)
        self.assertEqual(["Technology", "Engineering"], path)

    def test_normalize_library_classification_rejects_cyrillic_path(self) -> None:
        ddc, path = _normalize_library_classification(
            "600",
            ["Технология", "Инженерия"],
            applicable=True,
        )
        self.assertIsNone(ddc)
        self.assertIsNone(path)

    @patch("metadata.evaluation.Channel")
    @patch("metadata.evaluation._load_batch")
    @patch("metadata.evaluation.read_config")
    def test_evaluate_continues_after_batch_exception(self, read_config_mock, load_batch_mock, channel_cls_mock) -> None:
        read_config_mock.return_value = {"gemini_api_keys": ["k"], "sup_langs": {"tt": {"codes": ["tt-Cyrl"]}}}
        load_batch_mock.side_effect = [RuntimeError("boom"), []]
        channel = channel_cls_mock.return_value
        channel.exceeded_keys_set = set()
        channel.get_all_unprocessable_docs.return_value = set()

        args = SimpleNamespace(dry_run=True, batch_size=100, workers=1, excerpt_chars=1000)
        evaluate(args)

        self.assertEqual(2, load_batch_mock.call_count)
        self.assertTrue(channel.dump.called)

    @patch("metadata.evaluation.Channel")
    @patch("metadata.evaluation._load_batch")
    @patch("metadata.evaluation.read_config")
    def test_evaluate_handles_keyboard_interrupt(self, read_config_mock, load_batch_mock, channel_cls_mock) -> None:
        read_config_mock.return_value = {"gemini_api_keys": ["k"], "sup_langs": {"tt": {"codes": ["tt-Cyrl"]}}}
        load_batch_mock.side_effect = KeyboardInterrupt()
        channel = channel_cls_mock.return_value
        channel.exceeded_keys_set = set()
        channel.get_all_unprocessable_docs.return_value = set()

        args = SimpleNamespace(dry_run=True, batch_size=100, workers=1, excerpt_chars=1000)
        evaluate(args)

        self.assertEqual(1, load_batch_mock.call_count)
        self.assertTrue(channel.dump.called)
