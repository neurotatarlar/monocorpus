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
        prompt = build_library_applicability_prompt({"md5": "x"})
        self.assertGreaterEqual(len(prompt), 2)
        text = prompt[0]["text"]
        self.assertIn("applicable(bool)", text)
        self.assertIn("reason(str|null)", text)
        all_text = "\n".join([part["text"] for part in prompt[:-1]])
        self.assertIn("library_classification", all_text)
        self.assertIn("If uncertain, prefer applicable=false.", all_text)
        self.assertIn("If upstream_metadata is provided", all_text)
        self.assertIn('"md5": "x"', prompt[-1]["text"])

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
        self.assertIn('"content_excerpt": "sample excerpt"', prompt[-1]["text"])

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
        self.assertNotIn("name", patch)
        self.assertNotIn("datePublished", patch)
        self.assertNotIn("isbn", patch)
        self.assertEqual("Some description", patch["description"])
        self.assertEqual("Test Publisher", patch["publisher"]["name"])

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

    @patch("metadata.evaluation.gemini_api")
    def test_evaluate_attaches_pdf_slice_when_no_text_excerpt(self, gemini_api_mock) -> None:
        gemini_api_mock.return_value = (
            [SimpleNamespace(text='{"applicable": true, "reason": null, "library_classification": {"ddc": "600", "path": ["Technology", "Engineering"]}}')],
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
        self.assertIn('"upstream_metadata": "{\\"name\\":\\"Upstream\\"}"', prompt[-1]["text"])

    @patch("metadata.evaluation.gemini_api")
    def test_evaluate_requires_classification_for_applicable_doc(self, gemini_api_mock) -> None:
        gemini_api_mock.return_value = (
            [SimpleNamespace(text='{"applicable": true, "reason": "book"}')],
            [],
        )
        with patch.object(LibraryApplicabilityWorker, "_load_content_excerpt", return_value="sample excerpt"):
            evaluation = self._worker()._evaluate(self._task(), gemini_client=object())
        self.assertIsNone(evaluation)

    def test_normalize_library_classification_marks_existing(self) -> None:
        known = [{"ddc": "600", "path": ["Technology", "Engineering"]}]
        normalized = _normalize_library_classification(
            {"ddc": "600", "path": ["Technology", "Engineering"]},
            applicable=True,
            known_classifications=known,
        )
        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual("existing", normalized["source"])

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
