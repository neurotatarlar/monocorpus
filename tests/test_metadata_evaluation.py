"""Unit tests for metadata library-applicability evaluation."""

from __future__ import annotations

from queue import Queue
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from metadata.evaluation import (
    EvaluationTask,
    LibraryApplicabilityWorker,
    _build_applicability_prompt,
    _build_content_excerpt,
)


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
            content_url="https://storage.example/content/0.zip",
            schema_org={"name": "Sample title", "datePublished": "2018"},
        )

    def test_build_applicability_prompt_includes_core_policy(self) -> None:
        prompt = _build_applicability_prompt({"md5": "x"})
        self.assertEqual(2, len(prompt))
        text = prompt[0]["text"]
        self.assertIn("applicable(bool)", text)
        self.assertIn("reason(str|null)", text)
        self.assertIn("If uncertain, prefer applicable=false.", text)
        self.assertIn('"md5": "x"', prompt[1]["text"])

    @patch("metadata.evaluation.gemini_api")
    def test_evaluate_parses_library_decision(self, gemini_api_mock) -> None:
        gemini_api_mock.return_value = (
            [SimpleNamespace(text='{"applicable": false, "reason": "legal act"}')],
            None,
        )

        with patch.object(LibraryApplicabilityWorker, "_load_content_excerpt", return_value="sample excerpt"):
            evaluation = self._worker()._evaluate(self._task(), gemini_client=object())

        self.assertIsNotNone(evaluation)
        self.assertFalse(evaluation.applicable)
        self.assertEqual("legal act", evaluation.reason)
        prompt = gemini_api_mock.call_args.kwargs["prompt"]
        self.assertIn("public library collection for general readers", prompt[0]["text"])
        self.assertIn('"content_excerpt": "sample excerpt"', prompt[1]["text"])

    @patch("metadata.evaluation.gemini_api")
    def test_evaluate_returns_none_for_empty_response(self, gemini_api_mock) -> None:
        gemini_api_mock.return_value = ([SimpleNamespace(text="")], None)

        with patch.object(LibraryApplicabilityWorker, "_load_content_excerpt", return_value=None):
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
