"""Chunking-focused regression tests for PDF extraction flow."""

from __future__ import annotations

import json
import os
import random
import re
import sys
import tempfile
import threading
import unittest
from contextlib import ExitStack
from queue import Queue
from types import SimpleNamespace
from unittest.mock import Mock, patch

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from content.chunking import Chunk  # noqa: E402
from content.pdf_extractor import PdfExtractor  # noqa: E402
from dirs import Dirs  # noqa: E402


class _FakePdfDoc:
    def __init__(self, page_count: int):
        self.page_count = page_count

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeClientError(Exception):
    def __init__(self, message: str, code: int = 429, details=None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


class _DummyChannel:
    def __init__(self):
        self.unprocessable = []
        self.exceeded = []

    def add_unprocessable_doc(self, md5):
        self.unprocessable.append(md5)

    def add_exceeded_key(self, key):
        self.exceeded.append(key)


class _TwoChunkPlanner:
    """Predictable two-chunk planner used for prompt continuity tests."""

    def __init__(self, _chunked_results_dir, _pages_count=None, **_kwargs):
        self._chunks = [Chunk(0, 0), Chunk(1, 1)]

    def next(self):
        if not self._chunks:
            return None
        return self._chunks.pop(0)

    def decrease_chunk_size(self):
        return False

    def mark_success(self, _chunk):
        return None

    def verify_complete(self):
        return True, []


class PdfExtractorChunkingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _doc(self, md5: str = "a" * 32):
        return SimpleNamespace(
            md5=md5,
            ya_public_url="https://example.test/doc.pdf",
            sharing_restricted=False,
        )

    def _make_extractor(self):
        return PdfExtractor(
            gemini_api_key="k",
            tasks_queue=Queue(),
            config={},
            s3lient=object(),
            ya_client=object(),
            channel=_DummyChannel(),
            stop_event=threading.Event(),
        )

    def _fake_get_in_workdir(self, *dir_names, file=None):
        names = [d.value if hasattr(d, "value") else str(d) for d in dir_names]
        target_dir = os.path.join(self.tmp.name, *names)
        os.makedirs(target_dir, exist_ok=True)
        if file:
            return os.path.join(target_dir, file)
        return target_dir

    def _chunk_json_path(self, md5: str, start: int, end: int) -> str:
        return os.path.join(self._fake_get_in_workdir(Dirs.CHUNKED_RESULTS, md5), f"chunk-{start}-{end}.json")

    def _write_chunk(self, md5: str, start: int, end: int, content: str | None = None) -> None:
        path = self._chunk_json_path(md5, start, end)
        if content is None:
            content = " ".join(f"P{i}" for i in range(start, end + 1))
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"content": content}, f, ensure_ascii=False)

    def _extract_with_patches(
        self,
        *,
        doc,
        pages_count: int,
        gemini_side_effect=None,
        continue_side_effect=None,
        has_figure_side_effect=None,
        cook_side_effect=None,
        planner_cls=None,
        strict_missing_local: bool = False,
        return_extractor: bool = False,
    ):
        extractor = self._make_extractor()

        def _default_cook(start, end, next_footnote_num, headers_hierarchy, lang_tag):
            return [{"text": f"RANGE:{start}-{end};NEXT:{next_footnote_num}"}]

        def _default_gemini(*_args, **kwargs):
            prompt = kwargs["prompt"][0]["text"]
            m = re.search(r"RANGE:(\d+)-(\d+)", prompt)
            assert m
            start, end = int(m.group(1)), int(m.group(2))
            text = json.dumps({"content": " ".join(f"P{i}" for i in range(start, end + 1))})
            return [SimpleNamespace(text=text, usage_metadata=None)], []

        with ExitStack() as stack:
            stack.enter_context(patch("content.pdf_extractor.download_file_locally", return_value="/tmp/doc.pdf"))
            stack.enter_context(patch.object(PdfExtractor, "_enrich_context", return_value=None))
            stack.enter_context(patch("content.pdf_extractor.get_in_workdir", side_effect=self._fake_get_in_workdir))
            stack.enter_context(patch("content.pdf_extractor.pymupdf.open", return_value=_FakePdfDoc(pages_count)))
            stack.enter_context(patch.object(PdfExtractor, "_create_doc_clice", return_value="/tmp/slice.pdf"))
            stack.enter_context(patch.object(PdfExtractor, "_sleep_if_needed", return_value=None))
            stack.enter_context(
                patch(
                    "content.pdf_extractor.MISSING_LOCAL_CHUNK_IS_UNPROCESSABLE",
                    strict_missing_local,
                )
            )
            stack.enter_context(patch("content.pdf_extractor.tokens_info", return_value=""))
            stack.enter_context(
                patch(
                    "content.pdf_extractor.continue_smoothly",
                    side_effect=continue_side_effect or (lambda prev_chunk_tail, content: content),
                )
            )
            stack.enter_context(
                patch(
                    "content.pdf_extractor.has_figure_tag_with_missing_attributes",
                    side_effect=has_figure_side_effect or (lambda _text: False),
                )
            )
            stack.enter_context(
                patch(
                    "content.pdf_extractor.cook_extraction_prompt",
                    side_effect=cook_side_effect or _default_cook,
                )
            )
            gemini_mock = stack.enter_context(
                patch(
                    "content.pdf_extractor.gemini_api",
                    side_effect=gemini_side_effect or _default_gemini,
                )
            )
            if planner_cls is not None:
                stack.enter_context(patch("content.pdf_extractor.ChunkPlanner", planner_cls))

            result = extractor._extract_doc(doc, gemini_client=object())
        if return_extractor:
            return result, gemini_mock, extractor
        return result, gemini_mock

    @staticmethod
    def _extract_page_tokens(text: str) -> list[int]:
        return [int(x) for x in re.findall(r"P(\d+)", text)]

    def test_reuses_existing_chunks_without_duplicate_tail(self) -> None:
        doc = self._doc("1" * 32)
        self._write_chunk(doc.md5, 0, 2)
        self._write_chunk(doc.md5, 3, 5)

        result, gemini_mock = self._extract_with_patches(doc=doc, pages_count=6)
        self.assertFalse(result["stop_worker"])
        self.assertEqual(0, gemini_mock.call_count)

        with open(result["context"].unformatted_response_md, "r", encoding="utf-8") as f:
            tokens = self._extract_page_tokens(f.read())
        self.assertEqual([0, 1, 2, 3, 4, 5], tokens)

    def test_reuses_superset_local_chunk_instead_of_extracting_trimmed_chunk(self) -> None:
        doc = self._doc("8" * 32)
        self._write_chunk(doc.md5, 0, 0)
        self._write_chunk(doc.md5, 1, 2)

        result, gemini_mock = self._extract_with_patches(doc=doc, pages_count=2)
        self.assertFalse(result["stop_worker"])
        self.assertEqual(0, gemini_mock.call_count)

        chunk_paths = [os.path.basename(p) for p in result["context"].chunk_paths]
        self.assertIn("chunk-1-2.json", chunk_paths)
        self.assertNotIn("chunk-1-1.json", chunk_paths)

    def test_missing_local_chunk_marks_doc_unprocessable_without_gemini(self) -> None:
        doc = self._doc("9" * 32)
        self._write_chunk(doc.md5, 0, 0)

        result, gemini_mock, extractor = self._extract_with_patches(
            doc=doc,
            pages_count=2,
            strict_missing_local=True,
            return_extractor=True,
        )

        self.assertFalse(result["stop_worker"])
        self.assertIsNone(result.get("context"))
        self.assertEqual(0, gemini_mock.call_count)
        self.assertIn(doc.md5, extractor.channel.unprocessable)

    def test_empty_local_chunk_is_reused_without_gemini(self) -> None:
        doc = self._doc("a" * 32)
        self._write_chunk(doc.md5, 0, 0)
        self._write_chunk(doc.md5, 1, 1, content="")

        result, gemini_mock, extractor = self._extract_with_patches(
            doc=doc,
            pages_count=2,
            strict_missing_local=True,
            return_extractor=True,
        )

        self.assertFalse(result["stop_worker"])
        self.assertIsNotNone(result.get("context"))
        self.assertEqual(0, gemini_mock.call_count)
        self.assertNotIn(doc.md5, extractor.channel.unprocessable)

    def test_invalid_local_chunk_marks_doc_unprocessable_without_gemini(self) -> None:
        doc = self._doc("b" * 32)
        self._write_chunk(doc.md5, 0, 0)
        self._write_chunk(doc.md5, 1, 1, content="BADFIG")

        def _has_bad_figure(text: str) -> bool:
            return "BADFIG" in text

        result, gemini_mock, extractor = self._extract_with_patches(
            doc=doc,
            pages_count=2,
            has_figure_side_effect=_has_bad_figure,
            strict_missing_local=True,
            return_extractor=True,
        )

        self.assertFalse(result["stop_worker"])
        self.assertIsNone(result.get("context"))
        self.assertEqual(0, gemini_mock.call_count)
        self.assertIn(doc.md5, extractor.channel.unprocessable)

    def test_resume_with_overlap_gap_and_stale_local_chunks(self) -> None:
        doc = self._doc("2" * 32)
        self._write_chunk(doc.md5, 0, 2)
        self._write_chunk(doc.md5, 2, 4)   # overlap (must be skipped)
        self._write_chunk(doc.md5, 99, 120)  # stale (must be ignored)

        result, gemini_mock = self._extract_with_patches(doc=doc, pages_count=6)
        self.assertEqual(1, gemini_mock.call_count)
        chunk_paths = [os.path.basename(p) for p in result["context"].chunk_paths]
        self.assertNotIn("chunk-2-4.json", chunk_paths)
        self.assertNotIn("chunk-99-120.json", chunk_paths)

        with open(result["context"].unformatted_response_md, "r", encoding="utf-8") as f:
            tokens = self._extract_page_tokens(f.read())
        self.assertEqual([0, 1, 2, 3, 4, 5], tokens)

    def test_retry_after_client_error_keeps_full_coverage(self) -> None:
        doc = self._doc("3" * 32)
        calls = {"n": 0}
        prompts = []

        def _cook(start, end, next_footnote_num, headers_hierarchy, lang_tag):
            prompts.append((start, end, next_footnote_num))
            return [{"text": f"RANGE:{start}-{end};NEXT:{next_footnote_num}"}]

        def _gemini(*_args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise _FakeClientError(
                    "rate limit",
                    code=429,
                    details={"quota": "GenerateContentInputTokensPerModelPerMinute-FreeTier"},
                )
            prompt = kwargs["prompt"][0]["text"]
            m = re.search(r"RANGE:(\d+)-(\d+)", prompt)
            assert m
            start, end = int(m.group(1)), int(m.group(2))
            return [
                SimpleNamespace(
                    text=json.dumps({"content": " ".join(f"P{i}" for i in range(start, end + 1))}),
                    usage_metadata=None,
                )
            ], []

        with patch("content.pdf_extractor.ClientError", _FakeClientError):
            result, gemini_mock = self._extract_with_patches(
                doc=doc,
                pages_count=6,
                gemini_side_effect=_gemini,
                cook_side_effect=_cook,
            )

        self.assertGreaterEqual(gemini_mock.call_count, 3)
        self.assertIn((0, 4, 1), prompts)  # initial large chunk attempt
        self.assertIn((0, 2, 1), prompts)  # retry with smaller chunk starts at same page

        with open(result["context"].unformatted_response_md, "r", encoding="utf-8") as f:
            tokens = self._extract_page_tokens(f.read())
        self.assertEqual([0, 1, 2, 3, 4, 5], tokens)

    def test_local_chunk_reextracted_when_figure_tag_is_invalid(self) -> None:
        doc = self._doc("4" * 32)
        self._write_chunk(doc.md5, 0, 0, content="BADFIG")

        def _has_bad_figure(text: str) -> bool:
            return "BADFIG" in text

        result, gemini_mock = self._extract_with_patches(
            doc=doc,
            pages_count=1,
            has_figure_side_effect=_has_bad_figure,
        )
        self.assertEqual(1, gemini_mock.call_count)
        with open(result["context"].unformatted_response_md, "r", encoding="utf-8") as f:
            self.assertEqual([0], self._extract_page_tokens(f.read()))

    def test_continue_smoothly_applies_between_chunks(self) -> None:
        doc = self._doc("5" * 32)
        self._write_chunk(doc.md5, 0, 0)
        self._write_chunk(doc.md5, 1, 1)

        continue_mock = Mock(side_effect=lambda prev_chunk_tail, content: f"<<{content}>>")
        result, _ = self._extract_with_patches(
            doc=doc,
            pages_count=2,
            continue_side_effect=continue_mock,
        )
        self.assertEqual(1, continue_mock.call_count)
        with open(result["context"].unformatted_response_md, "r", encoding="utf-8") as f:
            text = f.read()
        self.assertIn("<<P1>>", text)

    def test_next_footnote_number_propagates_to_next_prompt(self) -> None:
        doc = self._doc("6" * 32)
        seen_next_numbers = []

        def _cook(start, end, next_footnote_num, headers_hierarchy, lang_tag):
            seen_next_numbers.append(next_footnote_num)
            return [{"text": f"RANGE:{start}-{end};NEXT:{next_footnote_num}"}]

        call_index = {"n": 0}

        def _gemini(*_args, **kwargs):
            call_index["n"] += 1
            if call_index["n"] == 1:
                payload = {"content": "P0\n[^7]: footnote text\n"}
            else:
                payload = {"content": "P1"}
            return [SimpleNamespace(text=json.dumps(payload), usage_metadata=None)], []

        self._extract_with_patches(
            doc=doc,
            pages_count=2,
            gemini_side_effect=_gemini,
            cook_side_effect=_cook,
            planner_cls=_TwoChunkPlanner,
        )
        self.assertEqual([1, 8], seen_next_numbers)

    def test_part_file_is_not_treated_as_completed_chunk(self) -> None:
        doc = self._doc("7" * 32)
        chunk_dir = self._fake_get_in_workdir(Dirs.CHUNKED_RESULTS, doc.md5)
        stale_part = os.path.join(chunk_dir, "chunk-0-0.json.part")
        with open(stale_part, "w", encoding="utf-8") as f:
            f.write("stale")

        result, gemini_mock = self._extract_with_patches(doc=doc, pages_count=1)
        self.assertEqual(1, gemini_mock.call_count)
        self.assertTrue(os.path.exists(os.path.join(chunk_dir, "chunk-0-0.json")))
        self.assertFalse(os.path.exists(stale_part))
        with open(result["context"].unformatted_response_md, "r", encoding="utf-8") as f:
            self.assertEqual([0], self._extract_page_tokens(f.read()))

    def test_randomized_extraction_preserves_unique_monotonic_pages(self) -> None:
        rng = random.Random(1337)
        for idx in range(20):
            doc = self._doc(f"{idx:032x}")
            pages_count = rng.randint(1, 14)
            last_page = pages_count - 1

            for _ in range(rng.randint(0, 10)):
                start = rng.randint(-4, pages_count + 4)
                end = start + rng.randint(0, 6)
                clamped_start = max(0, start)
                clamped_end = min(last_page, end)
                if clamped_start <= clamped_end:
                    content = " ".join(f"P{i}" for i in range(clamped_start, clamped_end + 1))
                else:
                    content = "IGNORED"
                self._write_chunk(doc.md5, start, end, content=content)

            result, _ = self._extract_with_patches(doc=doc, pages_count=pages_count)
            with open(result["context"].unformatted_response_md, "r", encoding="utf-8") as f:
                tokens = self._extract_page_tokens(f.read())
            self.assertEqual(list(range(pages_count)), tokens)


if __name__ == "__main__":
    unittest.main()
