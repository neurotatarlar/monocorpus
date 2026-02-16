"""Tests for chunk planning edge cases and overlap safety."""

from __future__ import annotations

import os
import random
import sys
import tempfile
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from content.chunking import Chunk, ChunkPlanner  # noqa: E402


def _collect_ranges(planner: ChunkPlanner) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for _ in range(10_000):
        chunk = planner.next()
        if chunk is None:
            break
        out.append((chunk.start, chunk.end))
    return out


class ChunkPlannerTests(unittest.TestCase):
    """Protect against indexing and overlap regressions in chunk planning."""

    def test_pages_count_is_total_pages_not_last_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            planner = ChunkPlanner(tmp, pages_count=3, chunk_sizes=[1])
            self.assertEqual([(0, 0), (1, 1), (2, 2)], _collect_ranges(planner))

    def test_verify_complete_uses_total_pages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            open(os.path.join(tmp, "chunk-0-2.json"), "w", encoding="utf-8").close()
            planner = ChunkPlanner(tmp, pages_count=3, chunk_sizes=[1])
            complete, missing = planner.verify_complete()
            self.assertTrue(complete)
            self.assertEqual([], missing)

    def test_existing_last_chunk_does_not_trigger_extra_tail_page(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            open(os.path.join(tmp, "chunk-0-49.json"), "w", encoding="utf-8").close()
            planner = ChunkPlanner(tmp, pages_count=50, chunk_sizes=[1])
            self.assertEqual([(0, 49)], _collect_ranges(planner))

    def test_legacy_out_of_range_local_chunk_is_clamped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            # Old buggy runs could store end index equal to page_count.
            open(os.path.join(tmp, "chunk-0-50.json"), "w", encoding="utf-8").close()
            planner = ChunkPlanner(tmp, pages_count=50, chunk_sizes=[1])
            self.assertEqual([(0, 49)], _collect_ranges(planner))

    def test_partially_overlapping_local_chunks_are_not_reused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            # 4 overlaps: naive reuse of both chunks duplicates page 4.
            open(os.path.join(tmp, "chunk-0-4.json"), "w", encoding="utf-8").close()
            open(os.path.join(tmp, "chunk-4-6.json"), "w", encoding="utf-8").close()
            planner = ChunkPlanner(tmp, pages_count=7, chunk_sizes=[2])
            ranges = _collect_ranges(planner)

            # No overlap between emitted ranges.
            for prev, curr in zip(ranges, ranges[1:]):
                self.assertGreaterEqual(curr[0], prev[1] + 1)

            # Coverage is complete and exact for 7 pages (0..6).
            covered = set()
            for start, end in ranges:
                covered.update(range(start, end + 1))
            self.assertEqual(set(range(0, 7)), covered)

    def test_gap_between_existing_chunks_is_planned_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            open(os.path.join(tmp, "chunk-0-1.json"), "w", encoding="utf-8").close()
            open(os.path.join(tmp, "chunk-4-5.json"), "w", encoding="utf-8").close()
            planner = ChunkPlanner(tmp, pages_count=6, chunk_sizes=[2])
            self.assertEqual([(0, 1), (2, 3), (4, 5)], _collect_ranges(planner))

    def test_retry_with_smaller_chunk_size_does_not_skip_pages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            planner = ChunkPlanner(tmp, pages_count=10, chunk_sizes=[6, 3, 1])
            first = planner.next()
            self.assertEqual((0, 5), (first.start, first.end))

            self.assertTrue(planner.decrease_chunk_size())
            retry = planner.next()
            self.assertEqual((0, 2), (retry.start, retry.end))
            planner.mark_success(retry)

            # Remaining pages from failed bigger chunk should still be emitted.
            next_chunk = planner.next()
            self.assertEqual((3, 5), (next_chunk.start, next_chunk.end))

    def test_zero_pages_yields_no_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            planner = ChunkPlanner(tmp, pages_count=0, chunk_sizes=[5])
            self.assertIsNone(planner.next())
            complete, missing = planner.verify_complete()
            self.assertTrue(complete)
            self.assertEqual([], missing)

    def test_fuzz_no_overlap_and_complete_coverage(self) -> None:
        rng = random.Random(42)
        for _ in range(25):
            pages_count = rng.randint(1, 60)
            chunk_size = rng.randint(1, 9)
            with tempfile.TemporaryDirectory() as tmp:
                for _ in range(rng.randint(0, 15)):
                    start = rng.randint(-10, pages_count + 10)
                    end = start + rng.randint(0, 15)
                    open(os.path.join(tmp, f"chunk-{start}-{end}.json"), "w", encoding="utf-8").close()

                planner = ChunkPlanner(tmp, pages_count=pages_count, chunk_sizes=[chunk_size])
                ranges = _collect_ranges(planner)
                for start, end in ranges:
                    self.assertGreaterEqual(start, 0)
                    self.assertLess(end, pages_count)
                    self.assertLessEqual(start, end)
                    planner.mark_success(Chunk(start, end))

                for prev, curr in zip(ranges, ranges[1:]):
                    self.assertGreaterEqual(curr[0], prev[1] + 1)

                complete, missing = planner.verify_complete()
                self.assertTrue(complete)
                self.assertEqual([], missing)


if __name__ == "__main__":
    unittest.main()
