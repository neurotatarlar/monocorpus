"""Chunk planning primitives for paginated extraction flows."""

from __future__ import annotations

import os
import re


class Chunk:
    """Simple inclusive page range container (start, end)."""

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __lt__(self, other):
        return (self.start, self.end) < (other.start, other.end)

    def __le__(self, other):
        return (self.start, self.end) <= (other.start, other.end)

    def __gt__(self, other):
        return (self.start, self.end) > (other.start, other.end)

    def __ge__(self, other):
        return (self.start, self.end) >= (other.start, other.end)

    def __eq__(self, other):
        return (self.start, self.end) == (other.start, other.end)

    def __repr__(self):
        return f"Chunk({self.start}, {self.end})"


class ChunkPlanner:
    """Plan page ranges, reusing already processed chunks from disk."""

    def __init__(self, chunked_results_dir, pages_count, chunk_sizes=[5, 3, 2, 1]):
        self.chunked_results_dir = chunked_results_dir
        self.pages_count = pages_count
        self.chunk_sizes = chunk_sizes
        self.current_chunk_size_index = 0
        self.processed_ranges = self._load_processed_ranges()

        # iteration state
        self.cursor_page = 0
        self.idx_processed = 0
        self.last_attempted_chunk = None
        self.retry_mode = False

    def _load_processed_ranges(self):
        """Load already processed chunk ranges from the directory."""
        slice_pattern = re.compile(r"chunk-(\d+)-(\d+)\.json$")
        processed = []
        seen = set()
        for filename in os.listdir(self.chunked_results_dir):
            m = slice_pattern.match(filename)
            if m:
                start, end = map(int, m.groups())
                if (start, end) not in seen:
                    processed.append(Chunk(start, end))
                    seen.add((start, end))
        processed.sort()
        return processed

    def next(self):
        """Return the next chunk to process: either a processed one, or a gap."""
        if self.retry_mode and self.last_attempted_chunk:
            size = self.chunk_sizes[self.current_chunk_size_index]
            end_page = min(self.last_attempted_chunk.start + size - 1, self.pages_count)
            chunk = Chunk(self.last_attempted_chunk.start, end_page)
            self.last_attempted_chunk = chunk
            self.retry_mode = False
            return chunk

        while self.cursor_page <= self.pages_count:
            if self.idx_processed < len(self.processed_ranges):
                next_chunk = self.processed_ranges[self.idx_processed]
                if self.cursor_page < next_chunk.start:
                    size = self.chunk_sizes[self.current_chunk_size_index]
                    end_page = min(self.cursor_page + size - 1, self.pages_count)
                    chunk = Chunk(self.cursor_page, end_page)
                    self.last_attempted_chunk = chunk
                    self.cursor_page = end_page + 1
                    return chunk
                else:
                    self.cursor_page = next_chunk.end + 1
                    self.idx_processed += 1
                    return next_chunk
            else:
                if self.cursor_page <= self.pages_count:
                    size = self.chunk_sizes[self.current_chunk_size_index]
                    end_page = min(self.cursor_page + size - 1, self.pages_count)
                    chunk = Chunk(self.cursor_page, end_page)
                    self.last_attempted_chunk = chunk
                    self.cursor_page = end_page + 1
                    return chunk
        return None

    def decrease_chunk_size(self):
        if self.current_chunk_size_index < len(self.chunk_sizes) - 1:
            self.current_chunk_size_index += 1
            self.retry_mode = True
            return True
        return False

    def mark_success(self, chunk):
        """Record a successfully processed chunk."""
        if chunk not in self.processed_ranges:
            self.processed_ranges.append(chunk)
            self.processed_ranges.sort()

    def verify_complete(self):
        """Check if all pages from 0 to pages_count are covered without gaps."""
        covered = set()
        for chunk in self.processed_ranges:
            covered.update(range(chunk.start, chunk.end + 1))
        missing = [p for p in range(0, self.pages_count + 1) if p not in covered]
        return (len(missing) == 0, missing)
