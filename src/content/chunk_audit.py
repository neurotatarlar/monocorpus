"""Audit PDF chunk sets for completeness and optionally reset docs for reprocessing."""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pymupdf

from core.config import read_config
from core.db import get_session
from core.paths import get_in_workdir
from core.yadisk import download_file_locally, obtain_documents
from dirs import Dirs
from integrations.yadisk import YaDisk
from models import Document

from .pdf_utils import has_figure_tag_with_missing_attributes


CHUNK_JSON_RE = re.compile(r"^chunk-(\d+)-(\d+)\.json$")
SIZE_ANOMALY_MIN_VALID_CHUNKS = 4
SIZE_ANOMALY_LARGE_RATIO = 5.0
SIZE_ANOMALY_SMALL_RATIO = 0.2


@dataclass
class ChunkFileStat:
    """Parsed local chunk metadata used for coverage and anomaly checks."""

    filename: str
    start: int
    end: int
    pages: int
    file_bytes: int
    content_chars: int
    file_bytes_per_page: float
    content_chars_per_page: float


@dataclass
class ChunkAuditIssue:
    """Single document chunk-audit result."""

    md5: str
    pages_count: int
    missing_pages: list[int]
    invalid_chunks: list[str]
    size_anomaly_chunks: list[dict[str, Any]]


def _inspect_local_chunks(chunked_results_dir: str) -> tuple[list[ChunkFileStat], list[str]]:
    """Return valid local chunk stats and invalid chunk filenames from a chunk dir."""
    valid_chunks: list[ChunkFileStat] = []
    invalid_chunks: list[str] = []

    if not os.path.isdir(chunked_results_dir):
        return valid_chunks, invalid_chunks

    for filename in sorted(os.listdir(chunked_results_dir)):
        match = CHUNK_JSON_RE.match(filename)
        if not match:
            continue
        path = os.path.join(chunked_results_dir, filename)
        try:
            start = int(match.group(1))
            end = int(match.group(2))
            pages = end - start + 1
            if pages <= 0:
                raise ValueError("invalid chunk page range")
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            content = payload.get("content")
            if not isinstance(content, str):
                raise ValueError("content must be string")
            # Empty string is valid (e.g. blank page chunk).
            if has_figure_tag_with_missing_attributes(content):
                raise ValueError("figure tag missing attributes")
        except Exception:
            invalid_chunks.append(filename)
            continue
        file_bytes = os.path.getsize(path)
        content_chars = len(content)
        valid_chunks.append(
            ChunkFileStat(
                filename=filename,
                start=start,
                end=end,
                pages=pages,
                file_bytes=file_bytes,
                content_chars=content_chars,
                file_bytes_per_page=file_bytes / pages,
                content_chars_per_page=content_chars / pages,
            )
        )

    valid_chunks.sort(key=lambda c: (c.start, c.end))
    return valid_chunks, invalid_chunks


def _load_valid_chunk_ranges(chunked_results_dir: str) -> tuple[list[tuple[int, int]], list[str]]:
    """Return valid chunk ranges and invalid chunk filenames from a local chunk dir."""
    valid_chunks, invalid_chunks = _inspect_local_chunks(chunked_results_dir)
    return [(c.start, c.end) for c in valid_chunks], invalid_chunks


def _missing_pages_from_ranges(pages_count: int, ranges: list[tuple[int, int]]) -> list[int]:
    """Compute uncovered page indexes from chunk ranges (inclusive bounds)."""
    if pages_count <= 0:
        return []
    last_page = pages_count - 1
    covered: set[int] = set()
    for start, end in ranges:
        start = max(0, start)
        end = min(last_page, end)
        if start <= end:
            covered.update(range(start, end + 1))
    return [p for p in range(pages_count) if p not in covered]


def _detect_size_anomalies(
    valid_chunks: list[ChunkFileStat],
    *,
    large_ratio: float = SIZE_ANOMALY_LARGE_RATIO,
    small_ratio: float = SIZE_ANOMALY_SMALL_RATIO,
    min_valid_chunks: int = SIZE_ANOMALY_MIN_VALID_CHUNKS,
) -> list[dict[str, Any]]:
    """Detect per-document chunk size anomalies relative to average bytes-per-page."""
    if len(valid_chunks) < min_valid_chunks:
        return []
    if large_ratio <= 1.0:
        raise ValueError("large_ratio must be > 1.0")
    if not (0.0 < small_ratio < 1.0):
        raise ValueError("small_ratio must be in (0, 1)")

    avg_bytes_per_page = sum(c.file_bytes_per_page for c in valid_chunks) / len(valid_chunks)
    if avg_bytes_per_page <= 0:
        return []

    anomalies: list[dict[str, Any]] = []
    for chunk in valid_chunks:
        # Empty chunk content can be expected for blank pages; don't flag as tiny.
        if chunk.content_chars == 0:
            continue
        ratio = chunk.file_bytes_per_page / avg_bytes_per_page
        reason = None
        if ratio >= large_ratio:
            reason = "too_large_bytes_per_page"
        elif ratio <= small_ratio:
            reason = "too_small_bytes_per_page"
        if not reason:
            continue
        anomalies.append(
            {
                "chunk": chunk.filename,
                "start": chunk.start,
                "end": chunk.end,
                "pages": chunk.pages,
                "file_bytes": chunk.file_bytes,
                "content_chars": chunk.content_chars,
                "file_bytes_per_page": round(chunk.file_bytes_per_page, 2),
                "content_chars_per_page": round(chunk.content_chars_per_page, 2),
                "avg_file_bytes_per_page": round(avg_bytes_per_page, 2),
                "ratio_to_doc_avg": round(ratio, 2),
                "reason": reason,
            }
        )
    return anomalies


def _write_report(report_path: str | None, report: dict[str, Any]) -> str:
    """Write report JSON to the provided path or default logs dir."""
    if not report_path:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = get_in_workdir(Dirs.LOGS, file=f"chunk_audit_report_{stamp}.json")
    else:
        report_path = os.path.abspath(os.path.expanduser(report_path))
        os.makedirs(os.path.dirname(report_path) or ".", exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    return report_path


def run(
    *,
    md5: str | None = None,
    md5s: list[str] | None = None,
    path: str | None = None,
    reset_content_url: bool = False,
    report_path: str | None = None,
    size_anomaly_large_ratio: float = SIZE_ANOMALY_LARGE_RATIO,
    size_anomaly_small_ratio: float = SIZE_ANOMALY_SMALL_RATIO,
    size_anomaly_min_valid_chunks: int = SIZE_ANOMALY_MIN_VALID_CHUNKS,
) -> str:
    """Audit processed PDF docs for incomplete/invalid local chunk sets and optionally reset content_url."""
    config = read_config()
    cli_params = SimpleNamespace(md5=md5, md5s=md5s, path=path)
    predicate = (Document.mime_type == "application/pdf") & Document.content_url.is_not(None)

    summary = {
        "checked_docs": 0,
        "incomplete_docs": 0,
        "size_anomaly_docs": 0,
        "size_anomaly_chunks": 0,
        "reset_docs": 0,
    }
    issues: list[ChunkAuditIssue] = []

    with YaDisk(config["yandex"]["disk"]["oauth_token"], proxy=config.get("proxy")) as ya_client:
        with get_session() as session:
            docs = list(obtain_documents(cli_params, ya_client, entity_cls=Document, predicate=predicate, session=session))
            if not docs:
                report = {"summary": summary, "issues": [], "issue_docs": []}
                saved = _write_report(report_path, report)
                print(f"No matching processed PDF docs found. Report saved to {saved}")
                return saved

            for doc in docs:
                summary["checked_docs"] += 1
                chunked_results_dir = get_in_workdir(Dirs.CHUNKED_RESULTS, doc.md5)
                valid_chunks, invalid_chunks = _inspect_local_chunks(chunked_results_dir)
                valid_ranges = [(c.start, c.end) for c in valid_chunks]

                local_doc_path = download_file_locally(ya_client, doc, config)
                with pymupdf.open(local_doc_path) as pdf_doc:
                    pages_count = pdf_doc.page_count

                missing_pages = _missing_pages_from_ranges(pages_count, valid_ranges)
                size_anomalies = _detect_size_anomalies(
                    valid_chunks,
                    large_ratio=size_anomaly_large_ratio,
                    small_ratio=size_anomaly_small_ratio,
                    min_valid_chunks=size_anomaly_min_valid_chunks,
                )

                if size_anomalies:
                    summary["size_anomaly_docs"] += 1
                    summary["size_anomaly_chunks"] += len(size_anomalies)

                if not missing_pages and not invalid_chunks and not size_anomalies:
                    continue

                issues.append(
                    ChunkAuditIssue(
                        md5=doc.md5,
                        pages_count=pages_count,
                        missing_pages=missing_pages,
                        invalid_chunks=invalid_chunks,
                        size_anomaly_chunks=size_anomalies,
                    )
                )
                if missing_pages or invalid_chunks:
                    summary["incomplete_docs"] += 1

                if reset_content_url and doc.content_url is not None:
                    if missing_pages or invalid_chunks:
                        doc.content_url = None
                        summary["reset_docs"] += 1

            if reset_content_url and summary["reset_docs"]:
                session.commit()

    report = {
        "summary": summary,
        "issue_docs": [issue.md5 for issue in issues],
        "issues": [asdict(issue) for issue in issues],
    }
    saved = _write_report(report_path, report)
    print(
        f"Chunk audit complete. Checked: {summary['checked_docs']}, "
        f"incomplete: {summary['incomplete_docs']}, reset: {summary['reset_docs']}. "
        f"Report saved to {saved}"
    )
    return saved


__all__ = [
    "run",
    "_inspect_local_chunks",
    "_load_valid_chunk_ranges",
    "_missing_pages_from_ranges",
    "_detect_size_anomalies",
]
