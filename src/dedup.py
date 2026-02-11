"""Report-only near-full duplicate detection for extracted content archives."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
import hashlib
import json
import os
import re
import zipfile
from collections import defaultdict
from urllib.parse import urlparse

from rich import print
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from sqlalchemy import select, func

from dirs import Dirs
from metadata.fields import (
    extract_author,
    extract_isbn,
    extract_publish_year,
    extract_title,
    parse_meta,
)
from models import Document
from integrations.s3 import create_session
from core.paths import get_in_workdir
from core.db import get_session
from core.config import read_config

MARKDOWN_FENCE_RE = re.compile(r"```.*?```|~~~.*?~~~", flags=re.DOTALL)
INLINE_CODE_RE = re.compile(r"`[^`]*`")
HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+", flags=re.MULTILINE)
FOOTNOTE_REF_RE = re.compile(r"\[\^[^\]]+\]")
WHITESPACE_RE = re.compile(r"\s+")
PARAGRAPH_SPLIT_RE = re.compile(r"\n\s*\n")
NON_ALNUM_KEY_RE = re.compile(r"[^\w]+", flags=re.UNICODE)
YEAR_RE = re.compile(r"(1[5-9]\d{2}|20\d{2})")

FORMAT_PRIORITY = {"epub": 0, "fb2": 1, "docx": 2, "pdf": 3}


@dataclass
class DocMeta:
    md5: str
    content_url: str
    mime_type: Optional[str]
    ya_path: Optional[str]
    title: Optional[str]
    author: Optional[str]
    isbn: Optional[str]
    publish_year: Optional[int | str]


@dataclass
class Fingerprint:
    text_hash: str
    char_count: int
    paragraph_hashes: Set[str]


@dataclass
class DuplicateScore:
    exact_hash: bool
    containment: float
    jaccard: float
    length_ratio: float


@dataclass
class DedupStats:
    docs_total: int = 0
    candidate_docs: int = 0
    loaded_docs: int = 0
    download_errors: int = 0
    read_errors: int = 0
    groups_total: int = 0
    groups_skipped_large: int = 0
    pairs_checked: int = 0
    duplicate_pairs: int = 0
    duplicate_groups: int = 0
    failed: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))

    def add_error(self, stage: str, md5: str) -> None:
        self.failed[stage].append(md5)


class UnionFind:
    def __init__(self) -> None:
        self.parent: Dict[str, str] = {}
        self.rank: Dict[str, int] = {}

    def add(self, x: str) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x: str) -> str:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
            return
        if self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
            return
        self.parent[rb] = ra
        self.rank[ra] += 1


def run(
    threshold: float = 0.98,
    force_download: bool = False,
    max_group_size: int = 80,
    report_path: Optional[str] = None,
) -> None:
    """Detect near-full duplicate documents and write a JSON report."""
    if threshold <= 0 or threshold > 1:
        raise ValueError("threshold must be in (0, 1]")
    if max_group_size < 2:
        raise ValueError("max_group_size must be >= 2")

    config = read_config()
    s3client = create_session(config)
    content_bucket = config["yandex"]["cloud"]["bucket"]["content"]
    stats = DedupStats()

    if not report_path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = get_in_workdir(Dirs.LOGS, file=f"dedup_report_{timestamp}.json")

    docs_by_md5: Dict[str, DocMeta] = {}
    key_to_docs: Dict[str, Set[str]] = defaultdict(set)

    with get_session() as session:
        stats.docs_total = session.scalar(
            select(func.count()).select_from(Document).where(Document.content_url.is_not(None))
        ) or 0

        if stats.docs_total == 0:
            _write_report(report_path, threshold, max_group_size, stats, [], [])
            print("No documents with content_url to process.")
            print(f"Report saved to {report_path}")
            return

        stmt = (
            select(Document)
            .where(Document.content_url.is_not(None))
            .order_by(Document.md5.asc())
            .execution_options(stream_results=True)
        )
        docs = session.scalars(stmt).yield_per(200)

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
        ) as progress:
            task_id = progress.add_task("Indexing docs", total=stats.docs_total)
            for doc in docs:
                schema_org = None
                if getattr(doc, "metadata_row", None):
                    schema_org = doc.metadata_row.schema_org
                elif hasattr(doc, "meta"):
                    schema_org = getattr(doc, "meta")
                doc_meta = parse_meta(schema_org)
                meta = DocMeta(
                    md5=doc.md5,
                    content_url=doc.content_url,
                    mime_type=doc.mime_type,
                    ya_path=doc.ya_path,
                    title=extract_title(doc_meta) or getattr(doc, "title", None),
                    author=extract_author(doc_meta) or getattr(doc, "author", None),
                    isbn=extract_isbn(doc_meta) or getattr(doc, "isbn", None),
                    publish_year=extract_publish_year(doc_meta) or getattr(doc, "publish_year", None),
                )
                docs_by_md5[meta.md5] = meta
                for key in _candidate_keys(meta):
                    key_to_docs[key].add(meta.md5)
                progress.advance(task_id, 1)

    candidate_keys = {k: sorted(v) for k, v in key_to_docs.items() if len(v) >= 2}
    stats.groups_total = len(candidate_keys)
    candidate_md5s = sorted({md5 for ids in candidate_keys.values() for md5 in ids})
    stats.candidate_docs = len(candidate_md5s)

    fingerprints: Dict[str, Fingerprint] = {}
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
    ) as progress:
        task_id = progress.add_task("Loading candidate content", total=len(candidate_md5s))
        for md5 in candidate_md5s:
            meta = docs_by_md5[md5]
            try:
                local_zip, _, _ = _ensure_local_zip(
                    meta.md5,
                    meta.content_url,
                    s3client,
                    content_bucket,
                    force_download=force_download,
                )
                content = _read_markdown_from_zip(local_zip, md5)
                fingerprints[md5] = _build_fingerprint(content)
                stats.loaded_docs += 1
            except FileNotFoundError:
                stats.download_errors += 1
                stats.add_error("download", md5)
            except Exception:
                stats.read_errors += 1
                stats.add_error("read", md5)
            progress.advance(task_id, 1)

    seen_pairs: Set[Tuple[str, str]] = set()
    pair_results: List[Dict[str, object]] = []
    edges: List[Tuple[str, str]] = []

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
    ) as progress:
        task_id = progress.add_task("Comparing candidates", total=len(candidate_keys))
        for key, md5s in candidate_keys.items():
            if len(md5s) > max_group_size:
                stats.groups_skipped_large += 1
                progress.advance(task_id, 1)
                continue
            present = [m for m in md5s if m in fingerprints]
            for i in range(len(present)):
                for j in range(i + 1, len(present)):
                    a, b = present[i], present[j]
                    pair = (a, b) if a < b else (b, a)
                    if pair in seen_pairs:
                        continue
                    seen_pairs.add(pair)
                    stats.pairs_checked += 1
                    score = _compare_fingerprints(fingerprints[a], fingerprints[b])
                    if _is_duplicate(score, threshold):
                        stats.duplicate_pairs += 1
                        edges.append(pair)
                        pair_results.append(
                            {
                                "a_md5": pair[0],
                                "b_md5": pair[1],
                                "exact_hash": score.exact_hash,
                                "containment": round(score.containment, 6),
                                "jaccard": round(score.jaccard, 6),
                                "length_ratio": round(score.length_ratio, 6),
                                "source_key": key,
                            }
                        )
            progress.advance(task_id, 1)

    groups = _build_duplicate_groups(edges, docs_by_md5, fingerprints)
    stats.duplicate_groups = len(groups)

    _write_report(report_path, threshold, max_group_size, stats, groups, pair_results)
    _print_summary(report_path, stats)


def _candidate_keys(meta: DocMeta) -> Set[str]:
    keys: Set[str] = set()
    isbn = _normalize_isbn(meta.isbn)
    if isbn:
        keys.add(f"isbn:{isbn}")

    title = _normalize_key(meta.title)
    author = _normalize_key(meta.author)
    year = _extract_year(meta.publish_year)

    if title and author and len(title) >= 8:
        keys.add(f"title_author:{title}|{author}")
    if title and year:
        keys.add(f"title_year:{title}|{year}")
    if title and len(title) >= 18:
        keys.add(f"title:{title}")
    return keys


def _normalize_key(value: Optional[str]) -> str:
    if not value:
        return ""
    value = NON_ALNUM_KEY_RE.sub(" ", value.lower())
    return WHITESPACE_RE.sub(" ", value).strip()


def _normalize_isbn(value: Optional[str]) -> str:
    if not value:
        return ""
    parts = re.split(r"[,\s;]+", value)
    for part in parts:
        cleaned = re.sub(r"[^0-9Xx]", "", part).upper()
        if len(cleaned) in (10, 13):
            return cleaned
    cleaned = re.sub(r"[^0-9Xx]", "", value).upper()
    if len(cleaned) in (10, 13):
        return cleaned
    return ""


def _extract_year(value: Optional[int | str]) -> str:
    if not value:
        return ""
    if isinstance(value, int):
        return str(value)
    match = YEAR_RE.search(value)
    return match.group(1) if match else ""


def _build_fingerprint(markdown: str) -> Fingerprint:
    normalized_text = _normalize_text(markdown)
    text_hash = hashlib.sha1(normalized_text.encode("utf-8")).hexdigest()
    paragraphs = []
    for paragraph in PARAGRAPH_SPLIT_RE.split(markdown):
        p = _normalize_text(paragraph)
        if len(p) >= 80:
            paragraphs.append(p)
    if not paragraphs and normalized_text:
        paragraphs = [normalized_text]
    paragraph_hashes = {
        hashlib.sha1(p.encode("utf-8")).hexdigest()
        for p in paragraphs
    }
    return Fingerprint(
        text_hash=text_hash,
        char_count=len(normalized_text),
        paragraph_hashes=paragraph_hashes,
    )


def _normalize_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = MARKDOWN_FENCE_RE.sub(" ", text)
    text = INLINE_CODE_RE.sub(" ", text)
    text = HEADING_RE.sub("", text)
    text = FOOTNOTE_REF_RE.sub(" ", text)
    text = text.lower()
    text = WHITESPACE_RE.sub(" ", text)
    return text.strip()


def _compare_fingerprints(a: Fingerprint, b: Fingerprint) -> DuplicateScore:
    if a.char_count == 0 or b.char_count == 0:
        return DuplicateScore(exact_hash=False, containment=0.0, jaccard=0.0, length_ratio=0.0)

    exact_hash = a.text_hash == b.text_hash
    shared = len(a.paragraph_hashes & b.paragraph_hashes)
    min_size = min(len(a.paragraph_hashes), len(b.paragraph_hashes))
    max_chars = max(a.char_count, b.char_count)
    union_size = len(a.paragraph_hashes | b.paragraph_hashes)
    containment = (shared / min_size) if min_size else 0.0
    jaccard = (shared / union_size) if union_size else 0.0
    length_ratio = (min(a.char_count, b.char_count) / max_chars) if max_chars else 0.0
    return DuplicateScore(
        exact_hash=exact_hash,
        containment=containment,
        jaccard=jaccard,
        length_ratio=length_ratio,
    )


def _is_duplicate(score: DuplicateScore, threshold: float) -> bool:
    if score.exact_hash:
        return True
    return score.containment >= threshold and score.length_ratio >= 0.9


def _build_duplicate_groups(
    edges: List[Tuple[str, str]],
    docs_by_md5: Dict[str, DocMeta],
    fingerprints: Dict[str, Fingerprint],
) -> List[Dict[str, object]]:
    if not edges:
        return []

    uf = UnionFind()
    for a, b in edges:
        uf.add(a)
        uf.add(b)
        uf.union(a, b)

    components: Dict[str, List[str]] = defaultdict(list)
    for md5 in uf.parent:
        components[uf.find(md5)].append(md5)

    groups = []
    for members in components.values():
        members_sorted = sorted(set(members))
        keeper = _pick_keeper(members_sorted, docs_by_md5, fingerprints)
        duplicate_md5s = [m for m in members_sorted if m != keeper]
        groups.append(
            {
                "keeper_md5": keeper,
                "duplicate_md5s": duplicate_md5s,
                "members": [
                    {
                        "md5": m,
                        "format": _detect_format(docs_by_md5[m]),
                        "mime_type": docs_by_md5[m].mime_type,
                        "char_count": fingerprints.get(m).char_count if m in fingerprints else 0,
                        "title": docs_by_md5[m].title,
                        "author": docs_by_md5[m].author,
                        "isbn": docs_by_md5[m].isbn,
                        "publish_year": docs_by_md5[m].publish_year,
                    }
                    for m in members_sorted
                ],
            }
        )

    groups.sort(key=lambda g: (len(g["duplicate_md5s"]), g["keeper_md5"]), reverse=True)
    return groups


def _pick_keeper(
    members: List[str],
    docs_by_md5: Dict[str, DocMeta],
    fingerprints: Dict[str, Fingerprint],
) -> str:
    def sort_key(md5: str) -> Tuple[int, int, str]:
        meta = docs_by_md5[md5]
        fmt = _detect_format(meta)
        priority = FORMAT_PRIORITY.get(fmt, 99)
        char_count = fingerprints.get(md5).char_count if md5 in fingerprints else 0
        return (priority, -char_count, md5)

    return sorted(members, key=sort_key)[0]


def _detect_format(meta: DocMeta) -> str:
    mime = (meta.mime_type or "").lower()
    path = (meta.ya_path or "").lower()
    if "epub" in mime or path.endswith(".epub"):
        return "epub"
    if "fb2" in mime or path.endswith(".fb2") or path.endswith(".fb2.zip"):
        return "fb2"
    if "wordprocessingml.document" in mime or path.endswith(".docx"):
        return "docx"
    if mime == "application/pdf" or path.endswith(".pdf"):
        return "pdf"
    return "other"


def _ensure_local_zip(
    md5: str,
    content_url: str,
    s3client,
    fallback_bucket: str,
    force_download: bool,
) -> Tuple[str, str, str]:
    local_zip = get_in_workdir(Dirs.CONTENT, file=f"{md5}.zip")
    bucket, key = _parse_s3_location(content_url, fallback_bucket, f"{md5}.zip")
    if force_download or not os.path.exists(local_zip):
        s3client.download_file(bucket, key, local_zip)
    if not os.path.exists(local_zip):
        raise FileNotFoundError(local_zip)
    return local_zip, bucket, key


def _parse_s3_location(content_url: str, fallback_bucket: str, fallback_key: str) -> Tuple[str, str]:
    if content_url:
        try:
            parsed = urlparse(content_url)
            if parsed.scheme and parsed.netloc:
                path = parsed.path.lstrip("/")
                if path:
                    parts = path.split("/", 1)
                    bucket = parts[0]
                    key = parts[1] if len(parts) > 1 and parts[1] else fallback_key
                    return bucket, key
        except Exception:
            pass
    return fallback_bucket, fallback_key


def _read_markdown_from_zip(zip_path: str, md5: str) -> str:
    with zipfile.ZipFile(zip_path, "r") as zf:
        md_name = f"{md5}.md"
        names = zf.namelist()
        if md_name not in names:
            md_candidates = [n for n in names if n.lower().endswith(".md")]
            if not md_candidates:
                raise ValueError("No markdown file found in archive")
            md_name = md_candidates[0]
        return zf.read(md_name).decode("utf-8", errors="replace")


def _write_report(
    path: str,
    threshold: float,
    max_group_size: int,
    stats: DedupStats,
    groups: List[Dict[str, object]],
    pairs: List[Dict[str, object]],
) -> None:
    report_dir = os.path.dirname(path)
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)

    duplicate_docs = sorted(
        {
            md5
            for group in groups
            for md5 in group.get("duplicate_md5s", [])
        }
    )

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "settings": {
            "near_full_threshold": threshold,
            "max_group_size": max_group_size,
            "format_priority": ["epub", "fb2", "docx", "pdf"],
        },
        "summary": {
            "docs_total": stats.docs_total,
            "candidate_docs": stats.candidate_docs,
            "loaded_docs": stats.loaded_docs,
            "download_errors": stats.download_errors,
            "read_errors": stats.read_errors,
            "groups_total": stats.groups_total,
            "groups_skipped_large": stats.groups_skipped_large,
            "pairs_checked": stats.pairs_checked,
            "duplicate_pairs": stats.duplicate_pairs,
            "duplicate_groups": stats.duplicate_groups,
            "duplicate_docs": len(duplicate_docs),
        },
        "failed": stats.failed,
        "duplicate_docs": duplicate_docs,
        "duplicate_groups": groups,
        "duplicate_pairs": pairs,
    }
    with open(path, "w") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)


def _print_summary(report_path: str, stats: DedupStats) -> None:
    print("Dedup scan complete")
    print(f"Docs total: {stats.docs_total}")
    print(f"Candidate docs: {stats.candidate_docs}")
    print(f"Loaded docs: {stats.loaded_docs}")
    print(f"Groups total: {stats.groups_total}")
    print(f"Groups skipped (large): {stats.groups_skipped_large}")
    print(f"Pairs checked: {stats.pairs_checked}")
    print(f"Duplicate pairs: {stats.duplicate_pairs}")
    print(f"Duplicate groups: {stats.duplicate_groups}")
    print(f"Download errors: {stats.download_errors}")
    print(f"Read errors: {stats.read_errors}")
    print(f"Report saved to {report_path}")
