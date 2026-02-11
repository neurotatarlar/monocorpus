"""Evaluate document applicability for library management."""

from __future__ import annotations

import datetime
import json
import os
import random
import re
import threading
import time
import zipfile
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Iterable
from urllib.parse import urlparse

from google.genai.errors import ClientError
from pydantic import BaseModel
from rich import print

from integrations.gemini import create_client, gemini_api
from integrations.s3 import create_session
from dirs import Dirs
from .fields import extract_flat_fields
from models import Document, Metadata
from core.paths import get_in_workdir
from core.config import read_config
from core.db import get_session
from core.state import dump_expired_keys, load_expired_keys
from .repository import fetch_docs_for_evaluation, mark_docs_as_non_applicable


MODEL = "gemini-2.5-flash"

LEGAL_DOC_PATTERNS = [
    re.compile(r"^(?=.*common_crawl)(?=.*npa_ta_).*\.pdf$"),
    re.compile(r"^(?=.*pdf законов с pravo\.gov).*\.pdf$"),
]
ARTIFACTS_DIR = "_artifacts"
UNPROCESSABLES_DIR = os.path.join(ARTIFACTS_DIR, "unprocessables")
DEFAULT_EXCERPT_CHARS = 10_000
EXCERPT_PARTS = 3
EXCERPT_SEPARATOR = "\n\n[...]\n\n"
CODE_FENCE_RE = re.compile(r"```.*?```|~~~.*?~~~", flags=re.DOTALL)
BLANK_LINES_RE = re.compile(r"\n{3,}")


class Evaluation(BaseModel):
    """Classification result used to populate boolean `metadata.lib`."""

    applicable: bool = True
    reason: str | None = None

    @classmethod
    def nonapplicable(cls, reason: str) -> "Evaluation":
        return cls(applicable=False, reason=reason)


@dataclass
class EvaluationTask:
    """Document payload needed for library applicability evaluation."""
    md5: str
    ya_path: str | None
    language: str | None
    page_count: int | None
    full: bool | None
    sharing_restricted: bool | None
    content_url: str | None
    schema_org: dict | str | None


def evaluate(args) -> None:
    """Run batch evaluation and save results into `metadata.lib`."""
    config = read_config()
    channel = Channel(dry_run=args.dry_run)
    if args.dry_run:
        print("Running in dry-run mode: no DB/file state changes will be persisted.")

    excerpt_chars = max(0, getattr(args, "excerpt_chars", DEFAULT_EXCERPT_CHARS))

    while True:
        docs = _load_batch(config, args.batch_size, channel)
        if not docs:
            print("No more documents to process")
            break

        docs, non_applicables = _early_skip(docs)
        _save_non_applicable(non_applicables, dry_run=args.dry_run)
        if not docs:
            continue

        keys = _pick_keys(config, args.workers, channel)
        if not keys:
            print("No gemini keys available, exiting...")
            break

        tasks_queue = _create_queue(docs)
        print(f"Processing batch of {tasks_queue.qsize()} documents with {len(keys)} worker(s)")

        workers = []
        for key in keys[: min(len(keys), tasks_queue.qsize())]:
            worker = LibraryApplicabilityWorker(
                gemini_api_key=key,
                tasks_queue=tasks_queue,
                config=config,
                channel=channel,
                dry_run=args.dry_run,
                excerpt_chars=excerpt_chars,
            )
            thread = threading.Thread(target=worker, name=f"eval-{key[-6:]}")
            thread.start()
            workers.append(thread)
            time.sleep(2)

        for thread in workers:
            thread.join()

        channel.dump()


def _load_batch(config: dict, batch_size: int, channel: "Channel") -> list[EvaluationTask]:
    lang_codes = config["sup_langs"]["tt"]["codes"]
    rows = fetch_docs_for_evaluation(
        batch_size=batch_size,
        lang_codes=lang_codes,
        excluded_md5s=channel.get_all_unprocessable_docs(),
    )
    return [
        EvaluationTask(
            md5=doc.md5,
            ya_path=doc.ya_path,
            language=doc.language,
            page_count=extract_flat_fields(meta.schema_org if meta else None).get("page_count"),
            full=doc.full,
            sharing_restricted=doc.sharing_restricted,
            content_url=doc.content_url,
            schema_org=meta.schema_org if meta else None,
        )
        for doc, meta in rows
    ]


def _pick_keys(config: dict, workers: int, channel: "Channel") -> list[str]:
    keys = list(set(config["gemini_api_keys"]) - channel.exceeded_keys_set)
    random.shuffle(keys)
    return keys[:workers]


def _early_skip(docs: Iterable[EvaluationTask]) -> tuple[list[EvaluationTask], list[tuple[str, str]]]:
    probables = []
    non_applicables = []
    for doc in docs:
        if doc.full is not True:
            non_applicables.append((doc.md5, "not full"))
            continue
        if doc.sharing_restricted is True:
            non_applicables.append((doc.md5, "sharing restricted"))
            continue
        if doc.ya_path and any(pattern.match(doc.ya_path) for pattern in LEGAL_DOC_PATTERNS):
            non_applicables.append((doc.md5, "legal doc"))
            continue
        probables.append(doc)
    return probables, non_applicables


def _save_non_applicable(non_applicables: list[tuple[str, str]], dry_run: bool) -> None:
    if not non_applicables:
        return
    print(f"Marking {len(non_applicables)} documents as non-applicable")
    if dry_run:
        return
    mark_docs_as_non_applicable([md5 for md5, _reason in non_applicables])


def _create_queue(docs: list[EvaluationTask]) -> Queue:
    tasks_queue: Queue = Queue()
    for doc in docs:
        tasks_queue.put(doc)
    return tasks_queue


def _build_applicability_prompt(payload: dict) -> list[dict[str, str]]:
    """Build Gemini prompt for strict library-applicability classification."""
    return [
        {
            "text": (
                "You classify if a document should be included in a public library "
                "collection for general readers. Return strict JSON with fields: "
                "applicable(bool), reason(str|null). "
                "Use applicable=false for legal/regulatory/bureaucratic and utility "
                "documents: laws, decrees, orders, resolutions, court acts, statutes, "
                "budgets, reports, procurement docs, forms, blank templates, applications, "
                "notices, instructions, accounting/tax docs, schedules, meeting minutes. "
                "Use applicable=true for reader-oriented books: fiction, poetry, drama, "
                "children's literature, biographies, history, culture, popular science, "
                "dictionaries, encyclopedias, textbooks/manuals meant for broad reading. "
                "If uncertain, prefer applicable=false. Reason must be short (2-8 words)."
            )
        },
        {"text": json.dumps(payload, ensure_ascii=False)},
    ]


class LibraryApplicabilityWorker:
    """Single worker that consumes docs and saves applicability result."""

    def __init__(
        self,
        gemini_api_key: str,
        tasks_queue: Queue,
        config: dict,
        channel: "Channel",
        dry_run: bool,
        excerpt_chars: int = DEFAULT_EXCERPT_CHARS,
    ):
        self.key = gemini_api_key
        self.tasks_queue = tasks_queue
        self.config = config
        self.channel = channel
        self.dry_run = dry_run
        self.excerpt_chars = max(0, excerpt_chars)
        self._s3client = None

    def __call__(self) -> None:
        gemini_client = create_client(self.key)
        prev_req_time: datetime.datetime | None = None
        while True:
            try:
                doc = self.tasks_queue.get(block=False)
            except Empty:
                self.log("No tasks left, shutting down")
                return

            try:
                self.log(f"Evaluating {doc.md5} ({doc.ya_path})")
                prev_req_time = self._sleep_if_needed(prev_req_time)
                evaluation = self._evaluate(doc, gemini_client)
                if not evaluation:
                    self.log(f"Empty model response for {doc.md5}")
                    self.channel.add_unprocessable_doc(doc.md5)
                    continue
                self._save_result(doc.md5, evaluation)
            except ClientError as e:
                self.log(f"ClientError for {doc.md5}: {e}")
                self.channel.add_unprocessable_doc(doc.md5)
                if e.code == 429:
                    self.channel.add_exceeded_key(self.key)
                    self.tasks_queue.put(doc)
                    return
            except Exception as e:  # noqa: BLE001
                import traceback

                self.log(f"Unhandled error for {doc.md5}: {e}\n{traceback.format_exc()}")
                self.channel.add_unprocessable_doc(doc.md5)

    def _sleep_if_needed(self, prev_req_time: datetime.datetime | None) -> datetime.datetime:
        if prev_req_time:
            elapsed = datetime.datetime.now() - prev_req_time
            if elapsed < datetime.timedelta(seconds=2):
                time.sleep(2 - elapsed.total_seconds())
        return datetime.datetime.now()

    def _evaluate(self, doc: EvaluationTask, gemini_client) -> Evaluation | None:
        flattened_meta = extract_flat_fields(doc.schema_org)
        excerpt = self._load_content_excerpt(doc)
        payload = {
            "md5": doc.md5,
            "ya_path": doc.ya_path,
            "title": flattened_meta["title"],
            "author": flattened_meta["author"],
            "publisher": flattened_meta["publisher"],
            "genre": flattened_meta["genre"],
            "language": doc.language,
            "publish_year": flattened_meta["publish_year"],
            "isbn": flattened_meta["isbn"],
            "page_count": doc.page_count,
            "content_excerpt": excerpt,
            "meta": doc.schema_org,
        }

        prompt = _build_applicability_prompt(payload)

        response, _ = gemini_api(
            prompt=prompt,
            model=MODEL,
            client=gemini_client,
            schema=Evaluation,
            timeout_sec=180,
        )
        raw_response = "".join([chunk.text for chunk in response if chunk.text])
        if not raw_response:
            return None
        return Evaluation.model_validate_json(raw_response)

    def _load_content_excerpt(self, doc: EvaluationTask) -> str | None:
        if self.excerpt_chars <= 0 or not doc.content_url:
            return None
        try:
            content_bucket = self.config["yandex"]["cloud"]["bucket"]["content"]
            s3client = self._get_s3client()
            local_zip, _, _ = _ensure_local_zip(doc.md5, doc.content_url, s3client, content_bucket)
            markdown = _read_markdown_from_zip(local_zip, doc.md5)
            return _build_content_excerpt(markdown, self.excerpt_chars)
        except Exception as exc:  # noqa: BLE001
            self.log(f"Could not build excerpt for {doc.md5}: {exc}")
            return None

    def _get_s3client(self):
        if self._s3client is None:
            self._s3client = create_session(self.config)
        return self._s3client

    def _save_result(self, md5: str, evaluation: Evaluation) -> None:
        if self.dry_run:
            self.log(f"Dry-run: would persist evaluation for {md5}")
            return
        with get_session() as session:
            metadata = session.get(Metadata, md5)
            if metadata:
                metadata.lib = bool(evaluation.applicable)
                session.commit()

    def log(self, message: str) -> None:
        print(f"{threading.current_thread().name} {time.strftime('%d-%m-%y %H:%M:%S')} {self.key[-7:]}: {message}")


class Channel:
    """Shared state between workers: exhausted keys and failed docs."""

    def __init__(self, dry_run: bool):
        self.lock = threading.Lock()
        self.dry_run = dry_run
        self.keys_dir = os.path.join(ARTIFACTS_DIR, "expired_keys_eval")
        self.unprocessable_docs = self._load_file(UNPROCESSABLES_DIR, "unprocessables_eval.txt")
        self.repairable_docs = self._load_file(UNPROCESSABLES_DIR, "repairables_eval.txt")
        self.exceeded_keys_set = load_expired_keys(dir=self.keys_dir)

    def get_all_unprocessable_docs(self) -> set[str]:
        return self.unprocessable_docs | self.repairable_docs

    def dump(self) -> None:
        if self.dry_run:
            return
        with self.lock:
            dump_expired_keys(self.exceeded_keys_set, dir=self.keys_dir)
            self._dump_to_file(UNPROCESSABLES_DIR, "unprocessables_eval.txt", self.unprocessable_docs)
            self._dump_to_file(UNPROCESSABLES_DIR, "repairables_eval.txt", self.repairable_docs)

    def _load_file(self, dir_name: str, file_name: str) -> set[str]:
        candidates = [os.path.join(dir_name, file_name)]
        if dir_name.startswith(f"{ARTIFACTS_DIR}/"):
            candidates.append(os.path.join(dir_name.removeprefix(f"{ARTIFACTS_DIR}/"), file_name))

        loaded = set()
        for file_path in candidates:
            if os.path.exists(file_path):
                with open(file_path, "r") as f:
                    loaded.update({line.strip() for line in f.readlines() if line.strip()})
        return loaded

    def _dump_to_file(self, dir_name: str, file_name: str, items: set[str]) -> None:
        os.makedirs(dir_name, exist_ok=True)
        file_path = os.path.join(dir_name, file_name)
        with open(file_path, "w") as f:
            f.write("\n".join(sorted(items)))

    def add_exceeded_key(self, key: str) -> None:
        with self.lock:
            self.exceeded_keys_set.add(key)
            if not self.dry_run:
                dump_expired_keys(self.exceeded_keys_set, dir=self.keys_dir)

    def add_unprocessable_doc(self, md5: str) -> None:
        with self.lock:
            self.unprocessable_docs.add(md5)
            if not self.dry_run:
                self._dump_to_file(UNPROCESSABLES_DIR, "unprocessables_eval.txt", self.unprocessable_docs)

    def add_repairable_doc(self, md5: str) -> None:
        with self.lock:
                self.repairable_docs.add(md5)
                if not self.dry_run:
                    self._dump_to_file(UNPROCESSABLES_DIR, "repairables_eval.txt", self.repairable_docs)


def _ensure_local_zip(md5: str, content_url: str, s3client, fallback_bucket: str) -> tuple[str, str, str]:
    local_zip = get_in_workdir(Dirs.CONTENT, file=f"{md5}.zip")
    bucket, key = _parse_s3_location(content_url, fallback_bucket, f"{md5}.zip")
    if not os.path.exists(local_zip):
        s3client.download_file(bucket, key, local_zip)
    if not os.path.exists(local_zip):
        raise FileNotFoundError(local_zip)
    return local_zip, bucket, key


def _parse_s3_location(content_url: str, fallback_bucket: str, fallback_key: str) -> tuple[str, str]:
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


def _build_content_excerpt(text: str, max_chars: int) -> str | None:
    if max_chars <= 0:
        return None

    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = CODE_FENCE_RE.sub("\n", normalized)
    normalized = BLANK_LINES_RE.sub("\n\n", normalized).strip()
    if not normalized:
        return None
    if len(normalized) <= max_chars:
        return normalized

    chunk = max_chars // EXCERPT_PARTS
    head = normalized[:chunk]
    mid_start = max(0, (len(normalized) // 2) - (chunk // 2))
    middle = normalized[mid_start : mid_start + chunk]
    tail = normalized[-chunk:]
    excerpt = EXCERPT_SEPARATOR.join([head, middle, tail])
    return excerpt[:max_chars]
