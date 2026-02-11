"""Evaluate document applicability for library management."""

from __future__ import annotations

import datetime
import json
import os
import random
import re
import threading
import time
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Iterable

from google.genai.errors import ClientError
from pydantic import BaseModel
from rich import print
from sqlalchemy import select

from gemini import create_client, gemini_api
from meta_fields import extract_flat_fields
from models import Document, Metadata
from utils import dump_expired_keys, get_session, load_expired_keys, read_config


MODEL = "gemini-2.5-flash"

LEGAL_DOC_PATTERNS = [
    re.compile(r"^(?=.*common_crawl)(?=.*npa_ta_).*\.pdf$"),
    re.compile(r"^(?=.*pdf законов с pravo\.gov).*\.pdf$"),
]


class Evaluation(BaseModel):
    """Classification result used to populate boolean `metadata.lib`."""

    applicable: bool = True
    reason: str | None = None
    ddc: str | None = None
    topics: list[str] | None = None

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
    schema_org: dict | str | None


def evaluate(args) -> None:
    """Run batch evaluation and save results into `metadata.lib`."""
    config = read_config()
    channel = Channel(dry_run=args.dry_run)
    if args.dry_run:
        print("Running in dry-run mode: no DB/file state changes will be persisted.")

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
    predicate = _get_predicate(lang_codes, channel.get_all_unprocessable_docs())
    with get_session() as session:
        rows = session.execute(
            select(Document, Metadata)
            .join(Metadata, Metadata.md5 == Document.md5)
            .where(predicate)
            .limit(batch_size)
        )
        return [
            EvaluationTask(
                md5=doc.md5,
                ya_path=doc.ya_path,
                language=doc.language,
                page_count=extract_flat_fields(meta.schema_org if meta else None).get("page_count"),
                full=doc.full,
                sharing_restricted=doc.sharing_restricted,
                schema_org=meta.schema_org if meta else None,
            )
            for doc, meta in rows
        ]


def _get_predicate(codes: list[str], unprocessable: set[str]):
    predicate = (
        Metadata.lib.is_(None)
        & Document.language.in_(codes)
        & Document.content_url.is_not(None)
    )
    if unprocessable:
        predicate = predicate & Document.md5.not_in(unprocessable)
    return predicate


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
    with get_session() as session:
        for md5, _reason in non_applicables:
            stored = session.get(Metadata, md5)
            if stored:
                stored.lib = False
        session.commit()


def _create_queue(docs: list[EvaluationTask]) -> Queue:
    tasks_queue: Queue = Queue()
    for doc in docs:
        tasks_queue.put(doc)
    return tasks_queue


class LibraryApplicabilityWorker:
    """Single worker that consumes docs and saves applicability result."""

    def __init__(self, gemini_api_key: str, tasks_queue: Queue, config: dict, channel: "Channel", dry_run: bool):
        self.key = gemini_api_key
        self.tasks_queue = tasks_queue
        self.config = config
        self.channel = channel
        self.dry_run = dry_run

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
            "meta": doc.schema_org,
        }

        prompt = [
            {
                "text": (
                    "You classify if a document should be included in a public library collection. "
                    "Return strict JSON with fields: applicable(bool), reason(str|null), "
                    "ddc(str|null), topics(array[str]|null). "
                    "Mark non-applicable for legal acts, forms, blank templates, notices, "
                    "or non-book utility documents."
                )
            },
            {"text": json.dumps(payload, ensure_ascii=False)},
        ]

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
        self.keys_dir = "expired_keys_eval"
        self.unprocessable_docs = self._load_file("unprocessables", "unprocessables_eval.txt")
        self.repairable_docs = self._load_file("unprocessables", "repairables_eval.txt")
        self.exceeded_keys_set = load_expired_keys(dir=self.keys_dir)

    def get_all_unprocessable_docs(self) -> set[str]:
        return self.unprocessable_docs | self.repairable_docs

    def dump(self) -> None:
        if self.dry_run:
            return
        with self.lock:
            dump_expired_keys(self.exceeded_keys_set, dir=self.keys_dir)
            self._dump_to_file("unprocessables", "unprocessables_eval.txt", self.unprocessable_docs)
            self._dump_to_file("unprocessables", "repairables_eval.txt", self.repairable_docs)

    def _load_file(self, dir_name: str, file_name: str) -> set[str]:
        file_path = os.path.join(dir_name, file_name)
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                return {line.strip() for line in f.readlines() if line.strip()}
        return set()

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
                self._dump_to_file("unprocessables", "unprocessables_eval.txt", self.unprocessable_docs)

    def add_repairable_doc(self, md5: str) -> None:
        with self.lock:
            self.repairable_docs.add(md5)
            if not self.dry_run:
                self._dump_to_file("unprocessables", "repairables_eval.txt", self.repairable_docs)
