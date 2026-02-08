"""Postpostprocess extracted markdown archives and upload updates to S3."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json
import os
import re
import shutil
import tempfile
import zipfile
from urllib.parse import urlparse

import mdformat
from rich import print
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from sqlalchemy import select, func

from dirs import Dirs
from models import Document
from s3 import create_session, upload_file
from utils import get_in_workdir, get_session, read_config


TOC_MARKER_RE = re.compile(r"<!--\s*mdformat-toc start --no-anchors\s*-->")


@dataclass
class PpsStats:
    processed: int = 0
    changed: int = 0
    unchanged: int = 0
    downloaded: int = 0
    backup_created: int = 0
    read_errors: int = 0
    download_errors: int = 0
    upload_errors: int = 0
    write_errors: int = 0
    issue_counts: Dict[str, int] = field(default_factory=dict)
    errors: List[Dict[str, str]] = field(default_factory=list)

    def add_issue(self, name: str, count: int = 1) -> None:
        self.issue_counts[name] = self.issue_counts.get(name, 0) + count

    def add_error(self, md5: str, stage: str, error: str) -> None:
        self.errors.append({"md5": md5, "stage": stage, "error": error})


def run(force_download: bool = False, report_path: Optional[str] = None) -> None:
    """Run postpostprocessing for all documents with extracted content."""
    config = read_config()
    s3client = create_session(config)
    stats = PpsStats()
    content_bucket = config["yandex"]["cloud"]["bucket"]["content"]

    if not report_path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = get_in_workdir(Dirs.LOGS, file=f"pps_report_{timestamp}.json")

    with get_session() as session:
        total = session.scalar(
            select(func.count())
            .select_from(Document)
            .where(Document.content_url.is_not(None))
            .where(Document.mime_type == "application/pdf")
        )

        if not total:
            print("No documents with content_url to process.")
            _write_report(report_path, stats)
            print(f"Report saved to {report_path}")
            return

        stmt = (
            select(Document)
            .where(Document.content_url.is_not(None))
            .where(Document.mime_type == "application/pdf")
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
            task_id = progress.add_task("Postpostprocessing docs", total=total)
            for doc in docs:
                _process_doc(doc, config, s3client, content_bucket, force_download, stats)
                progress.advance(task_id, 1)

    _write_report(report_path, stats)
    _print_summary(report_path, stats)


def _process_doc(doc, config, s3client, content_bucket, force_download: bool, stats: PpsStats) -> None:
    stats.processed += 1
    md5 = doc.md5

    try:
        local_zip, bucket, key = _ensure_local_zip(
            doc, config, s3client, content_bucket, force_download, stats
        )
    except Exception as e:
        stats.download_errors += 1
        stats.add_error(md5, "download", str(e))
        return

    try:
        content, md_name = _read_markdown_from_zip(local_zip, md5)
    except Exception as e:
        stats.read_errors += 1
        stats.add_error(md5, "read", str(e))
        return

    new_content, issues = _apply_rules(content)
    if not issues and new_content == content:
        stats.unchanged += 1
        return
    formatted = _format_markdown(new_content)
    if formatted != new_content:
        issues["mdformat_applied"] = 1
    new_content = formatted
    
    backup_path = _backup_path(local_zip)
    try:
        if not os.path.exists(backup_path):
            shutil.copy2(local_zip, backup_path)
            stats.backup_created += 1
    except Exception as e:
        stats.write_errors += 1
        stats.add_error(md5, "backup", str(e))
        return

    try:
        _write_zip_with_updated_md(local_zip, md_name, new_content)
    except Exception as e:
        stats.write_errors += 1
        stats.add_error(md5, "write", str(e))
        return

    try:
        upload_file(local_zip, bucket, key, s3client, skip_if_exists=False)
    except Exception as e:
        stats.upload_errors += 1
        stats.add_error(md5, "upload", str(e))
        return

    stats.changed += 1
    for name, count in issues.items():
        stats.add_issue(name, count)
        stats.add_issue(f"{name}_docs", 1)


def _apply_rules(text: str) -> Tuple[str, Dict[str, int]]:
    issues: Dict[str, int] = {}

    updated, removed = _remove_duplicate_toc_markers(text)
    if removed:
        issues["duplicate_toc_markers_removed"] = removed

    return updated, issues


def _remove_duplicate_toc_markers(text: str) -> Tuple[str, int]:
    count = 0

    def _repl(match: re.Match) -> str:
        nonlocal count
        count += 1
        return match.group(0) if count == 1 else ""

    updated = TOC_MARKER_RE.sub(_repl, text)
    removed = max(0, count - 1)
    return updated, removed


def _format_markdown(text: str) -> str:
    formatted = mdformat.text(
        text,
        codeformatters=(),
        extensions=["toc", "footnote"],
        options={"wrap": "keep", "number": "keep", "validate": True, "end_of_line": "lf"},
    )
    # Keep behavior aligned with pdf_postprocess.
    return formatted.replace("\\\\", "\\").replace("\\_", "_").replace("\\<", "<")


def _read_markdown_from_zip(zip_path: str, md5: str) -> Tuple[str, str]:
    with zipfile.ZipFile(zip_path, "r") as zf:
        md_name = f"{md5}.md"
        names = zf.namelist()
        if md_name not in names:
            md_candidates = [n for n in names if n.lower().endswith(".md")]
            if not md_candidates:
                raise ValueError("No markdown file found in archive")
            md_name = md_candidates[0]
        content = zf.read(md_name).decode("utf-8", errors="replace")
        return content, md_name


def _write_zip_with_updated_md(zip_path: str, md_name: str, content: str) -> None:
    tmp_dir = os.path.dirname(zip_path)
    with tempfile.NamedTemporaryFile(dir=tmp_dir, delete=False, suffix=".zip") as tmp:
        tmp_path = tmp.name

    try:
        with zipfile.ZipFile(zip_path, "r") as src, zipfile.ZipFile(
            tmp_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as dst:
            for item in src.infolist():
                if item.filename == md_name:
                    continue
                dst.writestr(item, src.read(item.filename))
            dst.writestr(md_name, content)
        os.replace(tmp_path, zip_path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def _ensure_local_zip(doc, config, s3client, content_bucket: str, force_download: bool, stats: PpsStats):
    local_zip = get_in_workdir(Dirs.CONTENT, file=f"{doc.md5}.zip")
    if force_download or not os.path.exists(local_zip):
        bucket, key = _parse_s3_location(doc.content_url, content_bucket, f"{doc.md5}.zip")
        s3client.download_file(bucket, key, local_zip)
        stats.downloaded += 1
        return local_zip, bucket, key

    bucket, key = _parse_s3_location(doc.content_url, content_bucket, f"{doc.md5}.zip")
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


def _backup_path(zip_path: str) -> str:
    dir_name = os.path.dirname(zip_path)
    base_name = os.path.basename(zip_path)
    return os.path.join(dir_name, f"_backup{base_name}")


def _write_report(path: str, stats: PpsStats) -> None:
    report_dir = os.path.dirname(path)
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
    failed_by_reason: Dict[str, List[str]] = {}
    for err in stats.errors:
        reason = err["stage"]
        if reason not in failed_by_reason:
            failed_by_reason[reason] = []
        failed_by_reason[reason].append(err["md5"])
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "summary": {
            "processed": stats.processed,
            "changed": stats.changed,
            "unchanged": stats.unchanged,
            "downloaded": stats.downloaded,
            "backup_created": stats.backup_created,
            "read_errors": stats.read_errors,
            "download_errors": stats.download_errors,
            "upload_errors": stats.upload_errors,
            "write_errors": stats.write_errors,
        },
        "issues": stats.issue_counts,
        "failed": failed_by_reason,
    }
    with open(path, "w") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)


def _print_summary(report_path: str, stats: PpsStats) -> None:
    print("Postpostprocess complete")
    print(f"Processed: {stats.processed}")
    print(f"Changed: {stats.changed}")
    print(f"Unchanged: {stats.unchanged}")
    print(f"Downloaded: {stats.downloaded}")
    print(f"Backups created: {stats.backup_created}")
    print(f"Read errors: {stats.read_errors}")
    print(f"Download errors: {stats.download_errors}")
    print(f"Upload errors: {stats.upload_errors}")
    print(f"Write errors: {stats.write_errors}")
    print(f"Report saved to {report_path}")
