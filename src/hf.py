"""
Hugging Face Dataset Assembly Module

This module handles the creation of a structured dataset from content files stored in Yandex Cloud,
preparing them for use with Hugging Face's datasets library. 

The module processes Tatar language documents by:
1. Downloading content files from Yandex Cloud storage
2. Matching documents with metadata from the database
3. Extracting text content from zip archives
4. Assembling a structured dataset with metadata
5. Exporting the final dataset to parquet shards

The resulting dataset includes document ID (MD5 hash), publication year, genre, and full text content.
"""

import os
import zipfile
from typing import Dict, Iterable, List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from rich import print
from rich.progress import track
from sqlalchemy import select

from dirs import Dirs
from models import Document
from s3 import download
from utils import get_in_workdir, get_session, read_config


def assemble_dataset():
    """
    Assemble a structured dataset from content files.
    
    This function:
    1. Retrieves document metadata from the database
    2. Downloads and processes content files from Yandex Cloud
    3. Extracts text content from zip archives
    4. Creates a structured dataset with metadata
    5. Exports the dataset to parquet format
    
    The function tracks:
    - Empty documents (skipped)
    - Documents missing from the database
    - Processing progress
    
    Returns:
        None. Outputs parquet files with the assembled dataset.
    """
    print("Assembling structured dataset from content files (streaming mode)...")
    config = read_config()
    content_dir = get_in_workdir(Dirs.CONTENT)
    output_dir = get_in_workdir(Dirs.PARQUET)

    with get_session() as session:
        docs = {doc.md5: doc for doc in session.scalars(select(Document).where(Document.content_url.is_not(None)).order_by(Document.ya_path)).all()}

    empty_docs = set()
    not_in_gsheets = set()

    def _iter_rows() -> Iterable[Dict]:
        for content_file in track(
            download(
                bucket=config["yandex"]["cloud"]["bucket"]["content"],
                download_dir=content_dir,
            ),
            description="Processing documents",
        ):
            md5, _ = os.path.splitext(os.path.basename(content_file))
            if not (doc := docs.get(md5)):
                print(f"No matching document with md5 {md5}, skipping it...")
                not_in_gsheets.add(md5)
                continue

            with zipfile.ZipFile(content_file, "r") as zf:
                md_files = list(zf.namelist())
                if len(md_files) != 1:
                    raise ValueError(
                        f"Expected exactly one markdown file in the zip, found {len(md_files)}"
                    )
                content = zf.read(md_files[0])

            if not content:
                empty_docs.add(doc.md5)
                print(f"Content is empty for document {doc.md5}, skipping it...")
                continue

            yield {
                "id": md5,
                "publish_year": int(doc.publish_date) if doc.publish_date else None,
                "genre": doc.genre,
                "text": content.decode("utf-8"),
            }

    schema = pa.schema(
        [
            pa.field("id", pa.string()),
            pa.field("publish_year", pa.uint16(), nullable=True),
            pa.field("genre", pa.string(), nullable=True),
            pa.field("text", pa.string()),
        ]
    )

    total_rows, total_files = _write_parquet_shards(
        rows=_iter_rows(),
        output_dir=output_dir,
        schema=schema,
        max_rows_per_file=20_000,
        target_file_size_bytes=3 * 256 * 1024 * 1024,
        max_rows_per_row_group=256,
    )

    print(f"Final dataset size: {total_rows} documents across {total_files} parquet files")
    print(f"✅ Exported parquet files to '{output_dir}'")

    if empty_docs:
        print("Empty documents:")
        for doc in empty_docs:
            print(doc)

    if not_in_gsheets:
        print("Docs not present in gheets:")
        for doc in not_in_gsheets:
            print(doc)


def _write_parquet_shards(
    rows: Iterable[Dict],
    output_dir: str,
    schema: pa.Schema,
    max_rows_per_file: int,
    target_file_size_bytes: int,
    max_rows_per_row_group: int,
) -> Tuple[int, int]:
    """
    Stream documents into multiple parquet files to avoid loading everything in memory and
    keep row groups small enough for Hugging Face's parquet scan limits.
    """
    os.makedirs(output_dir, exist_ok=True)

    batch: List[Dict] = []
    batch_bytes = 0
    file_index = 0
    total_rows = 0

    def _flush(current_batch: List[Dict], index: int) -> None:
        table = pa.Table.from_pylist(current_batch, schema=schema)
        file_path = os.path.join(output_dir, f"tatar_structured_content_{index:04d}.parquet")
        pq.write_table(
            table,
            file_path,
            compression="snappy",
            row_group_size=max_rows_per_row_group,
            write_page_index=True,
        )
        print(f"Wrote {len(current_batch)} rows to {file_path}")

    for row in rows:
        estimated_bytes = (
            len(row["id"])
            + (len(row.get("genre") or ""))
            + len(row["text"].encode("utf-8"))
            + 4  # publish_year
        )
        batch.append(row)
        batch_bytes += estimated_bytes

        if batch and (len(batch) >= max_rows_per_file or batch_bytes >= target_file_size_bytes):
            _flush(batch, file_index)
            total_rows += len(batch)
            file_index += 1
            batch = []
            batch_bytes = 0

    if batch:
        _flush(batch, file_index)
        total_rows += len(batch)
        file_index += 1

    return total_rows, file_index
