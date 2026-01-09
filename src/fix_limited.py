import glob
import json
import os
from typing import List, Dict, Any

from sqlalchemy import select
from yadisk_client import YaDisk
from rich.progress import track

from dirs import Dirs
from models import Document
from utils import get_in_workdir, get_session, read_config, download_file_locally


def _has_local_copy(doc, entry_dir: str) -> bool:
    if os.path.exists(os.path.join(entry_dir, doc.md5)):
        return True
    return bool(glob.glob(os.path.join(entry_dir, f"{doc.md5}.*")))


def fix_limited(output_filename: str = "limited_documents.json") -> str:
    with get_session() as session:
        docs = session.scalars(select(Document).where(Document.full.is_(False))).all()

    rows: List[Dict[str, Any]] = [
        {column.name: getattr(doc, column.name) for column in Document.__table__.columns}
        for doc in docs
    ]

    output_path = get_in_workdir(file=output_filename)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(rows)} limited documents to '{output_path}'.")

    if not docs:
        return output_path

    config = read_config()
    entry_dir = get_in_workdir(Dirs.ENTRY_POINT)
    with YaDisk(config['yandex']['disk']['oauth_token'], proxy=config['proxy']) as ya_client:
        for doc in track(docs, description="Downloading limited documents"):
            if _has_local_copy(doc, entry_dir):
                continue
            if not doc.ya_public_url:
                print(f"Skipping {doc.md5}: missing ya_public_url")
                continue
            try:
                download_file_locally(ya_client, doc, config)
            except Exception as exc:
                print(f"Failed to download {doc.md5}: {exc}")

    return output_path
