import json
import os
from typing import Dict, Iterable, Tuple, Optional

from sqlalchemy import select

from dirs import Dirs
from models import Document
from utils import get_in_workdir, get_session, load_upstream_metadata
from pathlib import Path


def _load_metadata(md5: str, upstream_dir: str, upstream_url: str) -> Dict:
    json_path = Path(f"{upstream_dir}/{md5}/metadata.json")
    if not (os.path.exists(json_path) or load_upstream_metadata(upstream_url, md5)):
        return None
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _iter_full_documents(session) -> Iterable[Tuple[str, Optional[str], Optional[str]]]:
    rows = session.execute(
        select(Document.md5, Document.upstream_meta_url, Document.title, Document.ya_path).where(Document.full.is_(False))
    ).all()
    return rows


# def _download_upstream_metadata(md5: str, url: str, dest_path: Path) -> Optional[str]:
#     if not url:
#         return None

#     os.makedirs(dest_path.parent, exist_ok=True)
#     resp = requests.get(url, stream=True, timeout=60)
#     resp.raise_for_status()

#     ext = os.path.splitext(urlsplit(url).path)[1].lower()
#     # ext = ext if ext in {".zip", ".json"} else ".zip"

#     with open(dest_path, "wb") as f:
#         for chunk in resp.iter_content(chunk_size=8192):
#             if chunk:
#                 f.write(chunk)

#     if ext == ".zip":
#         try:
#             with zipfile.ZipFile(dest_path, "r") as zf:
#                 if "metadata.json" not in zf.namelist():
#                     raise FileNotFoundError(f"metadata.json not found inside downloaded archive {dest_path}")
#         except zipfile.BadZipFile as exc:
#             raise FileNotFoundError(f"Downloaded archive for {md5} is not a valid zip: {dest_path}") from exc

#     print(f"Downloaded upstream metadata for {md5} to {dest_path}")
#     return dest_path


def collect_download_codes(output_filename: str = "download_codes.json") -> None:
    upstream_dir = get_in_workdir(Dirs.UPSTREAM_METADATA)

    with get_session() as session:
        docs = set(_iter_full_documents(session))

    if not docs:
        print("No documents with full flag found. Nothing to do.")
        return

    download_codes = set()
    titles_without_upstream = set()

    for md5, upstream_url, title, ya_path in docs:
        if (metadata := _load_metadata(md5, upstream_dir, upstream_url)):
            if (download_code := metadata.get("download_code")):
                download_codes.add(download_code)
                continue
            elif (title := metadata.get("title")):
                titles_without_upstream.add(title)
                continue
                
        if title:
            titles_without_upstream.add(title)
        elif ya_path:
            title_from_filename = Path(ya_path).stem
            titles_without_upstream.add(title_from_filename)
        else:
            print(md5, upstream_url, title, ya_path)

    output_path = get_in_workdir(file=output_filename)
    result = {
        "download_codes": sorted(download_codes),
        "titles_without_upstream_metadata": sorted(titles_without_upstream),
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4, ensure_ascii=False)

    print(f"Collected {len(download_codes)} unique download codes from {len(docs)} documents.")
    if titles_without_upstream:
        print(f"Docs without upstream metadata: {len(titles_without_upstream)}")
    print(f"Saved download codes to '{output_path}'.")
    
