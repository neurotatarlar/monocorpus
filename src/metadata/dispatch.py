"""
Metadata Extraction Dispatcher Module

This module handles the orchestration of metadata extraction from different document types
using the Gemini AI model. It manages parallel processing, error handling, and state management
for extracting metadata from PDF and text documents.

Key Features:
- Parallel metadata extraction using multiple API keys
- Handling of both PDF and text-based documents
- Automatic retries on API rate limits
- Skip list management for problematic documents
- State persistence for failed extractions
"""

from rich import print
from integrations.s3 import upload_file, create_session
from core.config import read_config
from core.paths import get_in_workdir
from core.yadisk import download_file_locally
from core.state import load_expired_keys, dump_expired_keys
from core.db import get_session
from dirs import Dirs
from integrations.gemini import create_client
import zipfile
from google.genai.errors import ClientError
from google.genai.errors import ServerError
from queue import Queue, Empty
import threading
from .text_extractor import FromTextMetadataExtractor
from .pdf_slice_extractor import FromPdfSliceMetadataExtractor
import os
import re
from core.security import encrypt
from integrations.yadisk import YaDisk
import gc
import datetime
import time
import json
from models import Document, Metadata
from .repository import fetch_docs_for_metadata_extraction
from .isbn_utils import canonicalize_isbn_values
from .unprocessables import add_unprocessable, load_unprocessables
from .url_utils import normalize_url_list
import random

model = 'gemini-3-flash-preview'
# model = "gemini-2.5-flash"
UNKNOWN_VALUES = {"", "unknown", "неизвестно", "none", "null", "n/a"}
WHITESPACE_RE = re.compile(r"\s+")
HIGH_DEMAND_SLEEP_SECONDS = 60


def extract_metadata():
    """
    Main entry point for metadata extraction process.
    
    Processes all documents that don't have metadata and either:
    - Have content URL stored
    - Are PDF files
    # """
    print("Processing 'tt' documents without metadata")
    _process_by_predicate()
    
    
def _process_by_predicate(docs_batch_size=5000, keys_batch_size=1):
    """
    Process metadata extraction batches using parallel workers.
    
    Args:
        docs_batch_size: Number of documents to process in one batch
        keys_batch_size: Number of API keys to use in parallel
    """
    config = read_config()
    exceeded_keys_lock = threading.Lock()
    exceeded_keys_set = load_expired_keys()
    
    while True:
        tasks_queue = None
        threads = None
        dump_expired_keys(exceeded_keys_set)
        gc.collect()
        try: 
            unprocessles = load_unprocessables()
            with exceeded_keys_lock:
                available_keys =  list(set(config["gemini_api_keys"]) - exceeded_keys_set)
            random.shuffle(available_keys)
            keys_slice = available_keys[:keys_batch_size]
            if not keys_slice:
                print("No keys available, exiting...")
                return
            else:
                print(f"Available keys: {available_keys}, Total keys: {config['gemini_api_keys']}, Exceeded keys: {exceeded_keys_set}, Extracting with keys: {keys_slice}")
                
            docs = fetch_docs_for_metadata_extraction(
                limit=docs_batch_size,
                excluded_md5s=unprocessles,
            )

            print(f"Got {len(docs)} docs for metadata extraction")
            tasks_queue = Queue(maxsize=len(docs))
            for doc in docs:
                tasks_queue.put(doc)
                
            if tasks_queue.empty():
                print("No documents for processing...")
                return
                            
            threads = []
            with YaDisk(config['yandex']['disk']['oauth_token'], proxy=config['proxy']) as ya_client:
                for num in range(min(len(keys_slice), len(docs))):
                    key = keys_slice[num]
                    t = threading.Thread(target=MetadataExtractionWorker(key, tasks_queue, config, ya_client, exceeded_keys_lock, exceeded_keys_set))
                    t.start()
                    threads.append(t)
                    time.sleep(5)  # slight delay to avoid overwhelming the API with requests

            # Shutdown workers gracefully
            for t in threads:
                t.join()
        except KeyboardInterrupt:
            print("Interrupted, shutting down workers...")
            if tasks_queue:
                tasks_queue.queue.clear()  # Clear the queue to stop workers
            if threads:
                for t in threads:
                    t.join(timeout=120)
            return
        except Exception as e:
            print(f"Error during processing: {e}")
            continue
        finally:
            dump_expired_keys(exceeded_keys_set)

        
        
       
class MetadataExtractionWorker:
    """
    Worker thread for parallel metadata extraction.
    
    Attributes:
        api_key: Gemini API key
        docs_queue: Queue of documents to process
        results_queue: Queue for processing results
    """
    
    def __init__(self, gemini_api_key, tasks_queue, config, ya_client, exceeded_keys_lock, exceeded_keys_set):
        self.key = gemini_api_key
        self.tasks_queue = tasks_queue
        self.config = config
        self.ya_client = ya_client
        self.exceeded_keys_lock = exceeded_keys_lock
        self.exceeded_keys_set = exceeded_keys_set
        self.lang_tag = 'tt'
        
        
    def __call__(self):
        """Process documents from queue until receiving None"""
        gemini_client = create_client(self.key)
        prev_req_time = None
        while True:
            try:
                local_doc_path = None
                doc = self.tasks_queue.get(block=False)
                self.log(f"Extracting metadata from document {doc.md5}({doc.ya_public_url})")
                
                if doc.content_url:
                    prev_req_time = self._sleep_if_needed(prev_req_time)
                    metadata = FromTextMetadataExtractor(doc, self.config, gemini_client, model=model, lang_tag=self.lang_tag).extract()
                elif doc.mime_type == 'application/pdf':
                    local_doc_path = local_doc_path = download_file_locally(self.ya_client, doc, self.config)
                    prev_req_time = self._sleep_if_needed(prev_req_time)
                    metadata = FromPdfSliceMetadataExtractor(doc, self.config, gemini_client, model, local_doc_path, lang_tag=self.lang_tag).extract()
                else:
                    self.log(f"Document {doc.md5} has no content_url or is not a PDF, skipping...")
                    continue
                
                if not metadata:
                    self.log(f"No metadata was extracted from document {doc.md5}({doc.ya_public_url})")
                    add_unprocessable(doc.md5)
                    continue
                # write metadata to zip
                local_meta_path = get_in_workdir(Dirs.METADATA, file=f"{doc.md5}.zip")
                schema_org = _normalize_base_schema_org(json.loads(metadata.model_dump_json(by_alias=True, exclude_none=True, exclude_unset=True, ensure_ascii=False)))
                meta_json = json.dumps(schema_org, ensure_ascii=False)
                with zipfile.ZipFile(local_meta_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
                    zf.writestr("metadata.json", meta_json)

                # upload metadata to s3
                self._upload_artifacts_to_s3(doc, local_meta_path, local_doc_path)
                with get_session() as session:
                    self._update_document(doc.md5, metadata, session, schema_org)
                self.log(f"Metadata extracted and uploaded for document {doc.md5}({doc.ya_public_url})")
                self.log(f"Metadata: {meta_json}")
            except Empty:
                self.log("No tasks for processing, shutting down thread...")
                return
            except ClientError as e:
                print(f"ClientError during metadata extraction for doc '{doc.md5}({doc.ya_path})' with key '{self.key}': {e}")
                add_unprocessable(doc.md5)
                if e.code == 429:
                    self.log(f"Key {self.key} exhausted {e}, shutting down thread...") 
                    self.tasks_queue.put(doc)
                    with self.exceeded_keys_lock:
                        self.exceeded_keys_set.add(self.key)
                    break
                continue
            except ServerError as e:
                if _is_high_demand_503(e):
                    self.log(
                        f"Gemini high demand for {doc.md5}; sleeping {HIGH_DEMAND_SLEEP_SECONDS}s and retrying"
                    )
                    self.tasks_queue.put(doc)
                    time.sleep(HIGH_DEMAND_SLEEP_SECONDS)
                    continue
                import traceback
                self.log(f"ServerError during metadata extraction for doc {doc.md5}: {e} \n{traceback.format_exc()}")
                add_unprocessable(doc.md5)
                continue
            except Exception as e:
                import traceback
                self.log(f"Could not extract metadata from doc {doc.md5}: {e} \n{traceback.format_exc()}")
                add_unprocessable(doc.md5)
                continue
            

    def _sleep_if_needed(self, prev_req_time):
        if prev_req_time:
            elapsed = datetime.datetime.now() - prev_req_time
            if elapsed < datetime.timedelta(minutes=1):
                time_to_sleep = int(65 - elapsed.total_seconds()) + 1
                self.log(f"Sleeping for {time_to_sleep} seconds")
                time.sleep(time_to_sleep)
        return datetime.datetime.now()
            
            
    def _upload_artifacts_to_s3(self, doc, local_meta_path, local_doc_path):   
        s3lient = create_session(self.config)
        meta_key = f"{doc.md5}-meta.zip"
        meta_bucket = self.config["yandex"]["cloud"]['bucket']['metadata']
        upload_file(local_meta_path, meta_bucket, meta_key, s3lient, skip_if_exists=False)
        
        if local_doc_path:
            doc_bucket = self.config["yandex"]["cloud"]['bucket']['document']
            doc_key = os.path.basename(local_doc_path)
            remote_doc_url = upload_file(local_doc_path, doc_bucket, doc_key, s3lient, skip_if_exists=True)
            doc.document_url = encrypt(remote_doc_url, self.config) if doc.sharing_restricted else remote_doc_url


    def _update_document(self, doc_md5, meta, session, schema_org):
        doc = session.get(Document, doc_md5)
        doc.language=", ".join(sorted([i.strip() for i in meta.inLanguage.split(",") if i.strip()])) if meta.inLanguage else None

        metadata_row = session.get(Metadata, doc_md5)
        if metadata_row is None:
            metadata_row = Metadata(md5=doc_md5)
            session.add(metadata_row)
        metadata_row.schema_org = schema_org

        doc.meta_extraction_method = f"{model}/prompt.v2"
        if doc.sharing_restricted:
            if doc.ya_public_url and not doc.ya_public_url.startswith('enc:'):
                doc.ya_public_url = encrypt(doc.ya_public_url, self.config)
                
            doc.document_url = encrypt("https://storage.yandexcloud.net/ttdoc/2327c706ebdd9f6334fe889b32107787.pdf", self.config)
            
        session.commit()


    def log(self, message):
        message = f"{threading.current_thread().name} {time.strftime('%d-%m-%y %H:%M:%S')} {self.key[-7:]}: {message}"
        log_file = get_in_workdir(Dirs.LOGS, file=f"meta_extraction_{self.key}.log")
        with open(log_file, "a") as log:
            log.write(f"{message}\n")
        print(message)


def _normalize_base_schema_org(schema_org: dict) -> dict:
    """Normalize base metadata fields before storing in `metadata.schema_org`."""
    updated = dict(schema_org)

    _set_or_drop(updated, "name", _clean_text(updated.get("name"), max_len=600))
    _set_or_drop(updated, "description", _clean_text(updated.get("description"), max_len=5000))
    _set_or_drop(updated, "audience", _clean_text(updated.get("audience"), max_len=400))
    _set_or_drop(updated, "inLanguage", _normalize_in_language(updated.get("inLanguage")))
    _set_or_drop(updated, "datePublished", _normalize_date_published(updated.get("datePublished")))
    _set_or_drop(updated, "numberOfPages", _normalize_int(updated.get("numberOfPages"), 1, 20_000))
    _set_or_drop(updated, "bookEdition", _normalize_int(updated.get("bookEdition"), 1, 1_000))
    _set_or_drop(updated, "genre", _normalize_string_list(updated.get("genre"), max_len=120, lower_case=True))
    _set_or_drop(updated, "author", _normalize_people(updated.get("author"), keep_role=False))
    _set_or_drop(updated, "contributor", _normalize_people(updated.get("contributor"), keep_role=True))
    _set_or_drop(updated, "publisher", _normalize_publisher(updated.get("publisher")))
    _set_or_drop(updated, "isBasedOn", _normalize_is_based_on(updated.get("isBasedOn")))

    normalized_isbn = canonicalize_isbn_values(updated.get("isbn"))
    if normalized_isbn:
        updated["isbn"] = normalized_isbn
    else:
        updated.pop("isbn", None)

    about_items = updated.get("about")
    about = about_items if isinstance(about_items, list) else ([about_items] if about_items else [])

    seen = set()
    normalized_about = []
    for item in about:
        if not isinstance(item, dict):
            continue
        termset = _clean_text(item.get("inDefinedTermSet"), max_len=120)
        term_code = _clean_text(item.get("termCode") or item.get("name"), max_len=500)
        if not termset or not term_code:
            continue
        if termset.casefold() in {"ddc", "genre", "categorypath"}:
            continue
        key = (termset.casefold(), term_code.casefold())
        if key in seen:
            continue
        seen.add(key)
        normalized_about.append(
            {
                "@type": "DefinedTerm",
                "termCode": term_code,
                "inDefinedTermSet": termset,
            }
        )

    if normalized_about:
        updated["about"] = normalized_about
    elif "about" in updated:
        updated.pop("about", None)

    updated.pop("additionalProperty", None)
    return updated


def _is_high_demand_503(error: Exception) -> bool:
    """Return True when Gemini reports temporary 503 high-demand condition."""
    for value in (getattr(error, "status_code", None), getattr(error, "code", None)):
        if isinstance(value, int) and value == 503:
            return True
    message = str(error).casefold()
    return "currently experiencing high demand" in message


def _set_or_drop(data: dict, key: str, value):
    if value is None:
        data.pop(key, None)
    else:
        data[key] = value


def _clean_text(value, max_len: int = 1000):
    if value is None:
        return None
    text = WHITESPACE_RE.sub(" ", str(value)).strip()
    if not text:
        return None
    if text.casefold() in UNKNOWN_VALUES:
        return None
    return text[:max_len]


def _normalize_string_list(value, max_len: int = 200, lower_case: bool = False):
    items = value if isinstance(value, list) else [value]
    out = []
    seen = set()
    for item in items:
        text = _clean_text(item, max_len=max_len)
        if not text:
            continue
        if lower_case:
            text = text.lower()
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out or None


def _normalize_people(value, keep_role: bool):
    items = value if isinstance(value, list) else [value]
    out = []
    seen = set()
    for item in items:
        if isinstance(item, dict):
            name = _clean_text(item.get("name"), max_len=300)
            person_type = _clean_text(item.get("@type"), max_len=40) or "Person"
            role = _clean_text(item.get("role"), max_len=120) if keep_role else None
        else:
            name = _clean_text(item, max_len=300)
            person_type = "Person"
            role = None
        if not name:
            continue
        key = (name.casefold(), person_type.casefold(), (role or "").casefold())
        if key in seen:
            continue
        seen.add(key)
        normalized = {"@type": person_type, "name": name}
        if role:
            normalized["role"] = role
        out.append(normalized)
    return out or None


def _normalize_publisher(value):
    if isinstance(value, dict):
        name = _clean_text(value.get("name"), max_len=400)
    else:
        name = _clean_text(value, max_len=400)
    if not name:
        return None
    return {"@type": "Organization", "name": name}


def _normalize_is_based_on(value):
    if not isinstance(value, dict):
        return None

    normalized = {}
    work_type = _clean_text(value.get("@type"), max_len=80) or "CreativeWork"
    normalized["@type"] = work_type

    if name := _clean_text(value.get("name"), max_len=600):
        normalized["name"] = name
    if author := _normalize_people(value.get("author"), keep_role=False):
        normalized["author"] = author
    if in_language := _normalize_in_language(value.get("inLanguage")):
        normalized["inLanguage"] = in_language
    if urls := normalize_url_list(value.get("url")):
        normalized["url"] = urls

    if len(normalized) == 1 and normalized.get("@type") == "CreativeWork":
        return None
    return normalized


def _normalize_date_published(value):
    raw = _clean_text(value, max_len=40)
    if not raw:
        return None
    raw = raw.replace("/", "-")
    if re.fullmatch(r"\d{4}(-\d{2})?(-\d{2})?", raw):
        year = int(raw[:4])
        return raw if 1500 <= year <= 2100 else None
    return None


def _normalize_int(value, min_value: int, max_value: int):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        int_val = int(value)
        return int_val if min_value <= int_val <= max_value else None
    text = _clean_text(value, max_len=40)
    if not text:
        return None
    match = re.search(r"\d+", text)
    if not match:
        return None
    int_val = int(match.group(0))
    return int_val if min_value <= int_val <= max_value else None


def _normalize_in_language(value):
    text = _clean_text(value, max_len=200)
    if not text:
        return None
    codes = [part.strip() for part in text.split(",")]
    codes = [code for code in codes if code]
    if not codes:
        return None
    normalized = []
    seen = set()
    for code in codes:
        key = code.casefold()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(code)
    return ", ".join(sorted(normalized))
    
