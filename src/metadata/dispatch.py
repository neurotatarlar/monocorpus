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


from sqlalchemy import select
from rich import print
from s3 import upload_file, create_session
from utils import read_config, get_in_workdir, download_file_locally, load_expired_keys, dump_expired_keys, get_session
from dirs import Dirs
from gemini import create_client
import zipfile
import isbnlib
import re
import time
from google.genai.errors import ClientError
from queue import Queue, Empty
import threading
from .text_extractor import FromTextMetadataExtractor
from .pdf_slice_extractor import FromPdfSliceMetadataExtractor
import os
from utils import encrypt
from yadisk_client import YaDisk
import gc
import datetime
import time
from models import Document, DocumentCrh
import random

model = 'gemini-3-flash-preview'
# model = "gemini-2.5-flash"


def extract_metadata():
    """
    Main entry point for metadata extraction process.
    
    Processes all documents that don't have metadata and either:
    - Have content URL stored
    - Are PDF files
    # """
    print("Processing 'tt' documents without metadata")
    _process_by_predicate('tt')
    
    
def _process_by_predicate(lang_tag, docs_batch_size=5000, keys_batch_size=1):
    """
    Process documents matching the given predicate using parallel workers.
    
    Args:
        predicate: SQLAlchemy filter predicate
        docs_batch_size: Number of documents to process in one batch
        keys_batch_size: Number of API keys to use in parallel
    """
    config = read_config()
    exceeded_keys_lock = threading.Lock()
    exceeded_keys_set = load_expired_keys()
    entity_cls = Document if lang_tag == 'tt' else DocumentCrh
    
    while True:
        tasks_queue = None
        threads = None
        dump_expired_keys(exceeded_keys_set)
        gc.collect()
        try: 
            unprocessles = _load_unprocessables()
            predicate = (
                entity_cls.meta.is_(None) & (
                    entity_cls.content_url.is_not(None) | (entity_cls.mime_type == 'application/pdf')
                )
                & entity_cls.md5.not_in(unprocessles)
                # & ~entity_cls.ya_path.startswith('/НейроТатарлар/other_turkic_langs/Крымскотатарский/Пресса/Янъы Дюнья')
                # & ~entity_cls.ya_path.startswith('/НейроТатарлар/other_turkic_langs/Крымскотатарский/Книги/Kitaphanesi/Qadınlıq Sotsializm Yolunda')
            )
            with exceeded_keys_lock:
                available_keys =  list(set(config["gemini_api_keys"]) - exceeded_keys_set)
            random.shuffle(available_keys)
            keys_slice = available_keys[:keys_batch_size]
            if not keys_slice:
                print("No keys available, exiting...")
                return
            else:
                print(f"Available keys: {available_keys}, Total keys: {config['gemini_api_keys']}, Exceeded keys: {exceeded_keys_set}, Extracting with keys: {keys_slice}")
                
            with get_session() as session:
                docs = list(session.scalars(select(entity_cls).where(predicate).limit(docs_batch_size)))

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
                    t = threading.Thread(target=MetadataExtractionWorker(key, tasks_queue, config, ya_client, exceeded_keys_lock, exceeded_keys_set, lang_tag))
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
    
    def __init__(self, gemini_api_key, tasks_queue, config, ya_client, exceeded_keys_lock, exceeded_keys_set, lang_tag):
        self.key = gemini_api_key
        self.tasks_queue = tasks_queue
        self.config = config
        self.ya_client = ya_client
        self.exceeded_keys_lock = exceeded_keys_lock
        self.exceeded_keys_set = exceeded_keys_set
        self.lang_tag=lang_tag
        
        
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
                    self._dump_unprocessables(doc.md5)
                    continue
                # write metadata to zip
                local_meta_path = get_in_workdir(Dirs.METADATA, file=f"{doc.md5}.zip")
                with zipfile.ZipFile(local_meta_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
                    meta_json = metadata.model_dump_json(indent=None, by_alias=True, exclude_none=True, exclude_unset=True, ensure_ascii=False)
                    zf.writestr("metadata.json", meta_json)

                # upload metadata to s3
                self._upload_artifacts_to_s3(doc, local_meta_path, local_doc_path)
                with get_session() as session:
                    self._update_document(doc.md5, metadata, session, meta_json)
                self.log(f"Metadata extracted and uploaded for document {doc.md5}({doc.ya_public_url})")
                self.log(f"Metadata: {meta_json}")
            except Empty:
                self.log("No tasks for processing, shutting down thread...")
                return
            except ClientError as e:
                print(f"ClientError during metadata extraction for doc '{doc.md5}({doc.ya_path})' with key '{self.key}': {e}")
                self._dump_unprocessables(doc.md5)
                if e.code == 429:
                    self.log(f"Key {self.key} exhausted {e}, shutting down thread...") 
                    self.tasks_queue.put(doc)
                    with self.exceeded_keys_lock:
                        self.exceeded_keys_set.add(self.key)
                    break
                continue
            except Exception as e:
                import traceback
                self.log(f"Could not extract metadata from doc {doc.md5}: {e} \n{traceback.format_exc()}")
                self._dump_unprocessables(doc.md5)
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


    def _update_document(self, doc_md5, meta, session, meta_json):
        doc = session.get(Document, doc_md5) if self.lang_tag == 'tt' else session.get(DocumentCrh, doc_md5)
        doc.publisher = meta.publisher.name if meta.publisher and meta.publisher.name.lower() != 'unknown' else None
        doc.author =  ", ".join([a.name for a in meta.author if a.name.lower() != 'unknown' ]) if meta.author else None
        doc.title = meta.name if meta.name and meta.name.lower() != 'unknown' else None
        doc.language=", ".join(sorted([i.strip() for i in meta.inLanguage.split(",") if i.strip()])) if meta.inLanguage else None
        doc.genre=", ".join([g.lower() for g in meta.genre if g.lower() != 'unknown']) if meta.genre else None
        doc.translated = bool([c for c in meta.contributor if c.role == 'translator']) if meta.contributor else None
        if (_publish_date := meta.datePublished) and meta.datePublished.lower() != 'unknown':
            if res := re.match(r"^(\d{4})([\d-]*)$", _publish_date.strip()):
                doc.publish_date = res.group(1)
            
        doc.isbn = ''
        if meta.isbn:
            isbns = set()
            for isbn in meta.isbn:
                if scraped_isbns := isbnlib.get_isbnlike(isbn, level="strict"):
                    for _isbn in scraped_isbns:
                        if _isbn := _isbn.strip():
                            isbns.add(isbnlib.canonical(_isbn))
            if joined_isbn := ", ".join([isbn.strip() for isbn in sorted(isbns) if isbn.strip()]):
                doc.isbn = joined_isbn
                print(f"Extracted isbns: '{doc.isbn}'")
        
        def _extract_classification(_properties, _expected_names):
            if _properties:
                vals = [
                    p.value.strip().replace(' ', '').replace('\n', '')
                    for p
                    in _properties 
                    if p.name.strip().upper() in _expected_names and p.value.lower() not in ['unknown', 'неизвестно']]
                if len(vals) == 1:
                    return vals[0] 
            return None
            
        if meta.numberOfPages and doc.page_count and abs(meta.numberOfPages - int(doc.page_count)) < 5:
            # if model detected count of pages in the document 
            # and the pages count is not too far from count of pages in pdf file
            doc.page_count = meta.numberOfPages
         
        doc.meta = meta_json
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
    
    
    def _dump_unprocessables(self, md5, lock="unprocessables/unprocessables_meta.lock", file="unprocessables/unprocessables_meta.txt"):
        """
        Check lock file exists, if lock file is not exists, read the unprocessables file into set, add the given md5, and write it back.
        If lock file exists, sleep and retry until lock file is gone.
        """
        while os.path.exists(lock):
            time.sleep(1)
        try:
            with open(lock, "w") as _:
                unprocessables = set([md5])
                if os.path.exists(file):
                    with open(file, "r") as f:
                        for line in f:
                            unprocessables.add(line.strip())
                with open(file, "w") as f:
                    for item in sorted(unprocessables):
                        f.write(f"{item}\n")
                    f.flush()
        finally:
            if os.path.exists(lock):
                os.remove(lock)
        
        
def _load_unprocessables(file="unprocessables/unprocessables_meta.txt"):
    unprocessables = set()
    if os.path.exists(file):
        with open(file, "r") as f:
            for line in f:
                unprocessables.add(line.strip())
    return unprocessables