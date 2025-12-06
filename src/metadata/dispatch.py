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

model = 'gemini-2.5-pro'


skip_pdf = set([
    "f8eba00ee3e0a74aa15439f83173a358",
    "ecc476f02c8255e53ef76a019443f452",
    "0139072f6aeb40a0b85d9e3513ff3c18",
    "34a5f7d1f01d8bc7aadebf2bfba18a7f",
    "89e7b6bdfc19e7e3ff3319e1e498e99c",
    "73ab5e2215a6d0caa639af3adaa8dbe0",
    "8ee3f0890cc5fcde8a8f124c41e2e484",
    "67d74ff4b3437c4f4f36d93f2a565141",
    "cead69d49dc5293c3784498dcdf1b1cf",
    "e8d527cce10b132660a6225adfdd64ae",
    "aedb220536b900cd51b34fc349993b62",
    "40f582f43ed246b9c2510b64bc273bdf",
    "560a3cfcabd9d37a1ccc5293523ccff0",
    "743288b446c9489f218050df9ec85b28",
    "fe3134f337e21ac4f72173f4c15875c2",
    "f76fd2938d9c90e32df0935fe1a5d104",
    "12730476a04202678e5ed8549aeb54b8",
    "9a8773e8e2c524e6113c1061086e6d48",
    "9e13ff7fd9740e3f4089b6fd63bc259b",    
    "b56ff22e1fc6b2e834a9e6b34699c3cf",
    'e73af3961941eda31ec13eb83e9f7b39',
    '3e0e45b2353c99c80fa44f493b5f4d93',
    '6aa21b915021ddc713dab5a0b43d813b',
    '7349b5837efd90c5e0c0f9db79387916',
    'a503f53802bab9df52733806202dd797',
    '0bb284a498c012b690b26450a76ff581',
    '487b7ccaac157c062d111d46f48ec2fb',
    '30e52c059f85fc4701d0a66e9b1a4163',
    '0993ad06508d849d26b06b5fb2481446',
    'e06774a36d22c63fd420a49883569127',
    '9d7fdbae61b1bb84d40d2f3303edac3e',
    'd7b8654da4792c9c58df997219e692d0',
    "c6d393986c8a1dfe31a9dff35146dfb7",
    "5bb92eed2581797bf955ca2bf67967b3",
    "981870dd6f7fca02e442464ecdc958d7",
    "b59f92e4833d8cf94acbaec2876f3dc0",
    "2e141a65709cf83a1ef8895edca25c6c",
    "f1f4130e41d90d8a8e62846fc16ee97c",
    "78053ec0f31c6975276d1f713e16dcc0",
    "774dae921750ebc4d607bb509d075082",
    "0d8a8c8add3d963f83ab2d0fd8e45bc2",
    "f0746565926ecfbcec3f6f7282f55d41",
    "fc16079061fdec3564aa9e4de582a4c9",
    "e7a0b00ae075705417027afebdf3a513",
    "b3219129aa860c3736c2fb1ebc92e444",
    "348d8cc4883d7d5b6aea29444c6abf75",
    "ac129581ada1a3cd5d6b3ee6f5fa0885",
    "ed41ef170ec6c4469e81ee469aa77f1a",
    "d775776188d8f6da062bad57ebb38a51",
    "18ab2b7ef6fd66273fa860a2bc8d92e9",
])  # --- IGNORE ---

def extract_metadata():
    """
    Main entry point for metadata extraction process.
    
    Processes all documents that don't have metadata and either:
    - Have content URL stored
    - Are PDF files
    # """
    # print("Processing 'tt' documents without metadata")
    # predicate = (
    #     Document.meta.is_(None) & (
    #         Document.content_url.is_not(None) | (Document.mime_type == 'application/pdf')
    #     )
    #     & Document.md5.not_in(skip_pdf)
    # )
    # _process_by_predicate(predicate, 'tt')
    
    print("Processing 'crh' documents without metadata")
    predicate = (
        DocumentCrh.meta.is_(None) & (
            DocumentCrh.content_url.is_not(None) | (DocumentCrh.mime_type == 'application/pdf')
        )
        & DocumentCrh.md5.not_in(skip_pdf)
    )
    _process_by_predicate(predicate, 'crh')
    
    
def _process_by_predicate(predicate, lang_tag, docs_batch_size=300, keys_batch_size=6):
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
    
    while True:
        tasks_queue = None
        threads = None
        dump_expired_keys(exceeded_keys_set)
        gc.collect()
        try: 
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
                entity_cls = Document if lang_tag == 'tt' else DocumentCrh
                docs = list(session.scalars(select(entity_cls).where(predicate).limit(docs_batch_size).offset(10)))

            print(f"Got {len(docs)} docs for metadata extraction")
            tasks_queue = Queue(maxsize=len(docs))
            for doc in docs:
                if doc.md5 in skip_pdf:
                    continue
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
            except Empty:
                self.log("No tasks for processing, shutting down thread...")
                return
            except ClientError as e:
                print(f"ClientError during metadata extraction for doc '{doc.md5}({doc.ya_path})' with key '{self.key}': {e}")
                if e.code == 429:
                    self.log(f"Key {self.key} exhausted {e}, shutting down thread...") 
                    self.tasks_queue.put(doc)
                    with self.exceeded_keys_lock:
                        self.exceeded_keys_set.add(self.key)
                    return
                continue
            except Exception as e:
                import traceback
                self.log(f"Could not extract metadata from doc {doc.md5}: {e} \n{traceback.format_exc()}")
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
            
        session.commit()


    def log(self, message):
        message = f"{threading.current_thread().name} {time.strftime('%d-%m-%y %H:%M:%S')} {self.key[-7:]}: {message}"
        log_file = get_in_workdir(Dirs.LOGS, file=f"meta_extraction_{self.key}.log")
        with open(log_file, "a") as log:
            log.write(f"{message}\n")
        print(message)
    