from sqlalchemy import select
from rich import print
from s3 import upload_file, create_session
from utils import read_config, get_in_workdir, download_file_locally
from dirs import Dirs
from gemini import create_client
import zipfile
import isbnlib
import re
from monocorpus_models import Document, Session
import time
from google.genai.errors import ClientError
from queue import Queue, Empty
import threading
from .text_extractor import FromTextMetadataExtractor
from .pdf_slice_extractor import FromPdfSliceMetadataExtractor
import os
from utils import encrypt
from yadisk_client import YaDisk


model = 'gemini-2.5-pro'


skip_pdf = set([
    "dd713e13dd749131652b7eef5fedf4ac",
    "b2d56b82efc561e9e74f56d8701fd646",
    "913471a88265ebb27423b67477ea5f8a",
    "32fdbabed0d8c5542cc4cf6dfa69d9ee",
    "31a91979335f7c39e34ce764965f41d8",
    "d4aa4ac8fdcd996d985f5bdafe3244d7",
    "edadf934d54bb952958c9798b72af2fd",
    "779f5587af3faee67d767ae4170d7c7e",
    "2cd6ab2836e3062b322701da834ffb3e",
    "60be21a1742e1c5723a034651314fc96",
    "8f06ce5728c80dd2564eb0e7ada9b601",
    "ae032d9ba4b2d7a32e2862439c848099",
    "41752f8460921044a5909ff348b01b05",
    "63092bd67e856d3a2ee93066737a4640",
])


def extract_metadata():
    print("Processing documents without metadata")
    predicate = Document.metadata_url.is_(None) & (Document.content_url.is_not(None) | Document.mime_type.is_('application/pdf'))
    _process_by_predicate(predicate)
    
    predicate = Document.metadata_extraction_method.is_not("gemini-2.5-pro/prompt.v2") & (Document.content_url.is_not(None) | Document.mime_type.is_('application/pdf'))
    print("Processing documents with older metadata extraction method...")
    _process_by_predicate(predicate)

    
def _process_by_predicate(predicate, docs_batch_size=72, keys_batch_size=18):
    config = read_config()
    exceeded_keys_lock = threading.Lock()
    exceeded_keys_set = set()
    
    while True:
        with exceeded_keys_lock:
            if not (keys_slice := list(set(config["gemini_api_keys"]["free"]) - exceeded_keys_set)[:keys_batch_size]):
                return
        
        tasks_queue = None
        threads = None
        try: 
            while True:
                with Session() as read_session:
                    docs = read_session.query(select(Document).where(predicate).limit(docs_batch_size))

                print(f"Got {len(docs)} docs for metadata extraction")
                tasks_queue = Queue(maxsize=len(docs))
                for doc in docs:
                    if doc.md5 in skip_pdf:
                        continue
                    tasks_queue.put(doc)
                    
                if tasks_queue.empty():
                    print("No documents for processing...")
                    return
                
                s3lient = create_session(config)
                    
                threads = []
                with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client:
                    for key in keys_slice:
                        t = threading.Thread(target=MetadataExtractionWorker(key, tasks_queue, config, s3lient, ya_client, exceeded_keys_lock, exceeded_keys_set))
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
                    t.join(timeout=60)
            return
        
        
        
class MetadataExtractionWorker:
    
    
    def __init__(self, gemini_api_key, tasks_queue, config, s3lient, ya_client, exceeded_keys_lock, exceeded_keys_set):
        self.key = gemini_api_key
        self.tasks_queue = tasks_queue
        self.config = config
        self.s3lient = s3lient
        self.ya_client = ya_client
        self.exceeded_keys_lock = exceeded_keys_lock
        self.exceeded_keys_set = exceeded_keys_set
        
        
    def __call__(self):
        gemini_client = create_client(self.key)
        while True:
            try:
                local_doc_path = None
                doc = self.tasks_queue.get(block=False)
                self.log(f"Extracting metadata from document {doc.md5}({doc.ya_public_url}) by key {self.key}")
                
                if doc.content_url:
                    metadata = FromTextMetadataExtractor(doc, self.config, gemini_client, self.s3lient, model=model).extract()
                elif doc.mime_type == 'application/pdf':
                    local_doc_path = local_doc_path = download_file_locally(self.ya_client, doc, self.config)
                    metadata = FromPdfSliceMetadataExtractor(doc, self.config, gemini_client, self.s3lient, model, local_doc_path).extract()
                else:
                    self.log(f"Document {doc.md5} has no content_url or is not a PDF, skipping...")
                    continue
                
                if not metadata:
                    self.log(f"No metadata was extracted from document {doc.md5}")
                    continue
                # write metadata to zip
                local_meta_path = get_in_workdir(Dirs.METADATA, file=f"{doc.md5}.zip")
                with zipfile.ZipFile(local_meta_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
                    meta_json = metadata.model_dump_json(indent=None, by_alias=True, exclude_none=True, exclude_unset=True)
                    zf.writestr("metadata.json", meta_json)

                # upload metadata to s3
                self._upload_artifacts_to_s3(doc, local_meta_path, local_doc_path)
                doc.metadata_extraction_method = f"{model}/prompt.v2"
                with Session() as gsheet_session:
                    self._update_document(doc, metadata, gsheet_session)
                self.log(f"Metadata extracted and uploaded for document {doc.md5}({doc.ya_public_url})")
            except Empty:
                self.log("No tasks for processing, shutting down thread...")
                return
            except ClientError as e:
                if e.code == 429:
                    self.log(f"Key {self.key} exhausted {e}, shutting down thread...") 
                    self.tasks_queue.put(doc)
                    with self.exceeded_keys_lock:
                        self.exceeded_keys_set.add(self.key)
                return
            except Exception as e:
                import traceback
                self.log(f"Could not extract metadata from doc {doc.md5}: {e} \n{traceback.format_exc()}")
                continue
            
            
    def _upload_artifacts_to_s3(self, doc, local_meta_path, local_doc_path):        
        meta_key = f"{doc.md5}-meta.zip"
        meta_bucket = self.config["yandex"]["cloud"]['bucket']['metadata']
        doc.metadata_url = upload_file(local_meta_path, meta_bucket, meta_key, self.s3lient, skip_if_exists=False)
        
        if local_doc_path:
            doc_bucket = self.config["yandex"]["cloud"]['bucket']['document']
            doc_key = os.path.basename(local_doc_path)
            remote_doc_url = upload_file(local_doc_path, doc_bucket, doc_key, self.s3lient, skip_if_exists=True)
            doc.document_url = encrypt(remote_doc_url, self.config) if doc.sharing_restricted else remote_doc_url


    def _update_document(self, doc, meta, gsheet_session):
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
            
        if _bbc := _extract_classification(meta.additionalProperty, ["ББК", "BBC"]):
            doc.bbc = _bbc
            
        if _udc := _extract_classification(meta.additionalProperty, ["УДК", "UDC"]):
            doc.udc = _udc
            
        if meta.numberOfPages and doc.page_count and abs(meta.numberOfPages - int(doc.page_count)) < 5:
            # if model detected count of pages in the document 
            # and the pages count is not too far from count of pages in pdf file
            doc.page_count = meta.numberOfPages
            
        gsheet_session.update(doc)


    def log(self, message):
        message = f"{threading.current_thread().name} - {time.strftime('%Y-%m-%d %H:%M:%S')}: {message}"
        log_file = get_in_workdir(Dirs.LOGS, file=f"metadata_extraction_{self.key}.log")
        with open(log_file, "a") as log:
            log.write(f"{message}\n")
        print(message)