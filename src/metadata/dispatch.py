from sqlalchemy import select
from utils import decrypt
from prompt import DEFINE_META_PROMPT_NON_PDF_HEADER, DEFINE_META_PROMPT_BODY
from rich import print
from s3 import upload_file, create_session
from utils import read_config, get_in_workdir
from dirs import Dirs
from gemini import gemini_api, create_client
from metadata.schema import Book
import zipfile
import isbnlib
import requests
import re
from monocorpus_models import Document, Session
import time
from google.genai.errors import ClientError, ServerError
from concurrent.futures import ProcessPoolExecutor, as_completed
from queue import Queue, Empty
import threading

model = 'gemini-2.5-pro'

def extract_metadata():
    # print("Processing documents without metadata")
    # predicate = Document.metadata_url.is_(None) & Document.content_url.is_not(None)
    # _process_by_predicate(predicate)
    
    predicate = Document.metadata_extraction_method.is_not("gemini-2.5-pro/prompt.v2") & Document.content_url.is_not(None) & Document.mime_type.is_not('application/pdf')
    print("Processing documents with older metadata extraction method...")
    _process_by_predicate(predicate)
    
def _process_by_predicate(predicate):
    with Session() as read_session:
        docs = read_session.query(select(Document).where(predicate))
    if not docs:
        print("No documents for processing...")
        return
    print(f"Found {len(docs)} documents for metadata extraction")
    config = read_config()
    s3lient =  create_session(config)
    tasks_queue = Queue(maxsize=len(docs))
    for doc in docs:
        tasks_queue.put(doc)
        
    threads = []
    for key in config["gemini_api_keys"]["free"][:16]:
        t = threading.Thread(target=MetadataExtractionWorker(key, tasks_queue, config, s3lient))
        t.start()
        threads.append(t)
        time.sleep(5)  # slight delay to avoid overwhelming the API with requests

    try: 
    # Shutdown workers gracefully
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("Interrupted, shutting down workers...")
        tasks_queue.queue.clear()  # Clear the queue to stop workers
        for t in threads:
            t.join(timeout=60)
        
class KeyAwareWorker:
    def __init__(self, gemini_api_key, tasks_queue, config, s3lient):
        self.key = gemini_api_key
        self.tasks_queue = tasks_queue
        self.config = config
        self.s3lient = s3lient

    def __call__(self, *args, **kwds):
        pass
    
    def log(self, message):
        message = f"{threading.current_thread().name} - {time.strftime('%Y-%m-%d %H:%M:%S')}: {message}"
        log_file = get_in_workdir(Dirs.LOGS, file=f"metadata_extraction_{self.key}.log")
        with open(log_file, "a") as log:
            log.write(f"{message}\n")
        print(message)
    
class MetadataExtractionWorker(KeyAwareWorker):
    def __call__(self):
        try:
            gemini_client = create_client(self.key)
            while True:
                doc = self.tasks_queue.get(block=False)
                self.log(f"Extracting metadata from document {doc.md5}({doc.ya_public_url}) by key {self.key}")
                
                if doc.content_url:
                    metadata = FromTextMetadataExtractor(doc, self.config, gemini_client, self.s3lient)()
                elif doc.mime_type == 'application/pdf':
                    continue
                    # metadata = _extract_by_pdf_slice()
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
                meta_key = f"{doc.md5}-meta.zip"
                meta_bucket = self.config["yandex"]["cloud"]['bucket']['metadata']
                doc.metadata_url = upload_file(local_meta_path, meta_bucket, meta_key, self.s3lient, skip_if_exists=False)
                doc.metadata_extraction_method = f"{model}/prompt.v2"
                self._upload_artifacts_to_s3(doc, local_meta_path)
                with Session() as gsheet_session:
                    self._update_document(doc, metadata, gsheet_session)
                self.log(f"Metadata extracted and uploaded for document {doc.md5}({doc.ya_public_url})")
        except Empty:
            return
        except BaseException as e:
            import traceback
            self.log(f"Could not extract metadata from doc {doc.md5}: {e} \n{traceback.format_exc()}")
            
    def _upload_artifacts_to_s3(self, doc, local_meta_path):
        meta_key = f"{doc.md5}-meta.zip"
        meta_bucket = self.config["yandex"]["cloud"]['bucket']['metadata']
        doc.metadata_url = upload_file(local_meta_path, meta_bucket, meta_key, self.s3lient, skip_if_exists=False)

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
            
        gsheet_session.update(doc)


class FromTextMetadataExtractor:
    def __init__(self, doc, config, gemini_client, s3lient):
        self.doc = doc
        self.config = config
        self.gemini_client = gemini_client
        self.s3lient = s3lient
                
    def __call__(self):
        slice = self._load_extracted_content()
        # prepare prompt
        prompt = self._prepare_prompt(slice)
        response = gemini_api(client=self.gemini_client, model=model, prompt=prompt, schema=Book, timeout_sec=120)
        # validate response
        if not (raw_response := "".join([ch.text for ch in response if ch.text])):
            print(f"No metadata was extracted from document {self.doc.md5}")
            return
        else:
            metadata = Book.model_validate_json(raw_response)
        return metadata
    
    
    def _load_extracted_content(self, first_N=30_000):
        content_zip = get_in_workdir(Dirs.CONTENT, file=f"{self.doc.md5}.zip")
        content_url = decrypt(self.doc.content_url, self.config) if self.doc.sharing_restricted else self.doc.content_url
        
        with open(content_zip, "wb") as um_zip, requests.get(content_url, stream=True) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=512): 
                um_zip.write(chunk)

        content_dir = get_in_workdir(Dirs.CONTENT)
        with zipfile.ZipFile(content_zip, 'r') as enc_zip:
            content_path = enc_zip.extract(f"{self.doc.md5}.md", content_dir)
            
        with open(content_path, "r") as f:
            return f.read(first_N)
        
    def _prepare_prompt(self, slice):
        prompt = DEFINE_META_PROMPT_NON_PDF_HEADER.format(n=len(slice))
        prompt = [{'text': prompt}]
        prompt.append({'text': DEFINE_META_PROMPT_BODY})
        prompt.append({"text": "Now, extract metadata from the following extraction from the document"})
        prompt.append({"text": slice})
        return prompt
    
    
class FromPdfSliceMetadataExtractor:
    def __init__(self, doc, config, gemini_client, s3lient):
        self.doc = doc
        self.config = config
        self.gemini_client = gemini_client
        self.s3lient = s3lient
                
    def __call__(self):
        print(f"Extracting metadata from document {self.doc.md5}({self.doc.ya_public_url})")
        slice = self._load_extracted_content(self.doc, self.config)
        # prepare prompt
        prompt = self._prepare_prompt(slice)
        start_time = time.time()
        response = gemini_api(client=self.gemini_client, model=model, prompt=prompt, schema=Book, timeout_sec=120)
        # validate response
        if not (raw_response := "".join([ch.text for ch in response if ch.text])):
            print(f"No metadata was extracted from document {self.doc.md5}")
            return
        else:
            metadata = Book.model_validate_json(raw_response)
        print("Queried gemini", round(time.time() - start_time, 1))
        return metadata
    
    
    def _load_extracted_content(self, first_N=30_000):
        content_zip = get_in_workdir(Dirs.CONTENT, file=f"{self.doc.md5}.zip")
        content_url = decrypt(self.doc.content_url, self.config) if self.doc.sharing_restricted else self.doc.content_url
        
        with open(content_zip, "wb") as um_zip, requests.get(content_url, stream=True) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=512): 
                um_zip.write(chunk)

        content_dir = get_in_workdir(Dirs.CONTENT)
        with zipfile.ZipFile(content_zip, 'r') as enc_zip:
            content_path = enc_zip.extract(f"{self.doc.md5}.md", content_dir)
            
        with open(content_path, "r") as f:
            return f.read(first_N)
        
    def _prepare_prompt(self, slice):
        prompt = DEFINE_META_PROMPT_NON_PDF_HEADER.format(n=len(slice))
        prompt = [{'text': prompt}]
        prompt.append({'text': DEFINE_META_PROMPT_BODY})
        prompt.append({"text": "Now, extract metadata from the following extraction from the document"})
        prompt.append({"text": slice})
        return prompt
        

        


    
        
        
    