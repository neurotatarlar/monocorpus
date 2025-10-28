"""
Content Extraction Dispatcher Module

This module orchestrates the extraction of content from various document types (PDF, EPUB, DOCX)
using multiple processing strategies and parallel execution. It handles document downloading,
processing, and uploading to cloud storage while managing API rate limits and error cases.

Key Components:
1. Document Processing
   - PDF extraction using Gemini API
   - EPUB extraction
   - Office documents (DOCX, etc.) processing
   - Markdown formatting and compression

2. Cloud Integration
   - Yandex.Disk document source
   - S3 storage for processed content
   - Google Cloud authentication for additional processing

3. Parallel Processing
   - Multi-threaded extraction for PDFs
   - API key rotation and rate limit handling
   - Queue-based task distribution

4. State Management
   - Tracking of unprocessable documents
   - Management of rate-limited API keys
   - Persistent state for interrupted operations

Classes:
    Channel: Manages shared state and persistence for document processing
        - Tracks API key usage and rate limits
        - Maintains lists of unprocessable/repairable documents
        - Handles thread-safe state updates

Functions:
    extract_content(cli_params): Main entry point for content extraction
    _process_non_pdf(cli_params): Handles extraction from EPUB and Office documents
    _process_pdf(cli_params): Orchestrates parallel PDF content extraction
    _upload_artifacts_to_s3(...): Handles upload of processed content to S3
    _get_credentials(): Manages Google Cloud authentication

Configuration:
    - Requires configuration for:
        - Yandex.Disk OAuth token
        - S3 credentials
        - Gemini API keys
        - Google Cloud credentials

Error Handling:
    - Graceful shutdown on interruption
    - API rate limit management
    - Document processing failure tracking
    - State persistence for recovery

Usage:
    The module is typically invoked through CLI commands that specify:
    - Document selection (MD5 hash or path)
    - Batch size and worker count
    - Processing limits and filters
"""
from yadisk_client import YaDisk
import mdformat
from dirs import Dirs
from rich import print
from s3 import upload_file, create_session
import os
import zipfile
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from queue import Queue
from utils import read_config, obtain_documents, download_file_locally, get_in_workdir, encrypt, load_expired_keys, dump_expired_keys, get_session
from .epub_extractor import EpubExtractor
from .doc_like_extractor import DocLikeExtractor, to_docx_mime_types, check_encoding_mime_types
import threading
import time
from .pdf_extractor import PdfExtractor
import random
from models import Document
from rich.progress import track

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


non_pdf_format_types = to_docx_mime_types | \
    check_encoding_mime_types | \
    set(
        [
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 
            "text/markdown",
            'application/epub+zip',
        ]
    )

def extract_content(cli_params):
    _process_non_pdf(cli_params)
    
    _process_pdf(cli_params)
    
 
def _process_non_pdf(cli_params):
    print("Extracting content of nonpdf documents")
    predicate = (
        Document.content_url.is_(None) &
        Document.mime_type.in_(non_pdf_format_types)
    )
    config = read_config()
    s3client = create_session(config)
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client:
        with get_session() as session:
            docs = list(obtain_documents(cli_params, ya_client, predicate, session=session))
        if not docs:
            print("No documents for processing...")
            return
        
        print(f"Got {len(docs)} docs for content extraction")
        
        gcloud_creds = _get_credentials()
        for doc in track(docs, description="Processing documents..."):
            print(f"Extracting content from file {doc.md5}({doc.ya_public_url})")
            local_doc_path = download_file_locally(ya_client, doc, config)
            if doc.mime_type == 'application/epub+zip':
                content = EpubExtractor(doc, local_doc_path, config, s3client).extract()
            else:
                content = DocLikeExtractor(doc, local_doc_path, config, s3client, gcloud_creds).extract()
            
            formatted_content = mdformat.text(
                content,
                codeformatters=(),
                extensions=["toc", "footnote"],
                options={"wrap": "keep", "number": "keep", "validate": True, "end_of_line": "lf"},
            )
            formatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{doc.md5}-formatted.md")
            with open(formatted_response_md, 'w') as f:
                f.write(formatted_content)
                
            _upload_artifacts_to_s3(doc, formatted_response_md, local_doc_path, config, s3client)

            with get_session() as session:
                session.merge(doc)
                session.commit()
            
            
def _upload_artifacts_to_s3(doc, formatted_response_md, local_doc_path, config, s3lient):        
    content_key = f"{doc.md5}.zip"
    content_bucket = config["yandex"]["cloud"]['bucket']['content']
    local_content_path = get_in_workdir(Dirs.CONTENT, file=f"{doc.md5}.zip")
    with zipfile.ZipFile(local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.write(arcname=f"{doc.md5}.md", filename=formatted_response_md)
    doc.content_url = upload_file(local_content_path, content_bucket, content_key, s3lient)
    
    doc_bucket = config["yandex"]["cloud"]['bucket']['document']
    doc_key = os.path.basename(local_doc_path)
    remote_doc_url = upload_file(local_doc_path, doc_bucket, doc_key, s3lient, skip_if_exists=True)
    doc.document_url = encrypt(remote_doc_url, config) if doc.sharing_restricted else remote_doc_url


def _get_credentials():
    token_file = "personal_token.json"
    
    if os.path.exists(token_file):
        return Credentials.from_authorized_user_file(token_file, SCOPES)
    
    flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
    creds = flow.run_local_server(port=0)
    with open(token_file, 'w') as f:
        f.write(creds.to_json())
    return Credentials.from_authorized_user_file(token_file, SCOPES)

class Channel:
    
    def __init__(self):
        self.lock = threading.Lock()
        self.exceeded_keys_set = load_expired_keys()
        self.unprocessable_docs, self.repairable_docs = self._load_unprocessable_docs()
        
    def get_all_unprocessable_docs(self):
        return self.unprocessable_docs | self.repairable_docs
    
    def dump(self):
        dump_expired_keys(self.exceeded_keys_set)
        self._dump_to_file("unprocessables", "unprocessables.txt", self.unprocessable_docs)
        self._dump_to_file("unprocessables", "repairables.txt", self.repairable_docs)
            

    def _load_unprocessable_docs(self, dir = "unprocessables"):
        return self._load_file(dir, "unprocessables.txt"), self._load_file(dir, "repairables.txt")
    
    
    def _load_file(self, dir, file_name):
        file = os.path.join(dir, file_name)
        if os.path.exists(file):
            with open(file, "r") as f:
                return set([l.strip() for l in f.readlines()])
        else: 
            return set()
        
        
    def _dump_to_file(self, dir, file_name, items):
        os.makedirs(dir, exist_ok=True)
        file = os.path.join(dir, file_name)
        with open(file, "w") as f:
            f.write("\n".join([l.strip() for l in items]))
            
    
    def add_exceeded_key(self, key):
        with self.lock:
            self.exceeded_keys_set.add(key)
            dump_expired_keys(self.exceeded_keys_set)
    
            
    def add_unprocessable_doc(self, md5):
        with self.lock:
            self.unprocessable_docs.add(md5)
            self._dump_to_file("unprocessables", "unprocessables.txt", self.unprocessable_docs)
            
    
    def add_repairable_doc(self, md5):
        with self.lock:
            self.repairable_docs.add(md5)
            self._dump_to_file("unprocessables", "repairables.txt", self.repairable_docs)

    
def _process_pdf(cli_params):
    config = read_config()
    stop_event = threading.Event()
    print("Extracting content of pdf documents")
    
    while not stop_event.is_set():
        tasks_queue = None
        threads = None
        
        channel = Channel()
        predicate = (
            Document.content_url.is_(None) &
            (Document.mime_type ==  "application/pdf") &
            (Document.language == "tt-Cyrl") &
            (Document.full == True) & 
            (
                (Document.md5 == cli_params.md5) if cli_params.md5 else Document.md5.not_in(channel.get_all_unprocessable_docs())
            )
        )
        
        try:
            available_keys =  list(set(config["gemini_api_keys"]) - channel.exceeded_keys_set)
            random.shuffle(available_keys)
            keys_slice = available_keys[:cli_params.workers]
            if not keys_slice:
                print("No keys available, exiting...")
                return
            else:
                print(f"Available keys: {available_keys}, Total keys: {config['gemini_api_keys']}, Exceeded keys: {channel.exceeded_keys_set}, Extracting with keys: {keys_slice}")
            
            with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client:
                with get_session() as session:
                    docs = list(obtain_documents(cli_params, ya_client, predicate, limit=cli_params.batch_size, session=session))
                    
                if not docs:
                    print("No docs for processing, exiting...")
                    return

                print(f"Got {len(docs)} docs for content extraction")
                tasks_queue = Queue(maxsize=len(docs))
                for doc in docs:
                    tasks_queue.put(doc)
                    
                if tasks_queue.empty():
                    print("No documents for processing...")
                    return
                else:
                    print(f"Got {tasks_queue.qsize()} docs in tasks queue")
                
                s3lient = create_session(config)
                    
                threads = []
                for num in range(min(len(keys_slice), len(docs))):
                    key = keys_slice[num]
                    t = threading.Thread(target=PdfExtractor(key, tasks_queue, config, s3lient, ya_client, channel, stop_event))
                    t.start()
                    threads.append(t)
                    time.sleep(5)  # slight delay to avoid overwhelming the API with requests

            # waiting for workers shutdown gracefully
            for t in threads:
                t.join()
                
            channel.dump()
        except KeyboardInterrupt:
            print("Interrupted, shutting down workers...")
            stop_event.set()
            if tasks_queue:
                tasks_queue.queue.clear()  # Clear the queue to stop workers
            if threads:
                for t in threads:
                    t.join(timeout=60*10)
            channel.dump()
            return