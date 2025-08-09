from monocorpus_models import Document, Session, SCOPES
from yadisk_client import YaDisk
from utils import read_config, download_file_locally, get_in_workdir, encrypt
from monocorpus_models import Document, Session, SCOPES
import mdformat
from dirs import Dirs
from rich import print
import re
from s3 import upload_file, create_session
import os
import zipfile
from rich import print
from sqlalchemy import select
import subprocess
import chardet
import subprocess
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.discovery import build
import io
import shutil
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from queue import Queue, Empty
from yadisk_client import YaDisk
from utils import read_config, obtain_documents, download_file_locally, get_in_workdir
from monocorpus_models import Document, Session
from ebooklib import epub, ITEM_NAVIGATION, ITEM_DOCUMENT, ITEM_IMAGE, ITEM_STYLE, ITEM_FONT, ITEM_COVER, ITEM_UNKNOWN
from bs4 import BeautifulSoup, NavigableString
from markdownify import markdownify as md
import mdformat
from dirs import Dirs
from rich import print
import re
from s3 import upload_file, create_session
import os
import zipfile
from urllib.parse import urlparse
from rich import print
from .epub_extractor import EpubExtractor
from .doc_like_extractor import DocLikeExtractor, to_docx_mime_types, check_encoding_mime_types
import threading
import time
from .pdf_extractor import PdfExtractor


non_pdf_format_types = to_docx_mime_types | \
    check_encoding_mime_types | \
    set(
        [
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 
            "text/markdown",
            'application/epub+zip',
        ]
    )


pdf_format_types = set([
    "application/pdf"
])


skip_pdf = set([])


def extract_content():
    # print("Extracting content of nonpdf documents")
    # predicate = (
    #     Document.content_url.is_(None) &
    #     Document.mime_type.in_(non_pdf_format_types)
    # )
    # _process_non_pdf_by_predicate(predicate)
    
    print("Extracting content of pdf documents")
    predicate = (
        Document.content_url.is_(None) &
        Document.mime_type.in_(non_pdf_format_types)
    )
    _process_pdf_by_predicate(predicate)
    
    
    
class CliParams:
    md5: str
    path: str
    limit: int
    
    def __init__(self):
        self.md5 = None 
        self.path = None
        self.limit = None
    
    
def _process_non_pdf_by_predicate(predicate):
    config = read_config()
    s3client = create_session(config)
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client, Session() as gsheets_session:
        docs = obtain_documents(CliParams(), ya_client, predicate, limit=None)
        if not docs:
            print("No documents for processing...")
            return
        
        gcloud_creds = _get_credentials()
        for doc in docs:
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

            gsheets_session.update(doc)
            
            
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

    
def _process_pdf_by_predicate(predicate, docs_batch_size=72, keys_batch_size=18):
    config = read_config()
    all_keys = config["gemini_api_keys"]["free"]
    for shift in range(0, len(all_keys), keys_batch_size):
        keys_slice = all_keys[shift: shift + keys_batch_size]
        
        tasks_queue = None
        threads = None
        try: 
            while True:
                with Session() as read_session:
                    docs = read_session.query(select(Document).where(predicate).limit(docs_batch_size))

                print(f"Got {len(docs)} docs for content extraction")
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
                        t = threading.Thread(target=PdfExtractor(key))
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