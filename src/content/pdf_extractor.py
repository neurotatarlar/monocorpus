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
import os
from utils import encrypt
from yadisk_client import YaDisk
from utils import read_config, download_file_locally, obtain_documents, get_in_workdir, encrypt
from yadisk_client import YaDisk
from monocorpus_models import Document, Session
from content.pdf.context import Context, Message
from s3 import upload_file, create_session
import os
from gemini import gemini_api, create_client
import pymupdf
from dirs import Dirs
import shutil
from content.pdf.postprocess import postprocess
import re
import zipfile
from prompt import cook_extraction_prompt
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager
import json
from rich import print
from content.pdf.continuity_checker import continue_smoothly
from google.genai.errors import ClientError
from multiprocessing import Manager, Queue
from rich.console import Group
from rich.panel import Panel
import threading
from rich.table import Table
from rich.live import Live
import time
from datetime import timedelta, timezone, datetime
from pydantic import BaseModel
import time
import random


class PdfExtractor:
    
    def __init__(self, key, tasks_queue, config, s3lient, ya_client):
        self.key = key
        self.tasks_queue = tasks_queue
        self.config = config
        self.s3lient = s3lient
        self.ya_client = ya_client
        
    def __call__(self):
        gemini_client = create_client(self.key)
        while True:
            try: 
                doc = self.tasks_queue.get(block=False)
                self.log(f"Extracting content from document {doc.md5}({doc.ya_public_url}) by key {self.key}")
                self._extract(doc)
            except Empty:
                self.log("No tasks for processing, shutting down thread...")
                return
            except ClientError as e:
                if e.code == 429:
                    self.log(f"Key {self.key} exhausted {e}, shutting down thread...") 
                    self.tasks_queue.put(doc)
                return
            except Exception as e:
                import traceback
                self.log(f"Could not extract metadata from doc {doc.md5}: {e} \n{traceback.format_exc()}")
                continue
            
    def _extract(self, doc):
        local_doc_path = download_file_locally(self.ya_client, doc, self.config)
        context = Context(doc, local_doc_path)
        # request latest metadata of the doc in yandex disk
        self._enrich_context(self.ya_client, context)
        
        context.unformatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{self.context.md5}-unformatted.md")
        with pymupdf.open(context.local_doc_path) as pdf_doc, open(self.context.unformatted_response_md, "w") as output:
            context.doc_page_count=pdf_doc.page_count


    def _enrich_context(self, ya_client, context):
        ya_doc_meta = ya_client.get_public_meta(self.context.doc.ya_public_url, fields=['md5', 'name', 'public_key', 'resource_id', 'sha256'])
        context.md5 = ya_doc_meta.md5
        context.ya_file_name = ya_doc_meta.name
        context.ya_public_key = ya_doc_meta.public_key
        context.ya_resource_id = ya_doc_meta.resource_id
        
