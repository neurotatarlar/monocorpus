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
from rich import print
from s3 import upload_file, create_session
from utils import read_config, get_in_workdir, download_file_locally, obtain_documents, encrypt
from dirs import Dirs
from yadisk_client import YaDisk
import os
from itertools import groupby
import pymupdf
from gemini import gemini_api, create_client
from metadata.schema import Book
import zipfile
import isbnlib
from prompt import DEFINE_META_PROMPT_PDF_HEADER, DEFINE_META_PROMPT_BODY
import requests
import json
import re
from google.genai.errors import ClientError, ServerError
from monocorpus_models import Document, Session
import time



class FromPdfSliceMetadataExtractor:
    
    
    def __init__(self, doc, config, gemini_client, s3lient, model): 
        self.doc = doc
        self.config = config
        self.gemini_client = gemini_client
        self.s3lient = s3lient
        self.model = model
                
                
    def __call__(self):
        if self.doc.md5 in skip:
            print(f"Skipping document {self.doc.md5} as it is in the skip list")
            continue
        # with YaDisk(self.config['yandex']['disk']['oauth_token']) as ya_client: 
