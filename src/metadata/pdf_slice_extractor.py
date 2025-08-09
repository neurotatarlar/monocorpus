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

class FromPdfSliceMetadataExtractor:
    
    
    def __init__(self):
        pass
                
                
    def __call__(self):
        pass