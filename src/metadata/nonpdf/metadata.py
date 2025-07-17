from sqlalchemy import select
from utils import decrypt
from prompt import DEFINE_META_PROMPT_NON_PDF_HEADER, DEFINE_META_PROMPT_BODY
from rich import print
from s3 import upload_file, create_session
from utils import read_config, get_in_workdir
from dirs import Dirs
from gemini import request_gemini, create_client
from metadata.schema import Book
import zipfile
import isbnlib
import requests
import re
from monocorpus_models import Document, Session
import time
from google.genai.errors import ClientError, ServerError

model = 'gemini-2.5-pro'

def extract(key):
    attempt = 1
    with Session() as write_session, Session() as read_session:
        predicate = Document.metadata_extraction_method.is_not("gemini-2.5-pro/prompt.v2") & \
            Document.mime_type.is_not('application/pdf') & \
            Document.content_url.is_not(None)
        gemini_client = create_client(key)
        docs = read_session.query(select(Document).where(predicate))
        if not docs:
            print("No documents for processing...")
            return
        
        config = read_config()
        s3lient =  create_session(config)
        for doc in docs:
            try:
                metadata = _extract_metadata(doc, config, gemini_client)
                
                # write metadata to zip
                local_meta_path = get_in_workdir(Dirs.METADATA, file=f"{doc.md5}.zip")
                with zipfile.ZipFile(local_meta_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
                    meta_json = metadata.model_dump_json(indent=None, by_alias=True, exclude_none=True, exclude_unset=True)
                    zf.writestr("metadata.json", meta_json)

                # upload metadata to s3
                meta_key = f"{doc.md5}-meta.zip"
                meta_bucket = config["yandex"]["cloud"]['bucket']['metadata']
                doc.metadata_url = upload_file(local_meta_path, meta_bucket, meta_key, s3lient, skip_if_exists=False)
                doc.metadata_extraction_method = f"{model}/prompt.v2"
                _update_document(doc, metadata, write_session)
                attempt = 1
            except KeyboardInterrupt:
                exit()
            except BaseException as e:
                print(f"Could not extract metadata from doc {doc.md5}: {e}")
                if (isinstance(e, ClientError) and e.code == 429):
                    print("Rate limit exceeded, exiting...")
                    return
                if isinstance(e, ServerError):
                    print("Server error, sleeping for 60 seconds")
                    time.sleep(60)
                if attempt >= 10:
                    raise e
                attempt += 1
            
def _extract_metadata(doc, config, gemini_client):
    print(f"Extracting metadata from document {doc.md5}({doc.ya_public_url})")
    slice = _load_extracted_content(doc, config)
    # prepare prompt
    prompt = _prepare_prompt(slice)
    start_time = time.time()
    response = request_gemini(client=gemini_client, model=model, prompt=prompt, schema=Book, timeout_sec=120)
    # validate response
    if not (raw_response := "".join([ch.text for ch in response if ch.text])):
        print(f"No metadata was extracted from document {doc.md5}")
        return
    else:
        metadata = Book.model_validate_json(raw_response)
    print("Queried gemini", round(time.time() - start_time, 1))
    return metadata
        
def _load_extracted_content(doc, config, first_N=30_000):
    content_zip = get_in_workdir(Dirs.CONTENT, file=f"{doc.md5}.zip")
    content_url = decrypt(doc.content_url, config) if doc.sharing_restricted else doc.content_url
    
    with open(content_zip, "wb") as um_zip, requests.get(content_url, stream=True) as resp:
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=512): 
            um_zip.write(chunk)

    content_dir = get_in_workdir(Dirs.CONTENT)
    with zipfile.ZipFile(content_zip, 'r') as enc_zip:
        content_path = enc_zip.extract(f"{doc.md5}.md", content_dir)
        
    with open(content_path, "r") as f:
        return f.read(first_N)
    
            
def _prepare_prompt(slice):
    prompt = DEFINE_META_PROMPT_NON_PDF_HEADER.format(n=len(slice))
    prompt = [{'text': prompt}]
    prompt.append({'text': DEFINE_META_PROMPT_BODY})
    prompt.append({"text": "Now, extract metadata from the following extraction from the document"})
    prompt.append({"text": slice})
    return prompt
        
def _update_document(doc, meta, gsheet_session):
    doc.publisher = meta.publisher.name if meta.publisher and meta.publisher.name.lower() != 'unknown' else None
    doc.author =  ", ".join([a.name for a in meta.author if a.name.lower() != 'unknown' ]) if meta.author else None
    doc.title = meta.name if meta.name and meta.name.lower() != 'unknown' else None
    doc.language=meta.inLanguage
    doc.genre=", ".join([g.lower() for g in meta.genre if g.lower() != 'unknown']) if meta.genre else None
    doc.translated = bool([c for c in meta.contributor if c.role == 'translator']) if meta.contributor else None
    if (_publish_date := meta.datePublished) and meta.datePublished.lower() != 'unknown':
        if res := re.match(r"^(\d{4})([\d-]*)$", _publish_date.strip()):
            doc.publish_date = res.group(1)
            
    if meta.isbn:
        isbns = set()
        for isbn in meta.isbn:
            if scraped_isbn := isbnlib.get_isbnlike(isbn):
                if scraped_isbn := scraped_isbn.strip():
                    isbns.add(isbnlib.canonical(scraped_isbn).strip())
        if isbns:
            doc.isbn = ", ".join(isbns)
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
        
    start_time = time.time()
    gsheet_session.update(doc)
    print("Updating doc in gsheets", round(time.time() - start_time, 1))