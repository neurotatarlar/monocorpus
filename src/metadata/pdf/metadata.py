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

skip = set([
    "dd713e13dd749131652b7eef5fedf4ac",
    "b2d56b82efc561e9e74f56d8701fd646",
    "913471a88265ebb27423b67477ea5f8a",
])

def extract(cli_params):
    config = read_config()
    attempt = 1
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client, Session() as gsheet_session:
        predicate = (Document.metadata_url.is_(None) | Document.metadata_extraction_method.is_not("gemini-2.5-pro/prompt.v2")) & Document.mime_type.is_('application/pdf')
        # predicate = Document.metadata_url.is_(None) & Document.mime_type.is_('application/pdf')
        
        s3lient =  create_session(config)
        gemini_client = create_client(cli_params.key)
        
        print(f"Extracting metadata from documents with key {cli_params.key} using model {cli_params.model}")
        for doc in obtain_documents(cli_params, ya_client, predicate=predicate):
            if doc.md5 in skip:
                print(f"Skipping document {doc.md5} as it is in the skip list")
                continue
            try:
                _metadata(doc, config, ya_client, gemini_client, s3lient, cli_params, gsheet_session)
                attempt = 1
            except KeyboardInterrupt:
                exit()
            except BaseException as e:
                print(f"Could not extract metadata from doc {doc.md5} key {cli_params.key}: {e}")
                
                if (isinstance(e, ClientError) and e.code == 429):
                    print("Rate limit exceeded, exiting...")
                    return
                if isinstance(e, ServerError):
                    print("Server error, sleeping for 60 seconds")
                    time.sleep(60)
                if attempt >= 30:
                    raise e
                attempt += 1

def _metadata(doc, config, ya_client, gemini_client, s3lient, cli_params, gsheet_session):
    # download doc from yadisk
    start_time = time.time()
    local_doc_path = download_file_locally(ya_client, doc, config)
    print("Downloaded file", round(time.time() - start_time, 1))

    # upload doc to s3
    start_time = time.time()
    doc_bucket = config["yandex"]["cloud"]['bucket']['document']
    doc_key = os.path.basename(local_doc_path)
    remote_doc_url = upload_file(local_doc_path, doc_bucket, doc_key, s3lient, skip_if_exists=True)
    doc.document_url = encrypt(remote_doc_url, config) if doc.sharing_restricted else remote_doc_url
    print("Uploaded file to s3", round(time.time() - start_time, 1))
    
    if doc.mime_type != "application/pdf":
        print(f"Skipping file: {doc.md5} with mime-type {doc.mime_type}")
        return

    print(f"Extracting metadata from document {doc.md5}({doc.ya_public_url})")

    # create a slice of first n and last n pages
    slice_file_path = get_in_workdir(Dirs.DOC_SLICES, doc.md5, file=f"slice-for-meta")
    slice_page_count, original_doc_page_count = _prepare_slices(local_doc_path, slice_file_path, n=8)

    # prepare prompt
    prompt = _prepare_prompt(doc, slice_page_count)

    # send to gemini
    files = {slice_file_path: doc.mime_type}
    start_time = time.time()
    response = gemini_api(client=gemini_client, model=cli_params.model, prompt=prompt, files=files, schema=Book, timeout_sec=180)

    # validate response
    if not (raw_response := "".join([ch.text for ch in response if ch.text])):
        print(f"No metadata was extracted from document {doc.md5}")
        return
    else:
        metadata = Book.model_validate_json(raw_response)
    print("queried gemini", round(time.time() - start_time, 1))

    # write metadata to zip
    local_meta_path = get_in_workdir(Dirs.METADATA, file=f"{doc.md5}.zip")
    with zipfile.ZipFile(local_meta_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        meta_json = metadata.model_dump_json(indent=None, by_alias=True, exclude_none=True, exclude_unset=True)
        zf.writestr("metadata.json", meta_json)

    # upload metadata to s3
    meta_key = f"{doc.md5}-meta.zip"
    meta_bucket = config["yandex"]["cloud"]['bucket']['metadata']
    doc.metadata_url = upload_file(local_meta_path, meta_bucket, meta_key, s3lient, skip_if_exists=False)
    doc.metadata_extraction_method = f"{cli_params.model}/prompt.v2"

    # update metadata in gsheet
    _update_document(doc, metadata, original_doc_page_count, gsheet_session)
            
def _prepare_prompt(doc, slice_page_count):
    prompt = DEFINE_META_PROMPT_PDF_HEADER.format(n=int(slice_page_count / 2),)
    prompt = [{'text': prompt}]
    prompt.append({'text': DEFINE_META_PROMPT_BODY})
    if raw_input_metadata := _load_upstream_metadata(doc):
        prompt.append({
            "text": "📌 In addition to the content of the document, you are also provided with external metadata in JSON format. This metadata comes from other sources and should be treated as valid and trustworthy. Consider it alongside the doc content as if it were extracted from the document itself:"
        })
        prompt.append({
            "text": raw_input_metadata
        })
    prompt.append({"text": "Now, extract metadata from the following document"})
    return prompt
            
def _prepare_slices(pdf_doc, dest_path, n):
    """
    Prepare aux PDF doc with slices of pages of the original document for metadata extraction.
    :param pdf_doc: The PDF document to slice.
    :param n: Number of pages to include from the start.
    :return: The number of pages in the new document and the original document.
    """
    with pymupdf.open(pdf_doc) as pdf_doc, pymupdf.open() as doc_slice:
        pages = list(range(0, pdf_doc.page_count))
        pages = set(pages[:n] + pages[-n:])
        for start, end in list(_ranges(pages)):
            doc_slice.insert_pdf(pdf_doc, from_page=start, to_page=end)
        doc_slice.save(dest_path)
        return doc_slice.page_count, pdf_doc.page_count


def _ranges(_i):
    for _, _b in groupby(enumerate(_i), lambda pair: pair[1] - pair[0]):
        _b = list(_b)
        yield _b[0][1], _b[-1][1]
        
def _estimate_page_dpi(page):
    """
    Estimate DPI of a single PDF page by comparing pixel width to physical width in inches.
    """
    pix = page.get_pixmap(matrix=pymupdf.Matrix(1, 1))  # Render at 72 DPI
    width_inches = page.rect.width / 72  # 1 inch = 72 pt
    dpi = pix.width / width_inches
    return dpi

def _choose_zoom_from_dpi(dpi, target_dpi):
    """
    Choose a zoom factor to match or modestly downscale to target DPI.
    Prevents upscaling if original DPI is lower than target.
    """
    if dpi <= target_dpi:
        return dpi / 72  # Render at original DPI, no upscaling
    else:
        return target_dpi / 72  # Downscale to target DPI

def _update_document(doc, meta, pdf_doc_page_count, gsheet_session):
    doc.publisher = meta.publisher.name if meta.publisher and meta.publisher.name.lower() != 'unknown' else None
    doc.author =  ", ".join([a.name for a in meta.author if a.name.lower() != 'unknown' ]) if meta.author else None
    doc.title = meta.name if meta.name and meta.name.lower() != 'unknown' else None
    doc.language=", ".join(sorted([i.strip() for i in meta.inLanguage.split(",") if i.strip()])) if meta.inLanguage else None
    doc.genre=", ".join([g.lower() for g in meta.genre if g.lower() != 'unknown']) if meta.genre else None
    doc.translated = bool([c for c in meta.contributor if c.role == 'translator']) if meta.contributor else None
    doc.page_count=meta.numberOfPages or None
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
        if isbns:
            doc.isbn = ", ".join(sorted(isbns))
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
        
    if meta.numberOfPages and abs(meta.numberOfPages - pdf_doc_page_count) < 5:
        # if model detected count of pages in the document 
        # and the pages count is not too far from count of pages in pdf file
        doc.page_count = meta.numberOfPages
    else:
        doc.page_count = pdf_doc_page_count
    
    start_time = time.time()
    gsheet_session.update(doc)
    print("updating doc in gsheets", round(time.time() - start_time, 1))
    

def _load_upstream_metadata(doc):
    if not (upstream_metadata_url := doc.upstream_metadata_url):
        return None
    upstream_metadata_zip = get_in_workdir(Dirs.UPSTREAM_METADATA, file=f"{doc.md5}.zip")
    with open(upstream_metadata_zip, "wb") as um_zip, requests.get(upstream_metadata_url, stream=True) as resp:
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=8192): 
            um_zip.write(chunk)
            
    upstream_metadata_unzip = get_in_workdir(Dirs.UPSTREAM_METADATA, doc.md5)
    with zipfile.ZipFile(upstream_metadata_zip, 'r') as enc_zip:
        enc_zip.extractall(upstream_metadata_unzip)
        
    with open(os.path.join(upstream_metadata_unzip, "metadata.json"), "r") as raw_meta:
        _meta = json.load(raw_meta)
        _meta.pop("available_pages", None)
        _meta.pop("doc_card_url", None)
        _meta.pop("download_code", None)
        _meta.pop("doc_url", None)
        _meta.pop("access", None)
        _meta.pop("lang", None)
        return json.dumps(_meta, ensure_ascii=False)