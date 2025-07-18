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

# todo fix me
excluded_md5s = set([
    '15a6f709f8aef44b642f1cb0891f1a7e', '15d43435deccffe68148e8a1c8e8b45e', '2d8b64bde70efb705a7fbc279b1de214','d05ea94454e87892fba90cbbc81c45e1', '676f0f28c8efef5093e4b0288eaa2db0', 'fa93b9ebacc1680264e945f052549954', '0573113f0a022976d5381d2cec8ab305', '6b46d99568fad3168e2640dea63208db', 'f65985c3f183fdced6adcf57fb1b4b52', '223f363346ae4d3eaefd92241046ae29', '37fcdc1cc9dca80ee1b866fbe8c3acf7', 'e7f4e2e472fc2282fbf495cba90b67a3', '8ea99cbbd07fa3268b4879d69647d633', '346204bed567c1d69ab534b87cbaac29', '32ee190a64eed258eccbe4c8541d7adf',
    '62adcc106dcda1a3ffebc261bfc8f013',
    '392bd422b99fbb33306708bc656b6e06',
    '9380b355fe584bc8f3cad2f5083f173d',
    '1c7a438e1846086eae58b902a2d8f863',
    '29f138e944271d620641cb9be3e2eddb',
    'd7d4e15f89c552a0cf6ba4010db52292',
    'bafc66fc36ca3be69c0442f3738ecf23',
    '80a25ed65edf38dddb785012acff4be1',
    'ace4b159ac53af6b65aad3f378a40c1e',
    'e1c06d85ae7b8b032bef47e42e4c08f9',
    '216ae45bced61af1a2a210c8883c4855',
    'e1c06d85ae7b8b032bef47e42e4c08f9',
    '216ae45bced61af1a2a210c8883c4855',
    '917602d8a123acfd715fd41323cadf14',
    '7dbf0472f43ba285b41dff65fff3f9f5',
    '2513f3c6e08dcebf38ee7f1e80b192e9',
])

def extract(cli_params):
    config = read_config()
    attempt = 1
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client, Session() as gsheet_session:
        predicate = Document.metadata_url.is_(None) & Document.mime_type.is_('application/pdf')
        
        s3lient =  create_session(config)
        gemini_client = create_client(cli_params.key)
        
        for doc in obtain_documents(cli_params, ya_client, predicate=predicate):
            if doc.md5 in excluded_md5s:
                continue
            try:
                _metadata(doc, config, ya_client, gemini_client, s3lient, cli_params, gsheet_session)
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
    slice_page_count, original_doc_page_count = _prepare_slices(local_doc_path, slice_file_path, n=5)

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
        

def _update_document(doc, meta, pdf_doc_page_count, gsheet_session):
    doc.publisher = meta.publisher.name if meta.publisher and meta.publisher.name.lower() != 'unknown' else None
    doc.author =  ", ".join([a.name for a in meta.author if a.name.lower() != 'unknown' ]) if meta.author else None
    doc.title = meta.name if meta.name and meta.name.lower() != 'unknown' else None
    doc.language=meta.inLanguage
    doc.genre=", ".join([g.lower() for g in meta.genre if g.lower() != 'unknown']) if meta.genre else None
    doc.translated = bool([c for c in meta.contributor if c.role == 'translator']) if meta.contributor else None
    doc.page_count=meta.numberOfPages or None
    if (_publish_date := meta.datePublished) and meta.datePublished.lower() != 'unknown':
        if res := re.match(r"^(\d{4})([\d-]*)$", _publish_date.strip()):
            doc.publish_date = res.group(1)
    
    if meta.isbn:
        isbns = set()
        for isbn in meta.isbn:
            if scraped_isbn := isbnlib.get_isbnlike(isbn):
                for scraped_isbn in scraped_isbn:
                    if scraped_isbn := scraped_isbn.strip():
                        isbns.add(isbnlib.canonical(scraped_isbn))
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