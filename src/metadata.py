from gsheets import find_all_without_metadata, upsert
from rich.progress import track
from s3 import upload_file, create_session
from utils import read_config, get_in_workdir, calculate_md5
from dirs import Dirs
from yadisk_client import YaDisk
import os
from itertools import groupby
import pymupdf
from gemini import request_gemini, create_client
from schema import Book
import zipfile
import isbnlib
from prompt import DEFINE_META_PROMPT
import requests

def extract(model):
    config = read_config()
    s3lient =  create_session(config)
    gemini_client = create_client(config)
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client:
        
        # get all docs without extracted metadata
        for doc in track(find_all_without_metadata(), description="Extracting document metadata..."):
            if doc.mime_type != "application/pdf":
                print(f"Skipping file: {doc.md5} with mime-type {doc.mime_type}")
                continue
            
            # download doc from yadisk
            local_doc_path = _download_file_locally(ya_client, doc)
            
            # upload doc to s3
            doc_bucket = config["yandex"]["cloud"]['bucket']['document']
            doc_key = os.path.basename(local_doc_path)
            doc.document_url = upload_file(local_doc_path, doc_bucket, doc_key, s3lient, skip_if_exists=True)
            
            # create a slice of first n and last n pages
            slice_file_path = get_in_workdir(Dirs.DOC_SLICES, file=f"slice-{doc_key}")
            slice_page_count, original_doc_page_count = _prepare_slices(local_doc_path, slice_file_path, n=10)
            
            # prepare prompt
            prompt = _prepare_prompt(doc, slice_page_count)
            
            # send to gemini
            files = {slice_file_path: doc.mime_type}
            response = request_gemini(client=gemini_client, model=model, prompt=prompt, files=files, schema=Book)
            
            # validate response
            metadata = Book.model_validate_json("".join([ch.text for ch in response]))
            
            # write metadata to zip
            local_meta_path = get_in_workdir(Dirs.METADATA, file=f"{doc.md5}.zip")
            with zipfile.ZipFile(local_meta_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
                meta_json = metadata.model_dump_json(indent=None, by_alias=True, exclude_none=True, exclude_unset=True)
                zf.writestr("metadata.json", meta_json)
            
            # upload metadata to s3
            meta_key = f"{doc.md5}-meta.zip"
            meta_bucket = config["yandex"]["cloud"]['bucket']['metadata']
            doc.metadata_url = upload_file(local_meta_path, meta_bucket, meta_key, s3lient, skip_if_exists=False)
            
            # update metadata in gsheet
            _update_document(doc, metadata, original_doc_page_count)
            
def _prepare_prompt(doc, slice_page_count):
    prompt = DEFINE_META_PROMPT.strip().format(n=int(slice_page_count / 2))
    prompt = [{'text': prompt}]
    if raw_input_metadata := _load_upstream_metadata(doc):
        prompt.append({
            "text": "In addition to the content of the document, you are also provided with external metadata in JSON format. This metadata comes from other sources and should be treated as valid and trustworthy. Consider it alongside the doc content as if it were extracted from the document itself:"
        })
        prompt.append({
            "text": raw_input_metadata
        })
        print(prompt)
        exit(0)
    prompt.append({"text": "Now, extract metadata from the following document"})
    return prompt
            
def _download_file_locally(ya_client, doc):
    ext = doc.mime_type.split("/")[-1]
    if ext == "pdf":
        ext = "pdf"
    elif ext == "epub":
        ext = "epub"
    else:
        raise ValueError(f"Unsupported file type: {doc.mime_type}")
    
    local_path=get_in_workdir(Dirs.ENTRY_POINT, file=f"{doc.md5}.{ext}")
    if not (os.path.exists(local_path) and calculate_md5(local_path) == doc.md5):
        with open(local_path, "wb") as f:
            ya_client.download_public(doc.ya_public_url, f)
    return local_path

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
        

def _update_document(doc, meta, pdf_doc_page_count):
    doc.publisher = meta.publisher.name if meta.publisher and meta.publisher.name.lower() != 'unknown' else None
    doc.author =  ", ".join([a.name for a in meta.author if a.name.lower() != 'unknown' ]) if meta.author else None
    doc.title = meta.name if meta.name and meta.name.lower() != 'unknown' else None
    doc.age_limit = f"{meta.suggestedMinAge}+" if meta.suggestedMinAge else None
    doc.summary = meta.description.replace('\n', ' ') if meta.description and meta.description.lower() != 'unknown' else None
    doc.language=meta.inLanguage
    doc.genre=", ".join([g.lower() for g in meta.genre if g.lower() != 'unknown']) if meta.genre else None
    doc.translated = bool([c for c in meta.contributor if c.role == 'translator'])
    doc.page_count=meta.numberOfPages or None
    doc.publish_date = meta.datePublished if meta.datePublished and meta.datePublished.lower() != 'unknown' else None
    doc.edition = meta.bookEdition
    doc.audience = meta.audience if meta.audience and meta.audience.lower() != 'unknown' else None

    if meta.isbn and len(scraped_isbns := isbnlib.get_isbnlike(meta.isbn)) == 1:
        doc.isbn = isbnlib.canonical(scraped_isbns[0])
        
    def _extract_classification(_properties, _expected_names):
        if _properties:
            vals = [p.value.strip().replace(' ', '')  for p in _properties if p.name.strip().upper() in _expected_names and p.value.lower() not in ['unknown', 'неизвестно']]
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
        
    upsert(doc)

def _load_upstream_metadata(doc):
    if not (upstream_metadata_url := doc.upstream_metadata_url):
        return None
    upstream_metadata_zip = get_in_workdir(Dirs.UPSTREAM_METADATA, file=f"{doc.md5}.zip")
    with open(upstream_metadata_zip, "w") as um_zip, requests.get(upstream_metadata_url, stream=True) as resp:
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=8192): 
            um_zip.write(chunk)
            
    upstream_metadata_unzip = get_in_workdir(Dirs.UPSTREAM_METADATA, doc.md5)
    with zipfile.ZipFile(upstream_metadata_zip, 'r') as enc_zip:
        enc_zip.extractall(upstream_metadata_unzip)
        
    with open(os.path.join(upstream_metadata_unzip, "metadata.json"), "r") as raw_meta:
        return raw_meta.read()
            