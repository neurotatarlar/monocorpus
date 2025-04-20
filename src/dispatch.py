from context import Context
from rich import print
from extractor import extract
from utils import read_config, calculate_md5, get_in_workdir
from dirs import Dirs
from gsheets import find_by_md5, upsert
from monocorpus_models import Document
from yadisk_client import YaDisk
import os
import isbnlib
from s3 import upload_file, create_session

# todo sometimes we already have some piece of metadata, need to provide it to Gemini as well
# todo experiment with system prompt
# todo check one-shot approach
# todo check Many-Shot In-Context Learning https://arxiv.org/pdf/2404.11018
# todo deduplicate by isbn
# todo support slocing
# todo support force
# todo fix progress

def extract_content(public_url):
    config = read_config()
    try:
        with Context(config, public_url) as context:
            _download_file_locally(context)
            _load_document(context)
            if context.gsheet_doc.extraction_complete:
                print("Document's content already extracted")
                return
            extract(context)
            _upload_artifacts(context)
            _upsert_document(context)
    except KeyboardInterrupt:
        print("\nStopping...")
        return
    except BaseException as e:
        raise e
    
def _download_file_locally(context):
    with YaDisk(context.config['yandex']['disk']['oauth_token']) as client:
        ya_doc_meta = client.get_public_meta(context.ya_public_url, fields=['md5', 'name', 'public_key', 'resource_id', 'sha256'])
        context.md5 = ya_doc_meta['md5']
        context.ya_file_name = ya_doc_meta['name']
        context.ya_public_key = ya_doc_meta['public_key']
        context.ya_resource_id = ya_doc_meta['resource_id']

        context.local_doc_path=get_in_workdir(Dirs.ENTRY_POINT, file=f"{context.md5}.pdf")
        if not (os.path.exists(context.local_doc_path) and calculate_md5(context.local_doc_path) == context.md5):
            context.progress.main(f"Downloading doc from yadisk")
            with open(context.local_doc_path, "wb") as f:
                client.download_public(context.ya_public_url, f)
                
def _load_document(context):
    context.progress.main(f"Requesting doc details from gsheets")
    context.gsheet_doc = find_by_md5(context.md5) or Document(md5=context.md5, mime_type="application/pdf")

def _upsert_document(context):
    context.progress.main(f"Updating doc details in gsheets")

    meta, doc = context.metadata, context.gsheet_doc
    doc.file_name = context.ya_file_name
    doc.publisher = meta.publisher.name if meta.publisher else None
    doc.author =  ", ".join([a.name for a in meta.author]) if meta.author else None
    doc.title = meta.name
    doc.age_limit = meta.suggestedAge
    doc.summary = meta.description 
    doc.language=meta.inLanguage
    doc.genre=", ".join(meta.genre) if meta.genre else None
    doc.translated = True if meta.translator else None
    doc.page_count=meta.numberOfPages
    doc.publish_date=meta.datePublished
    doc.ya_public_key=context.ya_public_key
    doc.ya_public_url=context.ya_public_url
    doc.ya_resource_id=context.ya_resource_id
    
    if meta.isbn and len(scraped_isbns := isbnlib.get_isbnlike(meta.isbn)) == 1:
        doc.isbn = isbnlib.canonical(scraped_isbns[0])
        
    if meta.additionalProperty and len(bbc := [p.value.strip() for p in meta.additionalProperty if p.name.strip().upper() in ["ББК", "BBC"]]) == 1:
        doc.bbc = bbc[0]
        
    if meta.additionalProperty and len(udc := [p.value.strip() for p in meta.additionalProperty if p.name.strip().upper() in ["УДК", "UDC"]]) == 1:
        doc.udc = udc[0]
        
    doc.edition=meta.bookEdition
    doc.audience=meta.audience
    doc.extraction_method=context.extraction_method
    doc.extraction_complete=True
    
    if meta.numberOfPages and abs(meta.numberOfPages - context.doc_page_count) < 5:
        # if model detected count of pages in the document 
        # and the pages count is not too far from count of pages in pdf file
        doc.page_count = meta.numberOfPages
    else:
        doc.page_count = context.doc_page_count
        
    doc.document_url = context.remote_doc_url
    doc.content_url = context.remote_content_url
    doc.metadata_url = context.remote_meta_url
    
    upsert(doc)
    
def _upload_artifacts(context):
    context.progress.main(f"Uploading artifacts to object storage")
                
    session = create_session(context.config)
    
    if context.local_doc_path:
        doc_bucket = context.config["yandex"]["cloud"]['bucket']['document']
        doc_key = f"{context.md5}{os.path.splitext(context.local_doc_path)[1]}"
        context.remote_doc_url = upload_file(context.local_doc_path, doc_bucket, doc_key, session, skip_if_exists=True)
    
    if context.local_meta_path:
        meta_key = f"{context.md5}-meta.zip"
        meta_bucket = context.config["yandex"]["cloud"]['bucket']['metadata']
        context.remote_meta_url = upload_file(context.local_meta_path, meta_bucket, meta_key, session)
    
    if context.local_content_path:
        content_key = f"{context.md5}-content.zip"
        content_bucket = context.config["yandex"]["cloud"]['bucket']['content']
        context.remote_content_url = upload_file(context.local_content_path, content_bucket, content_key, session)
    
    
