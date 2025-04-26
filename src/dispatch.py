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

# todo experiment with system prompt
# todo check one-shot approach
# todo check Many-Shot In-Context Learning https://aload_test_docrxiv.org/pdf/2404.11018
# todo deduplicate by isbn

def extract_content(public_url, cli_params):
    config = read_config()
    try:
        with Context(config, public_url, cli_params) as context:
            _download_file_locally(context)
            _load_document(context)
            if not context.cli_params.force and context.gsheet_doc.extraction_complete:
                context.progress._update(f"Document already processed, skipping...")
                return
            extract(context)
            _upload_artifacts(context)
            _upsert_document(context)
    except KeyboardInterrupt:
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
            context.progress.operational(f"Downloading doc from yadisk")
            with open(context.local_doc_path, "wb") as f:
                client.download_public(context.ya_public_url, f)
                
def _load_document(context):
    context.progress.operational(f"Requesting doc details from gsheets")
    context.gsheet_doc = find_by_md5(context.md5) or Document(md5=context.md5, mime_type="application/pdf")

def _upsert_document(context):
    context.progress.operational(f"Updating doc details in gsheets")

    doc = context.gsheet_doc
    doc.file_name = context.ya_file_name
    doc.ya_public_key=context.ya_public_key
    doc.ya_public_url=context.ya_public_url
    doc.ya_resource_id=context.ya_resource_id

    doc.extraction_method=context.extraction_method
    doc.document_url = context.remote_doc_url
    doc.content_url = context.remote_content_url
    doc.extraction_complete=True
        
    upsert(doc)
    
def _upload_artifacts(context):
    context.progress.operational(f"Uploading artifacts to object storage")
                
    session = create_session(context.config)
    
    if context.local_content_path:
        content_key = f"{context.md5}-content.zip"
        content_bucket = context.config["yandex"]["cloud"]['bucket']['content']
        context.remote_content_url = upload_file(context.local_content_path, content_bucket, content_key, session)
    
    
