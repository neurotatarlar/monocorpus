from context import Context
from extractor import extract
from utils import read_config, download_file_locally, obtain_documents
from gsheets import upsert
from yadisk_client import YaDisk
from s3 import upload_file, create_session
from google.genai.errors import ClientError
from time import sleep
import os
from monocorpus_models import Document

# todo check Many-Shot In-Context Learning https://aload_test_docrxiv.org/pdf/2404.11018

def extract_content(cli_params):
    config = read_config()
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client:
        predicate = Document.extraction_complete.is_not(True) & Document.full.is_(True) 
        attempt = 0
        for doc in obtain_documents(cli_params, ya_client, predicate):
            try:
                with Context(config, doc, cli_params) as context:
                    if not context.cli_params.force and doc.extraction_complete:
                        context.progress._update(f"Document already processed. Skipping it...")
                        continue
                    
                    if doc.mime_type != "application/pdf":
                        context.progress._update(f"Skipping file: {doc.md5} with mime-type {doc.mime_type}")
                        continue
                    
                    context.progress.operational(f"Downloading file from yadisk")
                    context.local_doc_path = download_file_locally(ya_client, doc)
                    ya_doc_meta = ya_client.get_public_meta(doc.ya_public_url, fields=['md5', 'name', 'public_key', 'resource_id', 'sha256'])
                    context.md5 = ya_doc_meta.md5
                    context.ya_file_name = ya_doc_meta.name
                    context.ya_public_key = ya_doc_meta.public_key
                    context.ya_resource_id = ya_doc_meta.resource_id
                    
                    extract(context)
                    _upload_artifacts(context)
                    _upsert_document(context)
                    context.progress._update(decription=f"[bold green]Processing complete[/ bold green]")
                    attempt = 0
            except KeyboardInterrupt:
                exit()
            except BaseException as e:
                raise e
                # if attempt >= 5:
                #     raise e
                # if isinstance(e, ClientError) and e.code == 429:
                #     print("Sleeping for 60 seconds")
                #     sleep(60)
                # attempt += 1
    
def _upsert_document(context):
    context.progress.operational(f"Updating doc details in gsheets")

    doc = context.doc
    doc.file_name = context.ya_file_name
    doc.ya_public_key=context.ya_public_key
    doc.ya_resource_id=context.ya_resource_id

    doc.content_extraction_method=context.extraction_method
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
        
    if context.local_doc_path:
        doc_bucket = context.config["yandex"]["cloud"]['bucket']['document']
        doc_key = os.path.basename(context.local_doc_path)
        context.remote_doc_url = upload_file(context.local_doc_path, doc_bucket, doc_key, session, skip_if_exists=True)
    
    
