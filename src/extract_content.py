from utils import read_config, download_file_locally, obtain_documents, get_in_workdir
from yadisk_client import YaDisk
from monocorpus_models import Document
from context import Context
from time import sleep
from s3 import upload_file, create_session
import os
from gemini import request_gemini, create_client
import pymupdf
from itertools import batched
from dirs import Dirs
from schema import ExtractionResult
import shutil
from postprocess import postprocess
import re
import zipfile
from prompt import cook_extraction_prompt
from google.genai.errors import ClientError
from monocorpus_models import Session


def extract_structured_content(cli_params):
    config = read_config()
    gemini_client = create_client("free")
    attempt = 0
    predicate = Document.extraction_complete.is_not(True) & Document.full.is_(True) & Document.language.is_("tt-Cyrl") & Document.mime_type.is_('application/pdf')
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client, Session() as gsheet_session:
        for doc in obtain_documents(cli_params, ya_client, predicate = predicate, limit = 20):
            try:
                with Context(config, doc, cli_params, gsheet_session) as context:
                    if not context.cli_params.force and doc.extraction_complete:
                        context.progress._update(f"Document already processed. Skipping it...")
                        continue
                        
                    if doc.mime_type != "application/pdf":
                        context.progress._update(f"Skipping file: {doc.md5} with mime-type {doc.mime_type}")
                        continue
                    
                    _take_doc(context, ya_client, gemini_client)
                attempt = 0
            except KeyboardInterrupt:
                exit()
            except BaseException as e:
                print(e)
                if attempt >= 5:
                    raise e
                if isinstance(e, ClientError) and e.code == 429:
                    print("Sleeping for 60 seconds")
                    sleep(60)
                attempt += 1
        print("Sleeping for 10 seconds")
        exit()
        
                
def _take_doc(context, ya_client, gemini_client):
    context.progress.operational(f"Downloading file from yadisk")
    context.local_doc_path = download_file_locally(ya_client, context.doc)
    
    # request latest metadata of the doc in yandex disk
    ya_doc_meta = ya_client.get_public_meta(context.doc.ya_public_url, fields=['md5', 'name', 'public_key', 'resource_id', 'sha256'])
    context.md5 = ya_doc_meta.md5
    context.ya_file_name = ya_doc_meta.name
    context.ya_public_key = ya_doc_meta.public_key
    context.ya_resource_id = ya_doc_meta.resource_id
    
    with pymupdf.open(context.local_doc_path) as pdf_doc:
        _extract_content(context, pdf_doc, gemini_client)
        postprocessed = postprocess(context)
        
        context.formatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-formatted.md")
        with open(context.formatted_response_md, 'w') as f:
            f.write(postprocessed)
            
        context.local_content_path = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.zip")
        with zipfile.ZipFile(context.local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
            zf.write(arcname=f"{context.md5}.md", filename=context.formatted_response_md)
        
        context.extraction_method = f"{context.cli_params.model}/{context.cli_params.batch_size}/pdfinput"
        context.doc_page_count=pdf_doc.page_count
        
    _upload_artifacts(context)
    _upsert_document(context)
    context.progress._update(decription=f"[bold green]Processing complete[/ bold green]")
    
def _extract_content(context, pdf_doc, gemini_client):
    batch_size = context.cli_params.batch_size
    iter = list(batched(range(0, pdf_doc.page_count)[context.cli_params.page_slice], batch_size))

    prev_chunk_tail = None
    next_footnote_num = 1
    chunked_results_dir = get_in_workdir(Dirs.CHUNKED_RESULTS, context.md5)
    chunk_result_paths = []
    
    context.unformatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-unformatted.md")
    with open(context.unformatted_response_md, "w") as output:
        for chunk in context.progress.track_extraction(iter, f"Extracting..."):
            _from = chunk[0]
            _to = chunk[-1]
            chunk_result_complete_path = os.path.join(chunked_results_dir, f"chunk-{_from}-{_to}")
            if os.path.exists(chunk_result_complete_path):
                print(f"chunk {_from}-{_to} is already extracted")
                with open(chunk_result_complete_path, "r") as f:
                    content = ExtractionResult.model_validate_json(f.read()).content
            else:
                # create a pdf doc what will contain a slice of original pdf doc
                slice_file_path = _create_doc_clice(_from, _to, pdf_doc, context.md5)
                
                # prepare prompt
                prompt = cook_extraction_prompt(_from, _to, next_footnote_num, prev_chunk_tail, gemini_client)
            
                # request gemini
                files = {slice_file_path: "application/pdf"}
                response = request_gemini(client=gemini_client, model=context.cli_params.model, prompt=prompt, files=files, schema=ExtractionResult)
            
                # write result into file
                chunk_result_incomplete_path = chunk_result_complete_path + ".part"
                with open(chunk_result_incomplete_path, "w") as f:
                    for p in response:
                        if text := p.text:
                            f.write(text)
                            
                context.tokens.append(p.usage_metadata.total_token_count)

                # validating schema
                with open(chunk_result_incomplete_path, "r") as f:
                    content = ExtractionResult.model_validate_json(f.read()).content
                    
                # "mark" batch as extracted by renaming file
                shutil.move(chunk_result_incomplete_path, chunk_result_complete_path)
            
            output.write(content)
            output.flush()
            
            # define number of last footnote detected in the document
            footnote_counters = re.findall(r"\[\^(\d+)\]:", content)
            last_footnote_num = max(map(int, footnote_counters)) if footnote_counters else 0
            next_footnote_num = max(next_footnote_num, last_footnote_num + 1)
            prev_chunk_tail = content[-300:]

            chunk_result_paths.append(chunk_result_complete_path)

def _create_doc_clice(_from, _to, pdf_doc, md5):
    slice_file_path = get_in_workdir(Dirs.DOC_SLICES, md5, file=f"slice-{_from}-{_to}.pdf")
    if not os.path.exists(slice_file_path):
        doc_slice = pymupdf.open()
        doc_slice.insert_pdf(pdf_doc, from_page=_from, to_page=_to)
        doc_slice.save(slice_file_path)
    return slice_file_path
    
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
        
    context.gsheets_session.update(doc)