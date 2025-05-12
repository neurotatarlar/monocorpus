from utils import read_config, download_file_locally, obtain_documents, get_in_workdir
from yadisk_client import YaDisk
from monocorpus_models import Document, Session
from context import Context
from s3 import upload_file, create_session
import os
from gemini import request_gemini, create_client
import pymupdf
from more_itertools import batched
from dirs import Dirs
from schema import ExtractionResult
import shutil
from postprocess import postprocess
import re
import zipfile
from prompt import cook_extraction_prompt
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager, Value, Lock
from context import Context
from gemini import create_client
from utils import download_file_locally
import json
from rich import print
from continuity_checker import continue_smoothly
from google.genai.errors import ClientError
import time


# todo upload intermidiate results to s3?
# todo change seed in case of error?
# todo chunk and footnote
ATTEMPTS = 1

def extract_structured_content(cli_params):
    print(f'about to extract content with params => {", ".join([f"{k}: {v}" for k,v in cli_params.__dict__.items() if v])}')
    config = read_config()
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client, Manager() as manager:
        
        failure_count = manager.Value('i', 0)
        lock = manager.Lock()

        predicate = Document.extraction_complete.is_not(True) & Document.full.is_(True) & Document.language.is_("tt-Cyrl") & Document.mime_type.is_('application/pdf')
        docs = obtain_documents(cli_params, ya_client, predicate, limit=cli_params.limit)
        
        with ProcessPoolExecutor(max_workers=cli_params.workers) as executor:
            futures = {executor.submit(__task, config, doc, cli_params, failure_count, lock): doc for doc in docs}
            for future in as_completed(futures):
                if failure_count.value >= ATTEMPTS:
                    print("[red]Too many consecutive failures. Exiting all processing.[/red]")
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                
                try:
                    _ = future.result()
                except Exception as e:
                    print(f"Unhandled exception in main loop: {e}")
                
def __task(config, doc, cli_params, failure_count, lock):
    try:
        gemini_client = create_client(cli_params.tier)
        with Session() as gsheets_session, \
            Context(config, doc, cli_params, gsheets_session, failure_count, lock) as context, \
            YaDisk(config['yandex']['disk']['oauth_token']) as ya_client:
            if not context.cli_params.force and doc.extraction_complete:
                print(f"{doc.md5}: skipped because already processed")
                return
                
            print(f"{doc.md5}: downloading file from yadisk")
            context.local_doc_path = download_file_locally(ya_client, context.doc)
            
            # request latest metadata of the doc in yandex disk
            ya_doc_meta = ya_client.get_public_meta(context.doc.ya_public_url, fields=['md5', 'name', 'public_key', 'resource_id', 'sha256'])
            context.md5 = ya_doc_meta.md5
            context.ya_file_name = ya_doc_meta.name
            context.ya_public_key = ya_doc_meta.public_key
            context.ya_resource_id = ya_doc_meta.resource_id
            
            _process(context, gemini_client)
            with context.lock:
                if context.failure_count.value >= ATTEMPTS:
                    return
            _upload_artifacts(context)
            _upsert_document(context)
            
            with lock:
                failure_count.value = 0
                
            print(f"{doc.md5}: content extraction complete, tokens per chunk: {round(sum(context.tokens) / len(sum(context.tokens)))}")
    except KeyboardInterrupt:
        exit(0)
    except Exception as e:
        import traceback
        print(f"{doc.md5}: failed with error: {e}")
        traceback.print_exc()
        exit(1)
        
        if isinstance(e, ClientError) and e.code == 429:
            print(f"{doc.md5}: received 429, sleeping for 60 seconds")
            time.sleep(60)
        with lock:
            failure_count.value += 1
        return f"{doc.md5}: failed"

def _process(context, gemini_client):
    with pymupdf.open(context.local_doc_path) as pdf_doc:
        _extract_content(context, pdf_doc, gemini_client)
        with context.lock:
            if context.failure_count.value >= ATTEMPTS:
                return
        postprocessed = postprocess(context)
        
        context.formatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-formatted.md")
        with open(context.formatted_response_md, 'w') as f:
            f.write(postprocessed)
            
        context.local_content_path = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.zip")
        with zipfile.ZipFile(context.local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
            zf.write(arcname=f"{context.md5}.md", filename=context.formatted_response_md)
        
        context.extraction_method = f"{context.cli_params.model}/{context.cli_params.batch_size}/pdfinput"
        context.doc_page_count=pdf_doc.page_count
    
def _extract_content(context, pdf_doc, gemini_client):
    batch_size = context.cli_params.batch_size
    iter = list(batched(range(0, pdf_doc.page_count)[context.cli_params.page_slice], batch_size))

    prev_chunk_tail = None
    next_footnote_num = 1
    chunked_results_dir = get_in_workdir(Dirs.CHUNKED_RESULTS, context.md5)
    chunk_result_paths = []
    
    context.unformatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-unformatted.md")
    prompts_dir = get_in_workdir(Dirs.PROMPTS, context.md5)
    with open(context.unformatted_response_md, "w") as output:
        for idx, chunk in enumerate(iter, start=1):
            with context.lock:
                if context.failure_count.value >= ATTEMPTS:
                    break
            print(f"{context.md5}: extracting {idx} of {len(iter)}")
            _from = chunk[0]
            _to = chunk[-1]
            chunk_result_complete_path = os.path.join(chunked_results_dir, f"chunk-{_from}-{_to}")
            if os.path.exists(chunk_result_complete_path):
                print(f"{context.md5}: chunk {idx}({_from}-{_to}) is already extracted")
                with open(chunk_result_complete_path, "r") as f:
                    content = ExtractionResult.model_validate_json(f.read()).content
            else:
                # create a pdf doc what will contain a slice of original pdf doc
                slice_file_path = _create_doc_clice(_from, _to, pdf_doc, context.md5)
                
                # prepare prompt
                prompt = cook_extraction_prompt(_from, _to, next_footnote_num, gemini_client)
                with open(os.path.join(prompts_dir, f"chunk-{_from}-{_to}"), "w") as f:
                    json.dump(prompt, f, indent=4, ensure_ascii=False)
            
                # request gemini
                files = {slice_file_path: "application/pdf"}
                response = request_gemini(client=gemini_client, model=context.cli_params.model, prompt=prompt, files=files, schema=ExtractionResult, timeout_sec=6000)
            
                # write result into file
                chunk_result_incomplete_path = chunk_result_complete_path + ".part"
                with open(chunk_result_incomplete_path, "w") as f:
                    for p in response:
                        if text := p.text:
                            f.write(text)
                            
                context.tokens.append(p.usage_metadata.total_token_count)
                print(f"{context.md5}: tokens for this chunk {p.usage_metadata.total_token_count}")

                # validating schema
                with open(chunk_result_incomplete_path, "r") as f:
                    content = ExtractionResult.model_validate_json(f.read()).content
                    
                # "mark" batch as extracted by renaming file
                shutil.move(chunk_result_incomplete_path, chunk_result_complete_path)
            
            if prev_chunk_tail:
                content = continue_smoothly(prev_chunk_tail=prev_chunk_tail, content=content)
 
            # prev_chunk_tail = content[-300:]
            content = content.removesuffix('-')
            output.write(content)
            output.flush()
            
            # define number of last footnote detected in the document
            footnote_counters = re.findall(r"\[\^(\d+)\]:", content)
            last_footnote_num = max(map(int, footnote_counters)) if footnote_counters else 0
            next_footnote_num = max(next_footnote_num, last_footnote_num + 1)

            chunk_result_paths.append(chunk_result_complete_path)

def _create_doc_clice(_from, _to, pdf_doc, md5):
    slice_file_path = get_in_workdir(Dirs.DOC_SLICES, md5, file=f"slice-{_from}-{_to}.pdf")
    if not os.path.exists(slice_file_path):
        doc_slice = pymupdf.open()
        doc_slice.insert_pdf(pdf_doc, from_page=_from, to_page=_to)
        doc_slice.save(slice_file_path)
    return slice_file_path
    
def _upload_artifacts(context):
    print(f"{context.md5}: Uploading artifacts to object storage")
                
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
    print(f"{context.md5}: Updating doc details in gsheets")

    doc = context.doc
    doc.file_name = context.ya_file_name
    doc.ya_public_key=context.ya_public_key
    doc.ya_resource_id=context.ya_resource_id

    doc.content_extraction_method=context.extraction_method
    doc.document_url = context.remote_doc_url
    doc.content_url = context.remote_content_url
    doc.extraction_complete=True
        
    context.gsheets_session.update(doc)