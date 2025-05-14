from utils import read_config, download_file_locally, obtain_documents, get_in_workdir
from yadisk_client import YaDisk
from monocorpus_models import Document, Session
from context import Context, Message
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
from multiprocessing import Manager
from context import Context
from gemini import create_client
from utils import download_file_locally
import json
from rich import print
from continuity_checker import continue_smoothly
from google.genai.errors import ClientError
from multiprocessing import Manager, Queue
from rich.console import Group, Console
from rich.panel import Panel
import threading
from rich.table import Table
from rich.live import Live
import time
import datetime

# todo be ready for dynamic batch size
ATTEMPTS = 10

def extract_structured_content(cli_params):
    print(f'About to extract content with params => {", ".join([f"{k}: {v}" for k,v in cli_params.__dict__.items() if v])}')
    config = read_config()
    
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client, Manager() as manager:
        
        failure_count = manager.Value('i', 0)
        lock = manager.Lock()
        queue = manager.Queue()
        
        printer_thread = threading.Thread(target=printer_loop, args=(queue,), daemon=True)
        printer_thread.start()

        predicate = (
            Document.extraction_complete.is_not(True) 
            & Document.full.is_(True) 
            & Document.language.is_("tt-Cyrl") 
            & Document.mime_type.is_('application/pdf')
        )
        
        docs = obtain_documents(cli_params, ya_client, predicate, limit=cli_params.limit)
        
        with ProcessPoolExecutor(max_workers=cli_params.workers) as executor:
            futures = {
                executor.submit(__task_wrapper, config, doc, cli_params, failure_count, lock, queue): doc
                for doc 
                in docs
            }
            for future in as_completed(futures):
                if failure_count.value >= ATTEMPTS:
                    print("[red]Too many consecutive failures. Exiting all processing.[/red]")
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                
                try:
                    _ = future.result()
                except Exception as e:
                    # Already reported inside __task_wrapper
                    pass
                
        # Gracefully stop printer
        queue.put("__STOP__")
        printer_thread.join()
        

                
def __task_wrapper(config, doc, cli_params, failure_count, lock, queue):
    try:
        gemini_client = create_client(cli_params.tier)
        with Session() as gsheets_session, \
            Context(config, doc, cli_params, gsheets_session, failure_count, lock, queue) as context, \
            YaDisk(config['yandex']['disk']['oauth_token']) as ya_client:
            if not context.cli_params.force and doc.extraction_complete:
                context.log("Skipped because already processed")
                return
                
            context.log("Downloading file from yadisk")
        
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
                
            context.log("[bold green]Content extraction complete[/bold green]")
    except KeyboardInterrupt:
        exit(0)
    except Exception as e:
        context.log(f"[bold red]failed with error: {e}[/bold red]")
        # import traceback
        # traceback.print_exc()
        if isinstance(e, ClientError) and e.code == 429:
            context.log("[yellow]received 429, sleeping for 60 seconds[/yellow]")
            time.sleep(60)
        with lock:
            failure_count.value += 1
            if context.failure_count.value >= ATTEMPTS:
                return

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
    headers_hierarchy = []
    next_footnote_num = 1
    chunked_results_dir = get_in_workdir(Dirs.CHUNKED_RESULTS, context.md5)
    
    context.unformatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-unformatted.md")
    prompts_dir = get_in_workdir(Dirs.PROMPTS, context.md5)
    with open(context.unformatted_response_md, "w") as output:
        for idx, chunk in enumerate(iter, start=1):
            with context.lock:
                if context.failure_count.value >= ATTEMPTS:
                    break
            context.log(f"Extracting {idx} of {len(iter)}")
            _from = chunk[0]
            _to = chunk[-1]
            chunk_result_complete_path = os.path.join(chunked_results_dir, f"chunk-{_from}-{_to}.json")
            if os.path.exists(chunk_result_complete_path):
                context.log(f"Chunk {idx}({_from}-{_to}) is already extracted")
                with open(chunk_result_complete_path, "r") as f:
                    content = ExtractionResult.model_validate_json(f.read()).content
            else:
                # create a pdf doc what will contain a slice of original pdf doc
                slice_file_path = _create_doc_clice(_from, _to, pdf_doc, context.md5)
                
                # prepare prompt
                prompt = cook_extraction_prompt(_from, _to, next_footnote_num, headers_hierarchy)
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
                            
                context.log(f"Tokens count for this chunk {p.usage_metadata.total_token_count}")

                # validating schema
                with open(chunk_result_incomplete_path, "r") as f:
                    content = ExtractionResult.model_validate_json(f.read()).content
                    
                # "mark" batch as extracted by renaming file
                shutil.move(chunk_result_incomplete_path, chunk_result_complete_path)
            
            headers_hierarchy.extend(extract_markdown_headers(content))
            
            if prev_chunk_tail:
                content = continue_smoothly(prev_chunk_tail=prev_chunk_tail, content=content)
 
            prev_chunk_tail = content[-300:]
            content = content.removesuffix('-')
            output.write(content)
            output.flush()
            
            # define number of last footnote detected in the document
            footnote_counters = re.findall(r"\[\^(\d+)\]:", content)
            last_footnote_num = max(map(int, footnote_counters)) if footnote_counters else 0
            next_footnote_num = max(next_footnote_num, last_footnote_num + 1)            

            context.chunk_paths.append(chunk_result_complete_path)

def _create_doc_clice(_from, _to, pdf_doc, md5):
    slice_file_path = get_in_workdir(Dirs.DOC_SLICES, md5, file=f"slice-{_from}-{_to}.pdf")
    if not os.path.exists(slice_file_path):
        doc_slice = pymupdf.open()
        doc_slice.insert_pdf(pdf_doc, from_page=_from, to_page=_to)
        doc_slice.save(slice_file_path)
    return slice_file_path
    
def _upload_artifacts(context):
    context.log("Uploading artifacts to object storage")
                
    session = create_session(context.config)
    
    if context.local_content_path:
        content_key = f"{context.md5}-content.zip"
        content_bucket = context.config["yandex"]["cloud"]['bucket']['content']
        context.remote_content_url = upload_file(context.local_content_path, content_bucket, content_key, session)
        
    if context.local_doc_path:
        doc_bucket = context.config["yandex"]["cloud"]['bucket']['document']
        doc_key = os.path.basename(context.local_doc_path)
        context.remote_doc_url = upload_file(context.local_doc_path, doc_bucket, doc_key, session, skip_if_exists=True)
    
    for chunk_path in context.chunk_paths:
        file_name, _ = os.path.splitext(os.path.basename(chunk_path))
        file_name_ext = f"{file_name}.zip"
        key = f"{context.md5}/{file_name_ext}"
        chunk_path_arc = get_in_workdir(Dirs.CHUNKED_RESULTS, context.md5, file=file_name_ext)
        with zipfile.ZipFile(chunk_path_arc, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
            zf.write(arcname=f"{file_name}.json", filename=chunk_path)
            
        doc_bucket = context.config["yandex"]["cloud"]['bucket']['content_chunks']
        upload_file(chunk_path_arc, doc_bucket, key, session)
    
def _upsert_document(context):

    doc = context.doc
    doc.file_name = context.ya_file_name
    doc.ya_public_key=context.ya_public_key
    doc.ya_resource_id=context.ya_resource_id

    doc.content_extraction_method=context.extraction_method
    doc.document_url = context.remote_doc_url
    doc.content_url = context.remote_content_url
    doc.extraction_complete=True
        
    context.log("Updating doc details in gsheets")
    # context.gsheets_session.update(doc)
    
    

def extract_markdown_headers(content):
    """
    Extracts Markdown headers up to a certain level and returns them in a structured format.
    
    Args:
        text (str): The Markdown content.

    Returns:
        str: A formatted string showing the header hierarchy.
    """
    headers = re.findall(r'^(#{2,6})\s+(.+)', content, re.MULTILINE)
    
    output_lines = []
    for hashes, title in headers:
        output_lines.append(f"{hashes} {title.strip()}")

    return output_lines

def printer_loop(queue: Queue):
    """Continuously read messages from queue and print with rich."""
    tables = {}
    
    def render(style="white"):
        # This returns a Group of Panels, one for each task
        return Group(
            *[Panel(table, title=str(id), style=style) for id, table in tables.items()]
        )
    with Live(render(), refresh_per_second=1, screen=False) as live:        
        while True:
            msg = queue.get()
            if msg == "__STOP__":
                break
            
            if isinstance(msg, Message):
                if msg.id not in tables:
                    tables[msg.id] = Table.grid()
                    
                tables[msg.id].add_row(f"{format(datetime.datetime.now(datetime.timezone(3))).strftime('%Y %m %d %H:%M:%S')} => {msg.content}")
                # Update live display
                live.update(render(msg.style))
            else: 
                raise ValueError("tnknown message type")