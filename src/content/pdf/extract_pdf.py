from utils import read_config, download_file_locally, obtain_documents, get_in_workdir, encrypt
from yadisk_client import YaDisk
from monocorpus_models import Document, Session
from content.pdf.context import Context, Message
from s3 import upload_file, create_session
import os
from gemini import gemini_api, create_client
import pymupdf
from dirs import Dirs
import shutil
from content.pdf.postprocess import postprocess
import re
import zipfile
from prompt import cook_extraction_prompt
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager
from gemini import create_client
import json
from rich import print
from content.pdf.continuity_checker import continue_smoothly
from google.genai.errors import ClientError
from multiprocessing import Manager, Queue
from rich.console import Group
from rich.panel import Panel
import threading
from rich.table import Table
from rich.live import Live
import time
from datetime import timedelta, timezone, datetime
from pydantic import BaseModel

ATTEMPTS = 30

too_expensive = {
    "7e18fc2e65badafaeacd3503fcb8df46",
    "2d7b5f5732a0144fe0fcf0c44cffc926",
    "ced45598a9cc9b331e1529c89ad0c77a",
    "e9eb18e8ba5f694f3de49a63f43a6255",
    "2f974ec14f30e05954f2748899a078b2",
    "f3e9b4311f6506f1ceb0f6f4b4de5f54",
    "395e748bdd6d6bf129925a3b616610f8",
    "6efdfcdbee76e39b1f947642e0ae0a11",
    "ad03cef6565ad757d6c2d47f159d5a5d",
    "15b4893c6cd99195774548ca4276d06d",
    "d601f93e8ce2cb4e3bc7dd6feac91a00",
    "a2b2ff6423020c31dfc3e85940f24255",  
    "914632a4bbf6c6c663a77b0e9e9d7bfa",
    "29b3429e9f1e1e31c2a89ebc24f9a073",
    "2d153f45e769759a8a75742c34bda846",
    "de06112b86863a0696ce7486d920efe4",
    "c3f1358a7d04d8efc051d94c5943c946",
    "81b2b8e133d6f61adf6dd3023da686ae",
    "5d39267ba7f82ad8ba813067dd2e14d6",  
    "acaf023d731e6f46627f34b291103f8d",
    "35e278f0b4cdb38c350dad01ccc915c3", 
    "c47a47587ad50b26317232486a9150a6",
    "d8e154dee7ae0ca66c44dfca5e0c6b6e", 
    "66fb1f7c96aea07e4431ff5fe55ab476",
    "3388c2aa2ca2d219af9211fce849d815",
    "bbd9f7f9571224e8e2e2abd6e9beb7d5",
    "490cd5879ea5a7dacbbcd41630633ec7",    
    "9099fc243084336a3d4a5bcd1b06b571",
    "12e94fe079fdf11f9414aa3c59a807dc",
    "aaa2556dd51b33e95f476837b6effe79",
    "ccda7afd6ad404ff7d352fa9d204d58c", 
    'ec9ef0cdc988d2cb8b49f95ea0d1201b',
    '1fa8ab2b88f7249375fcb612f5046e05',
    '962a1464b3399e9e6f5b5cd69693e670',
    '54b44249d1ea90a279e4ab0cdd9752c6',
    'fa2fbb8c5f8e1f1650df7606b07aba2d',
    '7bee22a24d3d08eb985255abcad73f9a',
    'f7445cd9403c1d44e042cc4e815941a2',
    '602d02ced4d0b2aa8bcd34e26dcbfc58',
    '993abe6cfd09b40afdcf4b39eddec115',
    '6767031d9b0a44dbc051a7857011490f',
    "c74e15c342cd45bd877e0b6fb0bc2af8",
    '107984a814779344aefd65410b9c1e84',
    'a76477128a14422d45383dda39477912',
    '91a0c3ac41c48c4cf6776a67e62d1f24',
    '893bcc71b541cebb269c2f154bd95baf',
    '1120f5cb71de4cf7b6f8b80b2f9ca8c7',
    'e56b6a4119b75b33c5320e60c1867249',
    'aebaa8474695ea06429537584419c1ec',
    '56443fb769237020851bc5ccfa234cca',
    '95d0e85bcbe2f0da25e13dd49729fd31',
    '22a8af0eabc269d41e6d22e3646da9e8',
    '4bde1a9aabb6f6c7f5ead82aa51f8d27'
}

# these docs skipped because they are processed by external contributor
skipped_external = {
    "7ddc45e6fa6ed4caa120b11689cf200e",
    "23e247a5cf94523a26cef1baeca08330",
    "31ce8173d68e9d6ad43beb520d9e9448",
    "07cc4822f3e37effa20c74d10eff387a",
    "a9b1da6ea3a12aedb6ed27093eca1bce",
    "883d9c8190d42250a5081e1b7e5635d9",
    "ecdfa76d4ca720f647c4e03969cb052b",
    "e1e129d1fecae4ae7e97a487823d6e3b",
    "f026fd136cfdc31146eae9627f897d0a",
    "82c2ae6276da4ed305657700e0a3eb95",
    "c8698eaa01752a239d5779553ba8797e",
    "6350d3bebd8612c5d1f85d470c16f8f9",
    "b305bbcd3644e9a0cc5e74116d444727",
    "3996366e00a2398971f27b3d866b1f8d",
    "94c14cf503df51ebc166100d3b156116",
    "bb1499278121c478aff5a295f378b817",
    "cf97d9e734afe487b405b192a2b9132a",
    "a32a083aae191391afa1f0f0ad5612f6",
    "28797210aad79878bfca2f36c9ceffb2",
    "2284187654c2384c98bc2f218f4a4a31",
    "bc3baa864c4eb5b7ee16bcc693beeb3f",
    "fdb3ca5fec275257a473a078f5357762",
    "49d84164271052f59047aa55059ff354",
    "1c20250dcc2dfe2a576836209910eda2",
    "febbc761113bbb62d53d9d44b8aae03f",
    "85909aeddf6aad90af5e133647916a5c",
    "bb8054f3f97e6d8c24952747896ce798",
    "cb3a30518de60f86ac5a9320ddbc359f",
    "566ad47e7fa9d62de1aa6e718a51eefc",
    "9026cbbd642fe7e11263d1d93f341e46",
    "6e4d024cc644868d8cd4b1a61e6e6e01",
    "567e836d1d0b3e4844136298ff478e4d",
    "0ac2b8526619d90a033f555e96824241",
    "1aa8a0f53a6eb7d1a80fd6f277b1461b",
    "bb36a0f7472ad8bbf042e1808059e986",
    "a2836964850cfc7a0aa60c9d84238b67",
    "d17e958165101c38cbe54802cbf3ccfb",
    "33d340001666758a941f45d8e52918d1",
    "2b74413020def6d1721d2b4cebacadc4",
    "5bc67f299737246e0f158eda5f25613b",
    "83fd6bbe968f6d5927ff461d09ea4bad",
    "16f2434b740ee116ac6f634f35977345",
    "a2aee670bcf2824596cf1a2e82f7af11",
    "581d6547cdf1f7929541907285ddb56d",
    "d1ac7329ea0ada8a4f9382a63a59ddec",
    "1457cc34b6d426459b6bfcba4136a9f7",
    "4426f343d867a49c5c8b91a1af48e7f7",
    "57878b070e6074f42fe72bc09369b024",
    "fec950bab89ad759a306b26e38b71259",
    "359db1b930db12ddbd2697a119c7872e",
}

skipped = skipped_external | too_expensive

class ExtractionResult(BaseModel):
    content: str

def extract(cli_params):
    print(f'About to extract content with params => {", ".join([f"{k}: {v}" for k,v in cli_params.__dict__.items() if v])}')
    config = read_config()
    
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client, Manager() as manager:
        failure_count = manager.Value('i', 0)
        lock = manager.Lock()
        queue = manager.Queue()
        
        printer_thread = threading.Thread(target=printer_loop, args=(queue,), daemon=True)
        printer_thread.start()

        predicate = (
            Document.content_url.is_(None) &
            Document.full.is_(True) &
            Document.language.is_("tt-Cyrl") &
            Document.mime_type.is_('application/pdf')
            # Document.isbn.is_not(None)
        )
        
        docs = [d for d in obtain_documents(cli_params, ya_client, predicate, limit=cli_params.limit) if d.md5 not in skipped ]
        print(f"Found {len(docs)} documents to process")
        
        with ProcessPoolExecutor(max_workers=cli_params.workers) as executor:
            futures = {
                executor.submit(__task_wrapper, config, doc, cli_params, failure_count, lock, queue): doc
                for doc 
                in docs
            }
            for future in as_completed(futures):
                if _check_stop_file():
                    print("[yellow]Gracefully shutdown. Stopping...[/yellow]")
                    executor.shutdown(wait=True, cancel_futures=False)
                    break 
                elif failure_count.value >= ATTEMPTS:
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
    if _check_stop_file():
        return None # skip if shutdown was requested
    try:
        gemini_client = create_client(config['google_api_key']['free'])
        with Session() as gsheets_session, \
            Context(config, doc, cli_params, gsheets_session, failure_count, lock, queue) as context, \
            YaDisk(config['yandex']['disk']['oauth_token']) as ya_client:
                
            if not context.cli_params.force and doc.content_url:
                context.log("Skipped because already processed")
                return
                
            context.log("Downloading file from yadisk")
        
            context.local_doc_path = download_file_locally(ya_client, context.doc, config)
            
            # request latest metadata of the doc in yandex disk
            ya_doc_meta = ya_client.get_public_meta(context.doc.ya_public_url, fields=['md5', 'name', 'public_key', 'resource_id', 'sha256'])
            context.md5 = ya_doc_meta.md5
            context.ya_file_name = ya_doc_meta.name
            context.ya_public_key = ya_doc_meta.public_key
            context.ya_resource_id = ya_doc_meta.resource_id
            
            _process(context, gemini_client)
            if _check_stop_file():
                return
            _upload_artifacts(context)
            _upsert_document(context)
            
            with lock:
                failure_count.value = 0
                
            context.log(f"[bold green]Content extraction complete[/bold green], unmatched images: {context.unmatched_images} of {context.total_images}", complete=True)
    except KeyboardInterrupt:
        exit(0)
    except Exception as e:
        import traceback
        print(f"[red]Error during extraction: {type(e).__name__}: {e}[/red]")
        print(traceback.format_exc())
        exit()
        # context.log(f"[bold red]failed with error: {e}[/bold red]", complete=True)
        # with lock:
        #     failure_count.value += 1
        #     if context.failure_count.value >= ATTEMPTS:
        #         return


def _process(context, gemini_client):
    with pymupdf.open(context.local_doc_path) as pdf_doc:
        _extract_content(context, pdf_doc, gemini_client)
        if _check_stop_file():
            return None
        postprocessed = postprocess(context)
        
        context.formatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-formatted.md")
        with open(context.formatted_response_md, 'w') as f:
            f.write(postprocessed)
            
        context.local_content_path = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.zip")
        with zipfile.ZipFile(context.local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
            zf.write(arcname=f"{context.md5}.md", filename=context.formatted_response_md)
        
        context.extraction_method = f"gemini-2.5/pdfinput"
        context.doc_page_count=pdf_doc.page_count
    
    
def _extract_content(context, pdf_doc, gemini_client):
    chunked_results_dir = get_in_workdir(Dirs.CHUNKED_RESULTS, context.md5)
    chunks = _get_chunks(dir=chunked_results_dir, start_inc=0, end_excl=pdf_doc.page_count-1, chunk_size=context.cli_params.batch_size)
    context.log(f"Extracting chunks: {chunks}")
    prev_chunk_tail = None
    headers_hierarchy = []
    next_footnote_num = 1
    
    context.unformatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-unformatted.md")
    prompts_dir = get_in_workdir(Dirs.PROMPTS, context.md5)
    with open(context.unformatted_response_md, "w") as output:
        for idx, chunk in enumerate(chunks, start=1):
            with context.lock:
                if context.failure_count.value >= ATTEMPTS:
                    return
            if _check_stop_file():
                return
            _from = chunk[0]
            _to = chunk[-1]
            chunk_result_complete_path = os.path.join(chunked_results_dir, f"chunk-{_from}-{_to}.json")
            content = None
            if os.path.exists(chunk_result_complete_path):
                context.log(f"Chunk {idx}({_from}-{_to}) of {len(chunks)} is already extracted")
                with open(chunk_result_complete_path, "r") as f:
                    content = validate_chunk(f.read())
                    
            if not content:
                # create a pdf doc what will contain a slice of original pdf doc
                slice_file_path = _create_doc_clice(_from, _to, pdf_doc, context.md5)
                
                # prepare prompt
                prompt = cook_extraction_prompt(_from, _to, next_footnote_num, headers_hierarchy)
                with open(os.path.join(prompts_dir, f"chunk-{_from}-{_to}"), "w") as f:
                    json.dump(prompt, f, indent=4, ensure_ascii=False)
            
                # request gemini
                files = {slice_file_path: "application/pdf"}
                chunk_result_incomplete_path = chunk_result_complete_path + ".part"
                for model in context.config['gemini_models']:
                    try:
                        # todo as much workers as keys
                        # update prompt by adding location of input and ouput
                        response = gemini_api(
                            client=gemini_client,
                            model=model,
                            prompt=prompt,
                            files=files,
                            schema=ExtractionResult,
                            timeout_sec=6000
                        )
                        # write result into file
                        with open(chunk_result_incomplete_path, "w") as f:
                            for p in response:
                                if text := p.text:
                                    f.write(text)
                        # validating schema
                        with open(chunk_result_incomplete_path, "r") as f:
                            content = validate_chunk(f.read())
                            
                        if not content:
                            raise ValueError("Could not extract chunk")
                            
                        # "mark" batch as extracted by renaming file
                        shutil.move(chunk_result_incomplete_path, chunk_result_complete_path)
                         
                        context.log(
                            f"[green]Chunk {idx}({_from}-{_to}) of {len(chunks)} extracted with model '{model}': "
                            f"{p.usage_metadata.total_token_count} tokens used[/green]"
                        )
                        break
                    except Exception as e:
                        context.log(f"[red]Failed to extract chunk {idx}({_from}-{_to}) with model '{model}': {type(e).__name__}: {e}[/red]")
                        if isinstance(e, ClientError) and e.code == 429:
                            context.log("[yellow]Received status code 429, sleeping for 60 seconds[/yellow]")
                            time.sleep(60)
                else:
                    raise ValueError(f"[red]Could not extract chunk {idx}({_from}-{_to}) with any of the models, skipping it...")
                                        
            # shift footnotes up in the content to avoid heaving footnote text at the brake between slices
            content = shift_trailing_footnotes_up(content)
            headers_hierarchy.extend(extract_markdown_headers(content))
            
            if prev_chunk_tail:
                content = continue_smoothly(prev_chunk_tail=prev_chunk_tail, content=content)
 
            prev_chunk_tail = content[-300:]
            # important to remove hyphen after taking the chunk tail
            content = content.removesuffix('-').removesuffix('\n')
            output.write(content)
            output.flush()
            
            # define number of last footnote detected in the document
            footnote_counters = re.findall(r"\[\^(\d+)\]:", content)
            last_footnote_num = max(map(int, footnote_counters)) if footnote_counters else 0
            next_footnote_num = max(next_footnote_num, last_footnote_num + 1)            

            context.chunk_paths.append(chunk_result_complete_path)

def validate_chunk(raw_content):
    content = ExtractionResult.model_validate_json(raw_content).content
    return content

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
        content_key = f"{context.md5}.zip"
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
    doc.document_url = encrypt(context.remote_doc_url, context.config) if doc.sharing_restricted else context.remote_doc_url
    doc.content_url = context.remote_content_url
    doc.unmatched_images = f"{context.unmatched_images} of {context.total_images}"
        
    context.log("Updating doc details in gsheets")
    context.gsheets_session.update(doc)
    
    

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
    active_tables = {}
    results = Table.grid(padding=(0, 1, 0, 0), collapse_padding=False, pad_edge=True)
    
    def render(style='white'):
        # This returns a Group of Panels, one for each task
        return Group(
            *[Panel(table, title=str(id), style=style) for id, table in active_tables.items()], results,
        )
    with Live(render(), screen=False, auto_refresh=False) as live:        
        while True:
            msg = queue.get()
            if msg == "__STOP__":
                break
            
            if isinstance(msg, Message):
                if msg.complete:
                    del active_tables[msg.id]
                    results.add_row(msg.id, msg.content)
                else:
                    if msg.id not in active_tables:
                        active_tables[msg.id] = Table.grid()
                    log_time = datetime.now(timezone(timedelta(hours=+3))).strftime('%Y-%m-%d %H:%M:%S')
                    active_tables[msg.id].add_row(f"{log_time} => {msg.content}")
                
                # Update live display
                live.update(render())
                live.refresh()
                
            else: 
                raise ValueError(f"Unknown message type {msg}" )
            

def shift_trailing_footnotes_up(content):
    lines = content.strip().splitlines()
    footnote_pattern = re.compile(r'^\[\^\d+\]:')

    # ✅ Early exit if the last line is not a footnote
    if not lines or not footnote_pattern.match(lines[-1].strip()):
        return content

    footnotes = []
    i = len(lines) - 1

    # Step 1: Scan in reverse to collect trailing footnotes
    while i >= 0:
        line = lines[i].strip()
        if line == '' or footnote_pattern.match(line):
            footnotes.insert(0, lines[i])
            i -= 1
        else:
            break

    # Step 2: Remaining lines before footnotes
    body_lines = lines[:i + 1]

    # Step 3: Reassemble: body → blank line → footnotes → blank line → last paragraph
    reordered = body_lines[:-1] + [''] + footnotes + [''] + body_lines[-1:]

    return '\n'.join(reordered)

def _check_stop_file():
    return os.path.exists("stop")

def _get_chunks(dir, start_inc: int, end_excl: int, chunk_size: int, last_chunk_min_size=5):
    # # Step 1: Sort existing chunks
    existing_sorted = []
    pattern = re.compile(r"^chunk-(\d+)-(\d+)\.json$")
    for filename in os.listdir(dir):
        match = pattern.match(filename)
        if match:
            start, end = map(int, match.groups())
            existing_sorted.append((start, end))
    existing_sorted = list(sorted(existing_sorted))
    
    # Step 2: Build list of gaps (free ranges)
    gaps = []
    current = start_inc
    for start, end in sorted(existing_sorted):
        if current < start:
            gaps.append((current, start - 1))
        current = max(current, end + 1)
    if current <= end_excl:
        gaps.append((current, end_excl))

    # Step 3: Fill gaps with new chunks
    new_chunks = []
    for gap_start, gap_end in gaps:
        pos = gap_start
        while pos <= gap_end:
            end = min(pos + chunk_size - 1, gap_end)
            new_chunks.append((pos, end))
            pos = end + 1

    # Step 4: Merge last chunk if it's too small
    if len(new_chunks) >= 2:
        last_start, last_end = new_chunks[-1]
        prev_start, prev_end = new_chunks[-2]
        if (last_end - last_start + 1) < last_chunk_min_size:
            # Merge small last chunk into previous
            new_chunks[-2] = (prev_start, last_end)
            new_chunks.pop()

    # Step 5: Return sorted list of all chunks
    return sorted(existing_sorted + new_chunks)