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


# todo provide headers hierarchy in prompt
# todo page numbers mismatch
# todo model candidates order
# todo be ready for dynamic batch size
# todo improve progress
# todo limit image sizes
# todo False
ATTEMPTS = 10

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
                
            print(f"{doc.md5}: Content extraction complete")
    except KeyboardInterrupt:
        exit(0)
    except Exception as e:
        print(f"{doc.md5}: failed with error: {e}")
        import traceback
        traceback.print_exc()
        
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
            chunk_result_complete_path = os.path.join(chunked_results_dir, f"chunk-{_from}-{_to}.json")
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
        
    print(f"{context.md5}: Updating doc details in gsheets")
    # context.gsheets_session.update(doc)
    
    
# e69f6006f8ed1f3bdd4345717ff91dbe: downloading file from yadisk
# e69f6006f8ed1f3bdd4345717ff91dbe: extracting 1 of 3
# e69f6006f8ed1f3bdd4345717ff91dbe: tokens for this chunk 67253
# e69f6006f8ed1f3bdd4345717ff91dbe: extracting 2 of 3
# e69f6006f8ed1f3bdd4345717ff91dbe: tokens for this chunk 54852
# e69f6006f8ed1f3bdd4345717ff91dbe: extracting 3 of 3
# e69f6006f8ed1f3bdd4345717ff91dbe: tokens for this chunk 52661
# Unmatched bbox: {'html': '<figure data-bbox="[0,0,1000,1000]" data-page="6"></figure>', 'bbox': [0.0, 0.0, 5938.0, 7830.0]}
# Unmatched bbox: {'html': '<figure data-bbox="[700, 80, 950, 950]" data-page="18"></figure>', 'bbox': [475.04, 5477.5, 5641.099999999999, 7433.75]}
# Unmatched bbox: {'html': '<figure data-bbox="[50, 50, 950, 950]" data-page="28"></figure>', 'bbox': [296.90000000000003, 391.25, 5641.099999999999, 7433.75]}
# Unmatched bbox: {'html': '<figure data-bbox="[50, 50, 950, 950]" data-page="30"></figure>', 'bbox': [296.90000000000003, 391.25, 5641.099999999999, 7433.75]}
# Unmatched bbox: {'html': '<figure data-bbox="[650, 80, 950, 950]" data-page="8"></figure>', 'bbox': [475.04, 5086.25, 5641.099999999999, 7433.75]}
# Unmatched bbox: {'html': '<figure data-bbox="[550, 80, 950, 950]" data-page="10"></figure>', 'bbox': [475.04, 4303.75, 5641.099999999999, 7433.75]}
# Unmatched bbox: {'html': '<figure data-bbox="[750, 80, 950, 950]" data-page="13"></figure>', 'bbox': [475.04, 5868.75, 5641.099999999999, 7433.75]}
# Unmatched bbox: {'html': '<figure data-bbox="[450, 80, 950, 950]" data-page="22"></figure>', 'bbox': [475.04, 3521.25, 5641.099999999999, 7433.75]}
# Unmatched bbox: {'html': '<figure data-bbox="[550, 80, 950, 950]" data-page="23"></figure>', 'bbox': [475.04, 4303.75, 5641.099999999999, 7433.75]}
# Unmatched bbox: {'html': '<figure data-bbox="[550, 80, 950, 950]" data-page="31"></figure>', 'bbox': [475.04, 4303.75, 5641.099999999999, 7433.75]}
# Unmatched bbox: {'html': '<figure data-bbox="[50, 80, 400, 950]" data-page="42"></figure>', 'bbox': [475.04, 391.25, 5641.099999999999, 3130.0]}
# Unmatched bbox: {'html': '<figure data-bbox="[550, 80, 950, 950]" data-page="43"></figure>', 'bbox': [475.04, 4303.75, 5641.099999999999, 7433.75]}
# Unmatched bbox: {'html': '<figure data-bbox="[50, 50, 950, 950]" data-page="46"></figure>', 'bbox': [296.90000000000003, 391.25, 5641.099999999999, 7433.75]}
# Unmatched bbox: {'html': '<figure data-bbox="[600, 80, 950, 950]" data-page="50"></figure## СТАРОСТА БЕЛӘН ШАЙТАН\n(Латыш халык әкияте)\n\nСтароста үзенең йортыннан алпавыт ишегалдына таба бара икән, юлда аңа шайтан очраган. Менә алар бергәләп сөйләшә-сөйләшә барганнар һәм юл буенда дуңгызлар көтүче бер малайны күргәннәр. Аның бер зур дуңгызы бәрәңге кырына кергән булган. Малай, старостаны күрүгә, дуңгыз артыннан кырга ташланган:\n\n— Әй сине, шайтан алгыры! Бигрәк тә кабахәт дуңгызсың! — дигән.\n\nСтароста, моны ишеткәч, шайтанга төртеп куйган:\n\n— Ишетәсеңме, сиңа дуңгыз бирмәкче булалар, ә син алмыйсың. Мин булсам, хәзер үк барып алыр идем, — дигән.\n\nШайтан аңа болай дип җавап биргән:\n\n— Дуңгыз калсын, тимик! Көтүче малай ул — бер ятим бала, әгәр мин аның дуңгызын алсам,\n\n<figure data-bbox="[46, 59, 931, 948]" data-page="101"></figure>', 'bbox': [475.04, 4695.0, 5641.099999999999, 7433.75]}
# Unmatched bbox: {'html': '<figure data-bbox="[48, 105, 941, 901]" data-page="106"></figure>', 'bbox': [623.49, 375.6, 5350.138, 7363.325]}
# Unmatched bbox: {'html': '<figure data-bbox="[45, 101, 945, 904]" data-page="111"></figure>', 'bbox': [599.738, 352.125, 5367.952, 7394.625]}
# Unmatched bbox: {'html': '<figure data-bbox="[55, 0, 477, 701]" data-page="112"></figure>', 'bbox': [0.0, 430.375, 4162.538, 3732.5249999999996]}
# Unmatched bbox: {'html': '<figure data-bbox="[603, 168, 935, 838]" data-page="116"></figure>', 'bbox': [997.5840000000001, 4718.474999999999, 4976.044, 7316.375]}
# Unmatched bbox: {'html': '<figure data-bbox="[41, 101, 945, 904]" data-page="119"></figure>', 'bbox': [599.738, 320.825, 5367.952, 7394.625]}
# Unmatched bbox: {'html': '<figure data-bbox="[320, 416, 401, 582]" data-page="125"></figure>', 'bbox': [2470.208, 2504.0, 3455.9159999999997, 3137.8250000000003]}
# e69f6006f8ed1f3bdd4345717ff91dbe: Uploading artifacts to object storage
# e69f6006f8ed1f3bdd4345717ff91dbe: Updating doc details in gsheets
