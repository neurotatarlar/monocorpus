from rich import print
from utils import get_in_workdir, download_file_locally, encrypt
from dirs import Dirs
import zipfile
import re
from monocorpus_models import  Session
import time
from google.genai.errors import ClientError
from queue import Empty
import threading
from content.pdf_context import Context
import os
from gemini import gemini_api, create_client
import pymupdf
import shutil
from content.pdf_postprocess import postprocess
from prompt import cook_extraction_prompt
import json
from content.continuity_checker import continue_smoothly
from pydantic import BaseModel
from s3 import upload_file, create_session


model = 'gemini-2.5-pro'

class ExtractionResult(BaseModel):
    content: str
    
    
    
class ChunkPlanner:
    
    def __init__(self, chunks_dir):
        self.chunks_dir = chunks_dir
        
        
    def iterate():
        pass
    
    
    
class Chunk:
    
    def __init__(self, start, end):
        self.start = start
        self.end = end
    
    
    
class PdfExtractor:
    
    
    def __init__(self, key, tasks_queue, config, s3lient, ya_client, exceeded_keys_lock, exceeded_keys_set):
        self.key = key
        self.tasks_queue = tasks_queue
        self.config = config
        self.s3lient = s3lient
        self.ya_client = ya_client
        self.exceeded_keys_lock = exceeded_keys_lock, 
        self.exceeded_keys_set = exceeded_keys_set
        
    def __call__(self):
        gemini_client = create_client(self.key)
        while True:
            try: 
                doc = self.tasks_queue.get(block=False)
                self.log(f"Extracting content from document {doc.md5}({doc.ya_public_url}) by key {self.key}")
                context = self._extract_doc(doc, gemini_client)
                
                # additional processing
                self.log(f"Postprocessing document {doc.md5}({doc.ya_public_url})")
                postprocessed = postprocess(context, self.config)
                
                # write postprocessed content to a file
                context.formatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-formatted.md")
                with open(context.formatted_response_md, 'w') as f:
                    f.write(postprocessed)
                    
                # create a zip file with the content
                context.local_content_path = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.zip")
                with zipfile.ZipFile(context.local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
                    zf.write(arcname=f"{context.md5}.md", filename=context.formatted_response_md)
                
                context.extraction_method = f"gemini-2.5/pdfinput"

                # upload the content to S3
                self._upload_artifacts(context)
                
                # update the document in the gsheet
                with Session() as gsheets_session:
                    self._upsert_document(gsheets_session, context)
                
                self.log(f"[bold green]Content extraction complete[/bold green], unmatched images: {context.unmatched_images} of {context.total_images}")
            except Empty:
                self.log("No tasks for processing, shutting down thread...")
                return
            except ClientError as e:
                if e.code == 429:
                    self.log(f"Key {self.key} exhausted {e}, shutting down thread...") 
                    self.tasks_queue.put(doc)
                    with self.exceeded_keys_lock:
                        self.exceeded_keys_set.add(self.key)
                return
            except Exception as e:
                import traceback
                self.log(f"Could not extract content from doc {doc.md5}: {e} \n{traceback.format_exc()}")
                continue
            
            
    def _extract_doc(self, doc, gemini_client):
        local_doc_path = download_file_locally(self.ya_client, doc, self.config)
        context = Context(doc, local_doc_path)
        # request latest metadata of the doc in yandex disk
        self._enrich_context(self.ya_client, context)
        
        context.unformatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-unformatted.md")
        with pymupdf.open(context.local_doc_path) as pdf_doc, open(context.unformatted_response_md, "w") as output:
            context.doc_page_count=pdf_doc.page_count
            chunked_results_dir = get_in_workdir(Dirs.CHUNKED_RESULTS, context.md5)
            prev_chunk_tail = None
            headers_hierarchy = []
            next_footnote_num = 1
            
            chunk_planner = ChunkPlanner(chunked_results_dir)
            while True:
                chunk = chunk_planner.next()
                # if not chunk:
                #     break
                # chunk_result_complete_path = os.path.join(chunked_results_dir, f"chunk-{chunk.start}-{chunk.end}.json")
                # content = None
            # chunks = self._get_chunks(dir=chunked_results_dir, start_inc=0, end_excl=pdf_doc.page_count-1, chunk_size=20)
            # for idx, chunk in enumerate(chunks, start=1):
                # _from = chunk[0]
                # _to = chunk[-1] 
                chunk_result_complete_path = os.path.join(chunked_results_dir, f"chunk-{chunk.start}-{chunk.end}.json")
                content = None
                
                if os.path.exists(chunk_result_complete_path):
                    self.log(f"Chunk({chunk.start}-{chunk.end}) of {len(pdf_doc.page_count())} is already extracted")
                    with open(chunk_result_complete_path, "r") as f:
                        content = ExtractionResult.model_validate_json(f.read()).content
                        
                if not content:
                    # create a pdf doc what will contain a slice of original pdf doc
                    slice_file_path = self._create_doc_clice(chunk.start, chunk.end, pdf_doc, context.md5)
                
                    chunk_result_incomplete_path = chunk_result_complete_path + ".part"
                    
                    # prepare prompt
                    prompt = cook_extraction_prompt(chunk.start, chunk.end, next_footnote_num, headers_hierarchy)
                    prompts_dir = get_in_workdir(Dirs.PROMPTS, context.md5)
                    with open(os.path.join(prompts_dir, f"chunk-{chunk.start}-{chunk.end}"), "w") as f:
                        json.dump(prompt, f, indent=4, ensure_ascii=False)
                    
                    # request gemini
                    files = {slice_file_path: "application/pdf"}
                    
                    self.log(f"Requesting gemini for chunk ({chunk.start}-{chunk.end}) of {len(pdf_doc.page_count())} with key `{self.key}`")
                    
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
                        content = ExtractionResult.model_validate_json(f.read()).content
                        
                    if not content:
                        raise ValueError("Could not extract chunk")
                        
                    # "mark" batch as extracted by renaming file
                    shutil.move(chunk_result_incomplete_path, chunk_result_complete_path)
                        
                    self.log(f"[bold green]Chunk ({_from}-{_to}) extracted successfully'[/bold green]")

                # shift footnotes up in the content to avoid heaving footnote text at the brake between slices
                content = self._shift_trailing_footnotes_up(content)
                headers_hierarchy.extend(self._extract_markdown_headers(content))
                
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
                
        return context
               
               
    def _extract_markdown_headers(self, content):
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
    
    
    def _shift_trailing_footnotes_up(cself, content):
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


    def _get_chunks(self, dir, start_inc: int, end_excl: int, chunk_size: int, last_chunk_min_size=5):
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


    def _enrich_context(self, ya_client, context):
        ya_doc_meta = ya_client.get_public_meta(context.doc.ya_public_url, fields=['md5', 'name', 'public_key', 'resource_id', 'sha256'])
        context.md5 = ya_doc_meta.md5
        context.ya_file_name = ya_doc_meta.name
        context.ya_public_key = ya_doc_meta.public_key
        context.ya_resource_id = ya_doc_meta.resource_id
        
        
    def log(self, message):
        message = f"{threading.current_thread().name} - {time.strftime('%Y-%m-%d %H:%M:%S')}: {message}"
        log_file = get_in_workdir(Dirs.LOGS, file=f"content_extraction_{self.key}.log")
        with open(log_file, "a") as log:
            log.write(f"{message}\n")
        print(message)
        
        
    def _create_doc_clice(self, _from, _to, pdf_doc, md5):
        slice_file_path = get_in_workdir(Dirs.DOC_SLICES, md5, file=f"slice-{_from}-{_to}.pdf")
        if not os.path.exists(slice_file_path):
            doc_slice = pymupdf.open()
            doc_slice.insert_pdf(pdf_doc, from_page=_from, to_page=_to)
            doc_slice.save(slice_file_path)
        return slice_file_path


    def _upload_artifacts(self, context):
        self.log("Uploading artifacts to object storage")
                    
        session = create_session(self.config)
        
        if context.local_content_path:
            content_key = f"{context.md5}.zip"
            content_bucket = self.config["yandex"]["cloud"]['bucket']['content']
            context.remote_content_url = upload_file(context.local_content_path, content_bucket, content_key, session)
            
        if context.local_doc_path:
            doc_bucket = self.config["yandex"]["cloud"]['bucket']['document']
            doc_key = os.path.basename(context.local_doc_path)
            context.remote_doc_url = upload_file(context.local_doc_path, doc_bucket, doc_key, session, skip_if_exists=True)
        
        for chunk_path in context.chunk_paths:
            file_name, _ = os.path.splitext(os.path.basename(chunk_path))
            file_name_ext = f"{file_name}.zip"
            key = f"{context.md5}/{file_name_ext}"
            chunk_path_arc = get_in_workdir(Dirs.CHUNKED_RESULTS, context.md5, file=file_name_ext)
            with zipfile.ZipFile(chunk_path_arc, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
                zf.write(arcname=f"{file_name}.json", filename=chunk_path)
                
            doc_bucket = self.config["yandex"]["cloud"]['bucket']['content_chunks']
            upload_file(chunk_path_arc, doc_bucket, key, session)
    
    
    def _upsert_document(self, gsheets_session, context):
        doc = context.doc
        doc.file_name = context.ya_file_name
        doc.ya_public_key=context.ya_public_key
        doc.ya_resource_id=context.ya_resource_id

        doc.content_extraction_method=context.extraction_method
        doc.document_url = encrypt(context.remote_doc_url, self.config) if doc.sharing_restricted else context.remote_doc_url
        doc.content_url = context.remote_content_url
        doc.unmatched_images = f"{context.unmatched_images} of {context.total_images}"
            
        self.log("Updating doc details in gsheets")
        gsheets_session.update(doc)
