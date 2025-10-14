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
from content.pdf_postprocess import postprocess, NoBboxError
from prompt import cook_extraction_prompt
import json
from content.continuity_checker import continue_smoothly
from pydantic import BaseModel, ValidationError
from s3 import upload_file, create_session
from yadisk.exceptions import PathNotFoundError
from json.decoder import JSONDecodeError
import datetime
import time
from google.genai.errors import ServerError


model = 'gemini-2.5-pro'

FIGURE_TAG_PATTERN = re.compile(r"<figure\b[^>]*>", re.IGNORECASE)


class ExtractionResult(BaseModel):
    content: str
    
class ChunkPlanner:
    def __init__(self, chunked_results_dir, pages_count, chunk_sizes=[5, 3, 2, 1]):
        self.chunked_results_dir = chunked_results_dir
        self.pages_count = pages_count
        self.chunk_sizes = chunk_sizes
        self.current_chunk_size_index = 0
        self.processed_ranges = self._load_processed_ranges()

        # iteration state
        self.cursor_page = 0
        self.idx_processed = 0
        self.last_attempted_chunk = None
        self.retry_mode = False


    def _load_processed_ranges(self):
        """Load already processed chunk ranges from the directory."""
        slice_pattern = re.compile(r"chunk-(\d+)-(\d+)\.json$")
        processed = []
        seen = set()
        for filename in os.listdir(self.chunked_results_dir):
            m = slice_pattern.match(filename)
            if m:
                start, end = map(int, m.groups())
                if (start, end) not in seen:
                    processed.append(Chunk(start, end))
                    seen.add((start, end))
        processed.sort()
        return processed


    def next(self):
        """Return the next chunk to process: either a processed one, or a gap."""
        # Retry mode: use last start page with smaller size
        if self.retry_mode and self.last_attempted_chunk:
            size = self.chunk_sizes[self.current_chunk_size_index]
            end_page = min(self.last_attempted_chunk.start + size - 1, self.pages_count)
            chunk = Chunk(self.last_attempted_chunk.start, end_page)
            self.last_attempted_chunk = chunk
            self.retry_mode = False
            return chunk

        while self.cursor_page <= self.pages_count:
            if self.idx_processed < len(self.processed_ranges):
                next_chunk = self.processed_ranges[self.idx_processed]
                if self.cursor_page < next_chunk.start:
                    # gap found
                    size = self.chunk_sizes[self.current_chunk_size_index]
                    end_page = min(self.cursor_page + size - 1, self.pages_count)
                    chunk = Chunk(self.cursor_page, end_page)
                    self.last_attempted_chunk = chunk
                    self.cursor_page = end_page + 1
                    return chunk
                else:
                    # skip processed chunk
                    self.cursor_page = next_chunk.end + 1
                    self.idx_processed += 1
                    return next_chunk
            else:
                # fill until end
                if self.cursor_page <= self.pages_count:
                    size = self.chunk_sizes[self.current_chunk_size_index]
                    end_page = min(self.cursor_page + size - 1, self.pages_count)
                    chunk = Chunk(self.cursor_page, end_page)
                    self.last_attempted_chunk = chunk
                    self.cursor_page = end_page + 1
                    return chunk
        return None

    def decrease_chunk_size(self):
        if self.current_chunk_size_index < len(self.chunk_sizes) - 1:
            self.current_chunk_size_index += 1
            self.retry_mode = True
            return True
        return False

    def mark_success(self, chunk):
        """Record a successfully processed chunk."""
        if chunk not in self.processed_ranges:
            self.processed_ranges.append(chunk)
            self.processed_ranges.sort()

    def verify_complete(self):
        """Check if all pages from 0 to pages_count are covered without gaps."""
        covered = set()
        for chunk in self.processed_ranges:
            covered.update(range(chunk.start, chunk.end + 1))
        missing = [p for p in range(0, self.pages_count + 1) if p not in covered]
        return (len(missing) == 0, missing)
    
    
    
    
class Chunk:
    
    def __init__(self, start, end):
        self.start = start
        self.end = end
    
        
    def __lt__(self, other):
        return (self.start, self.end) < (other.start, other.end)
    
    
    def __le__(self, other):
        return (self.start, self.end) <= (other.start, other.end)
    
    
    def __gt__(self, other):
        return (self.start, self.end) > (other.start, other.end)
    
    
    def __ge__(self, other):
        return (self.start, self.end) >= (other.start, other.end)
    
    
    def __eq__(self, other):
        return (self.start, self.end) == (other.start, other.end)
    
    
    def __repr__(self):
        return f"Chunk({self.start}, {self.end})"
    
    
class PdfExtractor:
    
    
    def __init__(self, gemini_api_key, tasks_queue, config, s3lient, ya_client, channel, stop_event):
        self.key = gemini_api_key
        self.tasks_queue = tasks_queue
        self.config = config
        self.s3lient = s3lient
        self.ya_client = ya_client
        self.channel = channel
        self.stop_event = stop_event
        self.gemini_query_time = None
        
        
    def __call__(self):
        gemini_client = create_client(self.key)
        while not self.stop_event.is_set():
            try: 
                doc = self.tasks_queue.get(block=False)
                self.log(f"Processing doc {doc.md5}({doc.ya_public_url})")
                result = self._extract_doc(doc, gemini_client)
                
                if self.stop_event.is_set() or result.get("stop_worker"):
                    return
                
                if not (context := result.get("context")):
                    continue
                
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
                
                self.log(f"[bold green]Content extraction complete {context.doc.md5}({context.doc.ya_public_url})[/bold green]")
            except Empty:
                self.log("No tasks for processing, shutting down thread...")
                return
            except NoBboxError as e:
                print("No bbox")
                self.channel.add_repairable_doc(e.md5)
            except (JSONDecodeError, RecursionError, IndexError) as e:
                if doc:
                    import traceback
                    print(f"Error:", "\n", e, "\n", traceback.format_exc())
                    self.channel.add_repairable_doc(doc.md5)
            except Exception as e:
                import traceback
                self.log(f"Could not extract content from doc {doc.md5}({doc.ya_public_url}): {e} \n{traceback.format_exc()}")
            
            
    def _extract_doc(self, doc, gemini_client):
        self.log(f"About to download doc {doc.md5}({doc.ya_public_url})")
        local_doc_path = download_file_locally(self.ya_client, doc, self.config)
        self.log(f"Downloaded doc {doc.md5}({doc.ya_public_url})")
        context = Context(doc, local_doc_path)
        self._enrich_context(self.ya_client, context)
        
        unformatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-unformatted.md")
        with pymupdf.open(context.local_doc_path) as pdf_doc, open(unformatted_response_md, "w") as output:
            context.doc_page_count=pdf_doc.page_count
            chunked_results_dir = get_in_workdir(Dirs.CHUNKED_RESULTS, context.md5)
            prev_chunk_tail = None
            headers_hierarchy = []
            next_footnote_num = 1
            
            chunk_planner = ChunkPlanner(chunked_results_dir, pages_count=context.doc_page_count)
            
            while not self.stop_event.is_set():
                usage_meta = None
                
                if not (chunk := chunk_planner.next()):
                    complete, missing_pages = chunk_planner.verify_complete()
                    if not complete:
                        self.log(f"Chunk planner gave none chunks but there are missed pages '{missing_pages}' for doc '{context.md5}'")
                        return {"stop_worker": False}
                    break
                
                chunk_result_complete_path = os.path.join(chunked_results_dir, f"chunk-{chunk.start}-{chunk.end}.json")
                content = None
                
                if os.path.exists(chunk_result_complete_path):
                    self.log(f"Chunk({chunk.start}-{chunk.end})/{context.doc_page_count} of document {context.md5}({context.doc.ya_public_url}) is already extracted")
                    with open(chunk_result_complete_path, "r") as f:
                        deserialized = ExtractionResult.model_validate_json(f.read()).content
                        if not _has_figure_tag_with_missing_attributes(deserialized):
                            content = deserialized

                if not content:
                    self.log(f"Extracting chunk({chunk.start}-{chunk.end})/{context.doc_page_count} of document {context.md5}({context.doc.ya_public_url})")
                    
                    # create a pdf doc what will contain a slice of original pdf doc
                    slice_file_path = self._create_doc_clice(chunk.start, chunk.end, pdf_doc, context.md5)
                
                    if os.path.exists(chunk_result_complete_path): 
                        os.remove(chunk_result_complete_path)
                    chunk_result_incomplete_path = chunk_result_complete_path + ".part"
                    if os.path.exists(chunk_result_incomplete_path): 
                        os.remove(chunk_result_incomplete_path)
                    
                    # prepare prompt
                    prompt = cook_extraction_prompt(chunk.start, chunk.end, next_footnote_num, headers_hierarchy)
                    prompts_dir = get_in_workdir(Dirs.PROMPTS, context.md5)
                    with open(os.path.join(prompts_dir, f"chunk-{chunk.start}-{chunk.end}"), "w") as f:
                        json.dump(prompt, f, indent=4, ensure_ascii=False)
                    
                    # request gemini
                    files = {slice_file_path: "application/pdf"}
                                        
                    self._sleep_if_needed()
                    try:
                        resp = gemini_api(
                            client=gemini_client,
                            model=model,
                            prompt=prompt,
                            files=files,
                            schema=ExtractionResult,
                            timeout_sec=6000
                        )
                        # write result into file
                        with open(chunk_result_incomplete_path, "w") as f:
                            for p in resp:
                                if p.usage_metadata:
                                    usage_meta = p.usage_metadata
                                if text := p.text:
                                    f.write(text)
                                    
                        # validating schema
                        with open(chunk_result_incomplete_path, "r") as f:
                            content = ExtractionResult.model_validate_json(f.read()).content
                            if _has_figure_tag_with_missing_attributes(content):
                                raise ValidationError("Chunk has figure tag with missing attributes")
                            
                        # "mark" batch as extracted by renaming file
                        shutil.move(chunk_result_incomplete_path, chunk_result_complete_path)
                        self.log(f"Chunk ({chunk.start}-{chunk.end})/{context.doc_page_count} of document {context.md5}({context.doc.ya_public_url}) [bold green]extracted successfully[/bold green]: {_tokens_info(usage_meta)}")
                    except ServerError as e:
                        self.log(f"Server error: {e}")
                        self.tasks_queue.put(doc)  # return task to the queue for later processing
                        return {"stop_worker": False}  # continue to the next doc with timeout
                    except (ClientError, ValidationError) as e:
                        self.log(f"Client error: {e}")
                        if isinstance(e, ClientError):
                            self.log(f"Client error during extraction of content of doc {context.md5}({context.doc.ya_public_url}: {e}")
                            message = json.dumps(e.details)
                            # if e.code == 429 and "GenerateRequestsPerDayPerProjectPerModel-FreeTier" in message:
                            if e.code == 429:
                                self.log(f"Free tier limit reached for model {model}, stopping worker...")
                                # return task to the queue for later processing
                                self.tasks_queue.put(doc)
                                # add key to the exceeded keys set
                                self.channel.add_exceeded_key(self.key)
                                return {"stop_worker": True}
                            elif e.code == 429 and "GenerateContentInputTokensPerModelPerMinute-FreeTier" in message:
                                # try to decrease chunk size
                                pass
                            else:
                                print(e)
                                self.channel.add_repairable_doc(context.md5)
                                return {"stop_worker": False}

                        if chunk_planner.decrease_chunk_size():
                            chunk_size = f"with size {chunk.end - chunk.start + 1}" if chunk else ""
                            self.log(f"Could not extract chunk {chunk_size} of doc {context.md5}({context.doc.ya_public_url}){_tokens_info(usage_meta)}")
                            continue
                        else:
                            self.channel.add_unprocessable_doc(context.md5)
                            self.log(f"Could not extract chunk with any size of doc {context.md5}({context.doc.ya_public_url}){_tokens_info(usage_meta)}")
                            return {"stop_worker": False}

                    chunk_planner.mark_success(chunk)
                    
                # shift footnotes up in the content to avoid heaving footnote text at the brake between slices
                # content = self._shift_trailing_footnotes_up(content)
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

                context.add_chunk_path(chunk_result_complete_path)
                
        context.unformatted_response_md = unformatted_response_md
        return {"context": context, "stop_worker": False}
    
    
    def _sleep_if_needed(self):
        now = datetime.datetime.now()
        if self.gemini_query_time:
            elapsed = now - self.gemini_query_time
            if elapsed < datetime.timedelta(minutes=1):
                time_to_sleep = int(60 - elapsed.total_seconds()) + 1
                self.log(f"Sleeping for {time_to_sleep} seconds")
                time.sleep(time_to_sleep)
        self.gemini_query_time = now
    
    
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
        ya_doc_meta = ya_client.get_public_meta(context.doc.ya_public_url, fields=['md5', 'path', 'public_key', 'resource_id', 'sha256'])
        context.md5 = ya_doc_meta.md5
        context.ya_path = ya_doc_meta.path
        context.ya_public_key = ya_doc_meta.public_key
        context.ya_resource_id = ya_doc_meta.resource_id
        
        
    def log(self, message):
        message = f"{threading.current_thread().name} {time.strftime('%d-%m-%y %H:%M:%S')} {self.key[-7:]}: {message}"
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
        self.log(f"Uploading artifacts to object storage {context.doc.md5}({context.doc.ya_public_url})")
                    
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
        if context.ya_path and (ya_path := context.ya_path.removeprefix('disk:')) != '/':
            doc.ya_path = ya_path
        doc.ya_public_key=context.ya_public_key
        doc.ya_resource_id=context.ya_resource_id

        doc.content_extraction_method=context.extraction_method
        doc.document_url = encrypt(context.remote_doc_url, self.config) if doc.sharing_restricted else context.remote_doc_url
        doc.content_url = context.remote_content_url
            
        self.log(f"Updating doc details in gsheets {context.doc.md5}({context.doc.ya_public_url})")
        gsheets_session.update(doc)


def _has_figure_tag_with_missing_attributes(content):
    for match in FIGURE_TAG_PATTERN.finditer(content):
        tag = match.group(0)
        if 'data-bbox=' not in tag:
            print(f"Attribute `data-bbox` is missing")
            return True
        if 'data-page=' not in tag:
            print(f"Attribute `data-page` is missing")
            return True
    return False
    

def _tokens_info(usage_meta):
    if usage_meta:
        return f"input tokens:{usage_meta.prompt_token_count}, output tokens: {usage_meta.candidates_token_count}, total tokens: {usage_meta.total_token_count}"
    return ""
               