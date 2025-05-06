from utils import get_in_workdir
from dirs import Dirs
from itertools import batched
import pymupdf
from prompt import EXTRACT_CONTENT_PROMPT
import zipfile
from gemini import request_gemini, create_client
from schema import ExtractionResult
import mdformat
import re
from prepare_shots import load_inline_shots


# Content extraction checklist:
# - book in Tatar language
# - book does not have special chars, eg diacritic
# - all tables are oriented horizontally
# - all pages aligned horizontally

# todo postprocess output: remove page delimeters, fix headers hierarchy
# todo more shots: between pages, footnotes
# todo preview returned markdown
# todo upload inline shots in advance?
def extract(context):
    client = create_client("promo")
    with pymupdf.open(context.local_doc_path) as pdf_doc:
        _extract_content(context, pdf_doc, client)
        context.extraction_method = f"{context.cli_params.model}/{context.cli_params.batch_size}/noimg"
        context.doc_page_count=pdf_doc.page_count

def _extract_content(context, pdf_doc, client):
    batch_size = context.cli_params.batch_size
    iter = list(batched(range(0, pdf_doc.page_count)[context.cli_params.page_slice], batch_size))

    last_chunk_page = None
    context.formatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-formatted.md")
    unformatted_response_json = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.json")
    unformatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-unformatted.md")
    next_footnote_num = 1
    with open(context.formatted_response_md , "w", encoding="utf-8") as formatted, open(unformatted_response_json , "w", encoding="utf-8") as unformatted_json, open(unformatted_response_md , "w", encoding="utf-8") as unformatted_md:
        unformatted_json.write("[")
        for idx, batch in enumerate(context.progress.track_extraction(iter, f"Extracting content in batches of size '{batch_size}'"), start=1):
            # create a pdf doc what will contain a slice of original pdf doc
            slice_from = batch[0]
            slice_to = batch[-1]
            doc_slice = pymupdf.open()
            doc_slice.insert_pdf(pdf_doc, from_page=slice_from, to_page=slice_to)
            slice_file_path = get_in_workdir(Dirs.DOC_SLICES, context.md5, file=f"slice-{slice_from}-{slice_to}.pdf")
            doc_slice.save(slice_file_path)
            
            # prepare prompt
            prompt = _prepare_prompt(slice_from, slice_to, next_footnote_num, last_chunk_page)
            
            # request gemini
            files = {slice_file_path: "application/pdf"}
            response = request_gemini(client=client, model=context.cli_params.model, prompt=prompt, files=files, schema=ExtractionResult)
            
            raw_content = ''
            for chunk in response:
                if text := chunk.text:
                    raw_content += text
                    
            # write raw response in json 
            unformatted_json.write(raw_content)
            if idx < len(iter):
                unformatted_json.write(",\n")
            
            # validate response 
            extraction_result = ExtractionResult.model_validate_json(raw_content)
            
            # write down variant vefore preprocessing for debugging and observability purpose 
            unformatted_md.write(extraction_result.content)
            
            # postprocess response
            formatted_content, last_footnote_num = _post_process(extraction_result)
            next_footnote_num = last_footnote_num + 1
            
            # write down processed result
            formatted.write(formatted_content)
            
            context.tokens.append(chunk.usage_metadata.total_token_count)
        formatted.write('\n\n---\n')
        unformatted_json.write("]")


    mdformat.file(
        context.formatted_response_md,
        codeformatters=(),
        extensions=["toc", "footnote"],
        options={"wrap": "keep", "number": "keep", "validate": True, "end_of_line": "lf"},
    )

    context.local_content_path = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.zip")
    with zipfile.ZipFile(context.local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.write(arcname=f"{context.md5}.md", filename=context.formatted_response_md)
        
def _prepare_prompt(slice_from, slice_to, footnote_counter, last_chunk_page=None):
    prompt = [{"text": EXTRACT_CONTENT_PROMPT.format(slice_to=slice_to, slice_from=slice_from, footnote_start=footnote_counter)}]
    print(prompt)
    if slice_from:
        # does not contain document title, so all headers should be '##' or deeper
        prompt.append({"text": "📌 Document does not have a title page, so use ## for the highest-level headings, ### for subsections, and so on. Never use a single #. Always preserve the heading hierarchy based on the document's logical structure."})
    else:
        # may contain document title
        prompt.append({"text": "📌 Document may contain a main title. If you detect a main document title mark it with a single #. Use ## for top-level sections, ### for subsections, and so on. Always preserve the heading hierarchy based on the document's logical structure."})
    
    if last_chunk_page:
        prompt.append({"text": "📌 The last page of the previous chunk is attached. Use it to properly continue any broken sentences or structures at the beginning of the current chunk. Process the continuation according to all other instructions. The content of the previous chunk's last page is provided here for your reference:"})
        prompt.append({"text": last_chunk_page})
        
    prompt.extend(load_inline_shots())
    prompt.append({"text": "Now, extract structured content from the following document"})
    
    return prompt

def _post_process(extraction_result):
    postprocessed = extraction_result.content.replace("\\n", "\n").replace('-', '')
    # page = re.sub(r"<figure>\s*<img[^>]*\/>\s*(<figcaption>.*?<\/figcaption>)?\s*<\/figure>", "", page, flags=re.DOTALL)
    postprocessed = re.sub(r'<table\s+class="toc">.*?</table>','<!-- mdformat-toc start --no-anchors -->', postprocessed, flags=re.DOTALL)
    
    footnote_counters = re.findall(r"\[\^(\d+)\]:", postprocessed)
    max_footnote_counter = max(map(int, footnote_counters)) if footnote_counters else 0
    return postprocessed, max_footnote_counter
        