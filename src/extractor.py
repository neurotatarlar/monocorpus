from utils import get_in_workdir
from dirs import Dirs
from itertools import batched
from google import genai
import pymupdf
from prompt import EXTRACT_CONTENT_PROMPT
import zipfile
from gemini import request_gemini
from schema import ExtractionResult
import base64
import os
import mdformat
import re
# import mdformat_footnote
# import mdformat_toc


# Content extraction checklist:
# - book in Tatar language
# - book does not have special chars, eg diacritic
# - all tables are oriented horizontally
# - all pages aligned horizontally

# todo normalize headers
# todo optimize uploading of data to gemini
# todo postprocess output: remove page delimeters, fix headers hierarchy
# todo more shots: between pages, footnotes
# todo preview returned markdown
# todo replace toc
# 
def extract(context):
    client = genai.Client(api_key=context.config['google_api_key'])
    with pymupdf.open(context.local_doc_path) as pdf_doc:
        _extract_content(context, pdf_doc, client)
        context.extraction_method = f"{context.cli_params.model}/{context.cli_params.batch_size}"
        context.doc_page_count=pdf_doc.page_count

def _extract_content(context, pdf_doc, client):
    batch_size = context.cli_params.batch_size
    iter = list(batched(range(0, pdf_doc.page_count)[context.cli_params.page_slice], batch_size))

    last_chunk_page = None
    context.formatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.md")
    unformatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-unformatted.json")
    with open(context.formatted_response_md , "w", encoding="utf-8") as formatted, open(unformatted_response_md , "w", encoding="utf-8") as unformatted:
        unformatted.write("[")
        for idx, batch in enumerate(context.progress.track_extraction(iter, f"Extracting content in batches of size '{batch_size}'"), start=1):
            # create a pdf doc what will contain a slice of original pdf doc
            slice_from = batch[0]
            slice_to = batch[-1]
            doc_slice = pymupdf.open()
            doc_slice.insert_pdf(pdf_doc, from_page=slice_from, to_page=slice_to)
            slice_file_path = get_in_workdir(Dirs.DOC_SLICES, context.md5, file=f"slice-{slice_from}-{slice_to}.pdf")
            doc_slice.save(slice_file_path)
            
            # prepare prompt
            prompt = _prepare_prompt(slice_from, last_chunk_page)
            
            # request gemini
            files = {slice_file_path: "application/pdf"}
            response = request_gemini(client=client, model=context.cli_params.model, prompt=prompt, files=files, schema=ExtractionResult)
            
            unformatted_content = ''
            for chunk in response:
                if text := chunk.text:
                    unformatted_content += text
            context.tokens.append(chunk.usage_metadata.total_token_count)
            unformatted.write(unformatted_content)
            if idx < len(iter):
                unformatted.write(",\n")
            formatted_content, last_chunk_page = _post_process(unformatted_content)
            formatted.write(formatted_content)
        unformatted.write("]")


    mdformat.file(
        context.formatted_response_md,
        codeformatters=(),
        extensions=["toc", "footnote"],
        options={"wrap": "keep", "number": "keep", "validate": True, "end_of_line": "lf"},
    )

    context.local_content_path = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.zip")
    with zipfile.ZipFile(context.local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.write(arcname=f"{context.md5}.md", filename=context.formatted_response_md)
        
def _prepare_prompt(slice_from, last_chunk_page=None):
    prompt = [{"text": EXTRACT_CONTENT_PROMPT.strip()}]
    if slice_from:
        # does not contain document title, so all headers should be '##' or deeper
        prompt.append({"text": "📌 Document does not have a title page, so use ## for the highest-level headings, ### for subsections, and so on. Never use a single #. Always preserve the heading hierarchy based on the document's logical structure."})
    else:
        # may contain document title
        prompt.append({"text": "📌 Document may contain a main title. If you detect a main document title mark it with a single #. Use ## for top-level sections, ### for subsections, and so on. Always preserve the heading hierarchy based on the document's logical structure."})
    
    if last_chunk_page:
        prompt.append({
            "text": "📌 The last page of the previous chunk is attached. Use it to properly continue any broken sentences or structures at the beginning of the current chunk. Process the continuation according to all other instructions. The content of the previous chunk's last page is provided here for your reference:"
        })
        prompt.append({
            "text": last_chunk_page
        })
        
    prompt.append({"text": "Here are examples of how to extract content from a document:"})
    for idx, (image, ground_truth) in enumerate(_load_pairs("./shots/general")):
        prompt.append({"text": f"Example {idx+1} Image:"})
        prompt.append({"inline_data": {
            "data": image,
            "mime_type": "image/png",
        }})
        prompt.append({"text": f"✅ Example {idx+1} Ground Truth:\n```markdown\n{ground_truth}```"})
        
    prompt.append({"text": "Additional Examples: Handling Content Across Page Breaks. These examples illustrate how to correctly handle paragraphs and tables that continue across pages in this chunk. Follow these conventions to ensure structural continuity and preserve reading flow."})
    for idx, (image1, image2, ground_truth) in enumerate(_load_triplets("./shots/parapgraphs")):
        prompt.append({"text": f"Example {idx+1} Paragraph continued on the next page:"})
        prompt.append({"text": f"Previous page"})
        prompt.append({"inline_data": {
            "data": image1,
            "mime_type": "image/png",
        }})
        prompt.append({"text": f"Current page"})
        prompt.append({"inline_data": {
            "data": image2,
            "mime_type": "image/png",
        }})
        prompt.append({"text": f"✅ Example {idx+1} Ground Truth:\n```markdown\n{ground_truth}```"})
    prompt.append({"text": "Summary: **Do not** insert a blank line if the paragraph is continuing. Merge seamlessly and naturally"})    
    
        
    for idx, (image1, image2, ground_truth) in enumerate(_load_triplets("./shots/tables")):
        prompt.append({"text": f"Example {idx+1} Table continued on the next page:"})
        prompt.append({"text": f"Previous page"})
        prompt.append({"inline_data": {
            "data": image1,
            "mime_type": "image/png",
        }})
        prompt.append({"text": f"Current page"})
        prompt.append({"inline_data": {
            "data": image2,
            "mime_type": "image/png",
        }})
        prompt.append({"text": f"✅ Example {idx+1} Ground Truth:\n```markdown\n{ground_truth}```"})
        
    prompt.append({"text": "Summary: **Do not** restart the table if it continues from a previous page. Just append the rows inside the same block."})    
        
    prompt.append({"text": "Now, extract structured content from the following document"})
    return prompt

    
def _load_pairs(rel_path):
    for i in os.listdir(rel_path):
        # check if file 
        if i.endswith(".png"):
            image_path = os.path.join(rel_path, i)
            with open(image_path, "rb") as f:
                image = base64.b64encode(f.read()).decode("utf-8")
            with open(os.path.join(rel_path, i.replace(".png", ".md")), "r") as f:
                ground_truth = f.read()
            yield image, ground_truth
            
def _load_triplets(dir):
    files = os.listdir(dir)
    png_files = sorted([f for f in files if f.endswith('.png')])
    md_files = sorted([f for f in files if f.endswith('.md')])
    
    for md_file in md_files:
        # Extract index from md filename (e.g., '0.md' -> 0)
        index = int(os.path.splitext(md_file)[0])

        prev_image = f"{index:02d}.png"
        curr_image = f"{index+1:02d}.png"

        if prev_image in png_files and curr_image in png_files:
            with open(os.path.join(dir, prev_image), "rb") as f:
                _prev_image =  base64.b64encode(f.read()).decode("utf-8")
            with open(os.path.join(dir, curr_image), "rb") as f:
                _curr_image =  base64.b64encode(f.read()).decode("utf-8")
            with open(os.path.join(dir, md_file), "r") as f:
                ground_truth = f.read()
            yield _prev_image, _curr_image, ground_truth
        else:
            print(f"Warning: missing images for {md_file}")
            
def _post_process(unformatted):
    formatted = ""
    unformatted = unformatted.replace("\\n", "\n")
    pages = re.findall(r'<!--\s*page start\s*-->(.*?)<!--\s*page end\s*-->', unformatted, re.DOTALL)
    unprocessed_page = None
    for page in [page for page in pages]:
        unprocessed_page = page
        page = page.rstrip(" \t\n\r-")
        page = re.sub(r"<figure>\s*<img[^>]*\/>\s*(<figcaption>.*?<\/figcaption>)?\s*<\/figure>", "", page, flags=re.DOTALL)
        page = re.sub(r'<table\s+class="toc">.*?</table>','<!-- mdformat-toc start --no-anchors -->', page, flags=re.DOTALL)
        formatted += page
    return formatted, unprocessed_page
        