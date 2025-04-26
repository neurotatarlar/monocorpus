from utils import get_in_workdir
from dirs import Dirs
from itertools import batched
from google import genai
from google.genai import types
import pymupdf
from prompt import EXTRACT_CONTENT_PROMPT
import mdformat
import zipfile
from gemini import request_gemini
from schema import ExtractionResult

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
    context.local_content_path_raw = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.md")
    batch_size = context.cli_params.batch_size
    iter = list(batched(range(0, pdf_doc.page_count)[context.cli_params.page_slice], batch_size))

    with open(context.local_content_path_raw , "w") as result_file:
        for batch in context.progress.track_extraction(iter, f"Extracting content in batches of size '{batch_size}'"):
            # create a pdf doc what will contain a slice of original pdf doc
            slice_from = batch[0]
            slice_to = batch[-1]
            doc_slice = pymupdf.open()
            doc_slice.insert_pdf(pdf_doc, from_page=slice_from, to_page=slice_to)
            slice_file_path = get_in_workdir(Dirs.DOC_SLICES, context.md5, file=f"slice-{slice_from}-{slice_to}.pdf")
            doc_slice.save(slice_file_path)
            
            # prepare prompt
            prompt = _prepare_prompt(slice_from)
            
            # request gemini
            files = {slice_file_path: "application/pdf"}
            response = request_gemini(client=client, model=context.cli_params.model, prompt=prompt, files=files, schema=ExtractionResult)
            
            tokens = 0
            for chunk in response:
                if text := chunk.text:
                    result_file.write(text)
                tokens = chunk.usage_metadata.total_token_count
            context.tokens.append(tokens)
            
            result_file.flush()
            exit(0)
        result_file.flush()

    # mdformat.file(context.local_content_path_raw)
    
    context.local_content_path = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.zip")
    with zipfile.ZipFile(context.local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.write(arcname=f"{context.md5}.md", filename=context.local_content_path_raw)

def _prepare_prompt(slice_from, last_page=None):
    prompt = [{"text": EXTRACT_CONTENT_PROMPT.strip()}]
    if slice_from:
        # does not contain document title, so all headers should be '##' or deeper
        prompt.append({"text": "📌 Document does not have a title page, so use ## for the highest-level headings, ### for subsections, and so on. Never use a single #. Always preserve the heading hierarchy based on the document's logical structure."})
    else:
        # may contain document title
        prompt.append({"text": "📌 Document may contain a main title. If you detect a main document title mark it with a single #. Use ## for top-level sections, ### for subsections, and so on. Always preserve the heading hierarchy based on the document's logical structure."})
    
    if last_page:
        prompt.append({
            "text": "📌 The last page of the previous chunk is attached. Use it to properly continue any broken sentences or structures at the beginning of the current chunk. Process the continuation according to all other instructions. The content of the previous chunk's last page is provided here for your reference:"
        })
        prompt.append({
            "text": last_page
        })
        
    prompt.append({"text": "Now, extract structured content from the following document"})
    return prompt

   
# def _upload_shots(client, shots_file_name="shots-file"):
#     file = None
#     try:
#         file = client.files.get(name=shots_file_name)
#         print("File found")
#     except genai.errors.ClientError as e:
#         if e.code != 403:
#             raise e
    
#     if file and file.expiration_time and file.expiration_time - datetime.datetime.now(datetime.UTC) < datetime.timedelta(days=30):
#         print("File is about to expire, deleting it")
#         client.files.delete(name=shots_file_name)
#         file = None
#     if not file:
#         print("File not found, uploading a new one")
#         if not os.path.exists(shots_file_name):
#             _create_shots_file(shots_file_name)
#         file = client.files.upload(
#             file=shots_file_name,
#             config=types.UploadFileConfig(
#                 mime_type="application/json",
#                 name=shots_file_name,
#             ),
#         )
#     return file

# def _create_shots_file(shots_file_name):
#     pairs = _load_pairs()
#     shots = [{"text": "Here are examples of how to extract content from a document"}]
#     for idx, (image, ground_truth) in enumerate(pairs):
#         shots.append({"text": f"Example {idx+1} Image:"})
#         shots.append({"inline_data": {
#             "data": image,
#             "mime_type": "image/png",
#         }})
#         shots.append({"text": f"Example {idx+1} Ground Truth:\n```markdown{ground_truth}```"})
#     shots.append({"text": "Now, extract all the text from the following document using the same approach:"})
#     result = json.dumps(shots, indent=4, ensure_ascii=False)
#     with open(shots_file_name, "w") as f:
#         f.write(result)
    
# def _load_pairs():
#     import os
#     for i in os.listdir("./shots"):
#         # check if file 
#         if i.endswith(".png"):
#             image_path = os.path.join("./shots", i)
#             with open(image_path, "rb") as f:
#                 image = base64.b64encode(f.read()).decode("utf-8")
#             with open(os.path.join("./shots", i.replace(".png", ".md")), "r") as f:
#                 ground_truth = f.read()
#             yield image, ground_truth