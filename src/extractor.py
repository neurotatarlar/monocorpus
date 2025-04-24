from utils import get_in_workdir
from dirs import Dirs
from itertools import batched, groupby
from google import genai
from google.genai import types
import pymupdf
from prompt import EXTRACT_CONTENT_PROMPT, DEFINE_METADATA_PROMPT
import mdformat
from schema_org import Book
import zipfile
import base64
import json
import datetime
import os

# Content extraction checklist:
# - book in Tatar language
# - book does not have special chars, eg diacritic
# - all tables are oriented horizontally
# - all pages aligned horizontally


def extract(context):
    client = genai.Client(api_key=context.config['google_api_key'])
    with pymupdf.open(context.local_doc_path) as pdf_doc:
        if context.cli_params.meta:
            _extract_metadata(context, pdf_doc, client)
        _extract_content(context, pdf_doc, client)
        context.extraction_method = context.cli_params.model
        context.doc_page_count=pdf_doc.page_count

def _extract_metadata(context, pdf_doc, client):
    context.progress.operational(f"Extracting metadata of doc")

    def _ranges(_i):
        for _, _b in groupby(enumerate(_i), lambda pair: pair[1] - pair[0]):
            _b = list(_b)
            yield _b[0][1], _b[-1][1]

    doc_slice = pymupdf.open()
    pages = list(range(0, pdf_doc.page_count))
    pages = set(pages[:5] + pages[-3:])
    for start, end in list(_ranges(pages)):
        doc_slice.insert_pdf(pdf_doc, from_page=start, to_page=end)

    slice_file_path = get_in_workdir(Dirs.DOC_SLICES, context.md5, file=f"slice-of-pages-{'-'.join([str(i) for i in pages])}.pdf")
    doc_slice.save(slice_file_path)

    response = _interact_with_gemini(
        client=client, file_path=slice_file_path, prompt=DEFINE_METADATA_PROMPT, model=context.cli_params.model, schema=Book
    )
    metadata = Book.model_validate_json("".join([ch.text for ch in response]))
    context.metadata = metadata

    # write metadata to zip
    context.local_meta_path = get_in_workdir(Dirs.METADATA, file=f"{context.md5}.zip")
    with zipfile.ZipFile(context.local_meta_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        meta_json = metadata.model_dump_json(indent=None, by_alias=True, exclude_none=True, exclude_unset=True)
        zf.writestr("metadata.json", meta_json)

def _extract_content(context, pdf_doc, client):
    context.local_content_path_raw = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.md")
    model = context.cli_params.model
    batch_size = context.cli_params.batch_size
    iter = list(batched(range(0, pdf_doc.page_count)[context.cli_params.page_slice], batch_size))

    with open(context.local_content_path_raw , "w") as result_file:
        for batch in context.progress.track_extraction(iter, f"Extracting content in batches of size '{batch_size}'"):
            # create a pdf doc what will contain a slice of original pdf doc
            doc_slice = pymupdf.open()
            doc_slice.insert_pdf(
                pdf_doc, from_page=batch[0], to_page=batch[-1])
            slice_file_path = get_in_workdir(
                Dirs.DOC_SLICES, context.md5, file=f"slice-of-pages-{'-'.join([str(i) for i in batch])}.pdf")
            doc_slice.save(slice_file_path)
            response = _interact_with_gemini(client=client, file_path=slice_file_path, model=model)
            tokens = 0
            for chunk in response:
                if text := chunk.text:
                    result_file.write(text)
                tokens = chunk.usage_metadata.total_token_count
            context.tokens.append(tokens)
        result_file.flush()

    mdformat.file(context.local_content_path_raw)
    
    context.local_content_path = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.zip")
    with zipfile.ZipFile(context.local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.write(arcname=f"{context.md5}.md", filename=context.local_content_path_raw)


def _interact_with_gemini(client, file_path, model, schema=None, shots=None, prompt=None):
    file = client.files.upload(
        file=file_path,
        config={"mime_type": "application/pdf"}
    )
    # contents = [file, shots, {"text": "Extract all the text from the PDF document using the same approach:"}]
    # if shots:
    #     contents.append(shots)
    # if prompt:
    #     contents.append(prompt)
        
    # contents.append(file)
    # print(contents)
    contents = [
        {"text": EXTRACT_CONTENT_PROMPT},
    ]
    contents.append({"text": "Here are examples of how to extract content from a document"})
    for idx, (image, ground_truth) in enumerate(_load_pairs()):
        contents.append({"text": f"Example {idx+1} Image:"})
        contents.append({"inline_data": {
            "data": image,
            "mime_type": "image/png",
        }})
        contents.append({"text": f"Markdown formatted Example {idx+1} Ground Truth:\n{ground_truth}"})
    contents.append({"text": "Now, extract all the text from the following document using the same approach:"})
    contents.append(file)
    
    return client.models.generate_content_stream(
        model=model,
        contents=contents,
        # docs https://ai.google.dev/gemini-api/docs/text-generation#configuration-parameters
        config=types.GenerateContentConfig(
            temperature=0.1,
            # topK=1,
            response_mime_type="application/json" if schema else None,
            response_schema=schema,
            candidate_count=1,
            seed=1552,
        )
    )
    
   
def _upload_shots(client, shots_file_name="shots-file"):
    file = None
    try:
        file = client.files.get(name=shots_file_name)
        print("File found")
    except genai.errors.ClientError as e:
        if e.code != 403:
            raise e
    
    if file and file.expiration_time and file.expiration_time - datetime.datetime.now(datetime.UTC) < datetime.timedelta(days=30):
        print("File is about to expire, deleting it")
        client.files.delete(name=shots_file_name)
        file = None
    if not file:
        print("File not found, uploading a new one")
        if not os.path.exists(shots_file_name):
            _create_shots_file(shots_file_name)
        file = client.files.upload(
            file=shots_file_name,
            config=types.UploadFileConfig(
                mime_type="application/json",
                name=shots_file_name,
            ),
        )
    return file

def _create_shots_file(shots_file_name):
    pairs = _load_pairs()
    shots = [{"text": "Here are examples of how to extract content from a document"}]
    for idx, (image, ground_truth) in enumerate(pairs):
        shots.append({"text": f"Example {idx+1} Image:"})
        shots.append({"inline_data": {
            "data": image,
            "mime_type": "image/png",
        }})
        shots.append({"text": f"Example {idx+1} Ground Truth:\n```markdown{ground_truth}```"})
    shots.append({"text": "Now, extract all the text from the following document using the same approach:"})
    result = json.dumps(shots, indent=4, ensure_ascii=False)
    with open(shots_file_name, "w") as f:
        f.write(result)
    
def _load_pairs():
    import os
    for i in os.listdir("./shots"):
        # check if file 
        if i.endswith(".png"):
            image_path = os.path.join("./shots", i)
            with open(image_path, "rb") as f:
                image = base64.b64encode(f.read()).decode("utf-8")
            with open(os.path.join("./shots", i.replace(".png", ".md")), "r") as f:
                ground_truth = f.read()
            yield image, ground_truth