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


# Content extraction checklist:
# - book in Tatar language
# - book does not have special chars, eg diacritic
# - all tables are oriented horizontally
# - all pages aligned horizontally
BATCH_SIZE = 3


def extract(context):
    client = genai.Client(api_key=context.config['google_api_key'])
    with pymupdf.open(context.local_doc_path) as pdf_doc:
        _define_metadata(context, pdf_doc, client)
        _extract_content(context, pdf_doc, client)
        context.extraction_method = "gemini"
        context.doc_page_count=pdf_doc.page_count


def _define_metadata(context, pdf_doc, client):
    context.progress.main(f"Extracting metadata of doc")

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
        client, slice_file_path, DEFINE_METADATA_PROMPT, Book)
    metadata = Book.model_validate_json("".join([ch.text for ch in response]))
    context.metadata = metadata

    # write metadata to zip
    context.local_meta_path = get_in_workdir(Dirs.METADATA, file=f"{context.md5}.zip")
    with zipfile.ZipFile(context.local_meta_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        meta_json = metadata.model_dump_json(indent=None, by_alias=True, exclude_none=True, exclude_unset=True)
        zf.writestr("metadata.json", meta_json)

def _extract_content(context, pdf_doc, client):
    result_path = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.md")
    iter = list(batched(range(0, pdf_doc.page_count), BATCH_SIZE))

    with open(result_path , "w") as result_file:
        for batch in context.progress.track_extraction(iter, f"Calling Gemini to extract content in batches of size {BATCH_SIZE}"):
            # create a pdf doc what will contain a slice of original pdf doc
            doc_slice = pymupdf.open()
            doc_slice.insert_pdf(
                pdf_doc, from_page=batch[0], to_page=batch[-1])
            slice_file_path = get_in_workdir(
                Dirs.DOC_SLICES, context.md5, file=f"slice-of-pages-{'-'.join([str(i) for i in batch])}.pdf")
            doc_slice.save(slice_file_path)
            response = _interact_with_gemini(
                client, slice_file_path, EXTRACT_CONTENT_PROMPT)
            for chunk in response:
                result_file.write(chunk.text)
        result_file.flush()

    mdformat.file(result_path)
    
    context.local_content_path = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.zip")
    with zipfile.ZipFile(context.local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.write(arcname=f"{context.md5}.md", filename=result_path)


def _interact_with_gemini(client, file_path, prompt, schema=None):
    file = client.files.upload(
        file=file_path,
        config={
            "mime_type": "application/pdf",
        }
    )
    return client.models.generate_content_stream(
        model='gemini-2.0-flash',
        # model='Gemini 2.5 Flash Preview 04-17',
        contents=[file, prompt],
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
