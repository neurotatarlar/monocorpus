from utils import get_in_workdir
from dirs import Dirs
from itertools import batched
from google import genai
import pymupdf
from prompt import PROMPT
import mdformat

BATCH_SIZE = 4


def extract(context):
    result_path = get_in_workdir(Dirs.CONTENT, file=f"{context.hash}.md")

    with pymupdf.open(context.path_to_doc) as pdf_doc, open(result_path, "w") as result_file:

        client = genai.Client(api_key=context.config['google_api_key'])
        iter = list(batched(range(0, pdf_doc.page_count), BATCH_SIZE))

        for batch in context.progress.track_extraction(iter, f"Calling Gemini to extract content in batches of size {BATCH_SIZE}"):
            # create a pdf doc what will contain a slice of original pdf doc
            doc_slice = pymupdf.open()
            doc_slice.insert_pdf(
                pdf_doc, from_page=batch[0], to_page=batch[-1])
            slice_file_path = get_in_workdir(
                Dirs.DOC_SLICES, context.hash, file=f"slice-of-pages-{'-'.join([str(i) for i in batch])}.pdf")
            doc_slice.save(slice_file_path)

            # upload file in advance
            file = client.files.upload(
                file=slice_file_path,
                config={
                    "mime_type": "application/pdf",
                }
            )
            # request Gemini
            response = client.models.generate_content(
                # model='gemini-2.0-flash',
                model='gemini-2.5-pro-exp-03-25',
                contents=[file, PROMPT]
            )
            result_file.write(response.text.removeprefix(
                "```markdown").removesuffix("```"))
            result_file.flush()

    # lint
    mdformat.file(result_path)
