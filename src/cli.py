import typer
from typing_extensions import Annotated
from typing import Optional
from dataclasses import dataclass
import string

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})

# slice_pattern = re.compile(r'^(?P<start>-?\d*)?:?(?P<stop>-?\d*)?:?(?P<step>-?\d*)?$')

# @dataclass
# class ExtractPdfParams:
#     md5: str
#     path: str
#     force: bool
#     page_slice: str
#     batch_size: int
#     model: str
#     limit: int
    
@dataclass
class ExtractParams:
    md5: str
    path: str
    batch_size: int
    workers: int

@dataclass
class CliParams:
    md5: str
    path: str

# def slice_parser(value: str):
#     if value:
#         match = slice_pattern.match(value)
#         if not match:
#             raise typer.BadParameter(f"Invalid slice string: `{value}`")

#         start = match.group('start')
#         stop = match.group('stop')
#         step = match.group('step')

#         # Convert to integers if they are not empty, otherwise use None
#         start = int(start) if start else None
#         stop = int(stop) if stop else None
#         step = int(step) if step else None

#         return slice(start, stop, step)
#     return slice(0, None, 1)


def md5_validator(value: str):
    if value:
        if len(value) != 32:
            raise typer.BadParameter("MD5 should be 32 characters long")
        value = value.lower()
        if not all(ch in string.hexdigits for ch in value):
            raise typer.BadParameter("MD5 should be a hex string")
    return value


@app.command()
def sync():
    """
    Synchronize documents between Yandex Disk and Google Sheets.

    This command traverses files and directories in Yandex Disk, identifies new or updated entries, 
    and uploads them to Google Sheets. It ensures that the local and remote data are in sync, 
    facilitating seamless integration and data management.
    """
    import sync
    sync.sync()


# @extract_app.command(name="pdf")
# def extract_pdf(
#     md5: Annotated[
#         Optional[str],
#         typer.Option(
#             "--md5",
#             callback=md5_validator,
#             help="MD5 hash of the document. If not provided, all local documents will be processed."
#         )
#     ] = None,
#     path: Annotated[
#         Optional[str],
#         typer.Option(
#             "--path", "-p",
#             help="Path to the document or directory in yandex disk"
#         )
#     ] = None,
#     force: Annotated[
#         bool,
#         typer.Option(
#             "--force", "-f",
#             help="Force the processing even if the document is already sent for annotation"
#         )
#     ] = False,
#     pages_slice: Annotated[
#         Optional[str],
#         typer.Option(
#             "--slice", "-s",
#             parser=slice_parser,
#             help="Slice of the pages to process. Format: `start:stop:step`. If not provided, all pages will be processed."
#         )
#     ] = "::",
#     batch_size: Annotated[
#         int,
#         typer.Option(
#             "--batch-size", "-b",
#             help="Batch size for processing pages",
#         )
#     ] = 20,
#     model: Annotated[
#         str,
#         typer.Option(
#             "--model", "-m",
#             help="Model to use for processing. See available models here: https://ai.google.dev/gemini-api/docs/models",
#         )
#     ] = "gemini-2.5-pro",
#     limit: Annotated[
#         int,
#         typer.Option(
#             "--limit", "-l",
#             help="Limit processed documents. If not provided, than all unprocessed documents will be taken",
#         )
#     ] = None,
# ):
#     from content.pdf import extract
#     cli_params = ExtractPdfParams(
#         md5=md5,
#         path=path,
#         force=force,
#         page_slice=pages_slice,
#         batch_size=batch_size,
#         model=model,
#         limit=limit,
#     )
#     extract(cli_params)


# @extract_app.command(name="epub")
# def extract_epub(
#     md5: Annotated[
#         Optional[str],
#         typer.Option(
#             "--md5",
#             callback=md5_validator,
#             help="MD5 hash of the document. If not provided, all local documents will be processed."
#         )
#     ] = None,
#     path: Annotated[
#         Optional[str],
#         typer.Option(
#             "--path", "-p",
#             help="Path to the document or directory in yandex disk"
#         )
#     ] = None,
#     limit: Annotated[
#         int,
#         typer.Option(
#             "--limit", "-l",
#             help="Limit processed documents. If not provided, than all unprocessed documents will be taken",
#         )
#     ] = None,
# ):
#     from content.epub import extract
#     cli_params = ExtractEpubParams(
#         md5=md5, 
#         path=path,
#         limit=limit
#     )
#     extract(cli_params)
    
# @extract_app.command(name="docx")
# def extract_docx():
#     from content.docx import extract
#     extract()
    
    
@app.command()
def hf():
    """
    Assemble structured dataset from content files stored in S3.
    """
    import hf 
    hf.assemble_dataset()
    
    
@app.command()
def meta():
    """
    Extract and normalize metadata for documents.
    """
    import metadata
    metadata.extract_metadata()
    
    
@app.command()
def extract(
    md5: Annotated[
        Optional[str],
        typer.Option(
            "--md5",
            callback=md5_validator,
            help="MD5 hash of the document. If not provided, all local documents will be processed."
        )
    ] = None,
    path: Annotated[
        Optional[str],
        typer.Option(
            "--path", "-p",
            help="Path to the document or directory in yandex disk. If not provided, all yandex disk will be processed"
        )
    ] = None,
    batch_size: Annotated[
        int,
        typer.Option(
            "--batch-size", "-b",
            help="Count of documents to process in one batch",
        )
    ] = None,
    workers: Annotated[
        int,
        typer.Option(
            "--workers", "-w",
            help="Count of parallel workers to process documents. Each worker use separate Gemini API key. Cannot be more than count of available API keys.",
        )
    ] = 8):
    """
    Extract content from documents stored in Yandex Disk.
    """
    import content
    cli_params = ExtractParams(
        md5=md5.strip() if md5 else None, 
        path=path.strip() if path else None,
        workers=workers,
        batch_size=batch_size if batch_size and batch_size > 0 else workers*3,
    )
    content.extract_content(cli_params)
    
    
@app.command()
def layouts(
        md5: Annotated[
        Optional[str],
        typer.Option(
            "--md5",
            callback=md5_validator,
            help="MD5 hash of the document. If not provided, all local documents will be processed."
        )
    ] = None,
    path: Annotated[
        Optional[str],
        typer.Option(
            "--path", "-p",
            help="Path to the document or directory in yandex disk. If not provided, all yandex disk will be processed"
        )
    ] = None,
):
    from layout.dispatch import layouts
    cli_params = CliParams(
        md5=md5.strip() if md5 else None, 
        path=path.strip() if path else None,
    )
    layouts(cli_params)
    
    
@app.command()
def match_limited():
    """
    Match limited and full books and check unmatched 
    """
    import match_limited
    match_limited.match_limited()


@app.command()
def fix_limited(
    output: Annotated[
        str,
        typer.Option(
            "--output",
            "-o",
            help="Output filename to write inside the workdir (~/.monocorpus)",
        ),
    ] = "limited_documents.json",
):
    """
    Dump limited documents to JSON and download missing files into the entry point.
    """
    import fix_limited as fix_limited_module
    fix_limited_module.fix_limited(output)
    
    
@app.command()
def sharing_restricted():
    """
    Check docs in sharing restricted folder are matches to docs in gsheets
    """
    import sharing_restricted
    sharing_restricted.check()
    
    
@app.command()
def check_artifacts():
    import check_artifacts
    check_artifacts.check()
    
    
@app.command()
def check_pub_links():
    """
    Check public links of documents in Yandex Disk and restore if needed
    """
    import check_pub_links
    check_pub_links.check()
    

@app.command()
def dump_state():
    """
    Dump current database state into google sheets and google drive
    """
    import dump_state
    dump_state.dump()


@app.command()
def upload_to_s3():
    from models import DocumentCrh
    from utils import read_config, get_session, download_file_locally
    from s3 import create_session, upload_file
    import os
    from rich.progress import track
    from yadisk_client import YaDisk
    from sqlalchemy import select

    print("Uploading docs to s3")
    predicate = (
        DocumentCrh.meta.is_(None)
        |
        DocumentCrh.language.in_(['crh-Latn', 'crh-Cyrl', 'crh-Latn-x-yanalif', 'crh-Arab'])
    )
    config = read_config()
    doc_bucket = config["yandex"]["cloud"]['bucket']['document']
    s3client = create_session(config)

    with get_session() as session, YaDisk(config['yandex']['disk']['oauth_token'], proxy=config['proxy']) as ya_client:
        docs = session.scalars(select(DocumentCrh).where(predicate))
        for doc in track(docs, "Processing docs"):
            local_doc_path = download_file_locally(ya_client, doc, config)
            doc_key = os.path.basename(local_doc_path)
            document_url = upload_file(local_doc_path, doc_bucket, doc_key, s3client, skip_if_exists=True)
            if doc.document_url != document_url:
                doc.document_url = document_url
                session.commit()


@app.command()
def collect_download_codes(
    output: Annotated[
        str,
        typer.Option(
            "--output",
            "-o",
            help="Output filename to write inside the workdir (~/.monocorpus)",
        ),
    ] = "download_codes.json",
):
    """
    Collect download_code values from upstream metadata for all full documents; if upstream metadata is absent (or lacks a code), record the document title instead and write both lists to JSON.
    """
    import download_codes
    download_codes.collect_download_codes(output)


@app.command()
def fix_limited():
    """
    Fix limited books in the database.
    """
    import fix_limited
    fix_limited.fix_limited()
