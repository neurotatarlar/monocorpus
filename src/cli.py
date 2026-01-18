import typer
from typing_extensions import Annotated
from typing import Optional
from dataclasses import dataclass
import string

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})

    
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
