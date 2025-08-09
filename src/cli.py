import typer
from typing_extensions import Annotated
from typing import Optional
import re
from dataclasses import dataclass
import string
from enum import Enum

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})
extract_app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})
app.add_typer(extract_app, name="extract", help="Extract by format")

slice_pattern = re.compile(r'^(?P<start>-?\d*)?:?(?P<stop>-?\d*)?:?(?P<step>-?\d*)?$')

class Tier(str, Enum):
    free = "free"
    promo = "promo"

@dataclass
class ExtractPdfParams:
    md5: str
    path: str
    force: bool
    page_slice: str
    batch_size: int
    model: str
    limit: int
    
@dataclass
class ExtractEpubParams:
    md5: str
    path: str
    limit: int

def slice_parser(value: str):
    if value:
        match = slice_pattern.match(value)
        if not match:
            raise typer.BadParameter(f"Invalid slice string: `{value}`")

        start = match.group('start')
        stop = match.group('stop')
        step = match.group('step')

        # Convert to integers if they are not empty, otherwise use None
        start = int(start) if start else None
        stop = int(stop) if stop else None
        step = int(step) if step else None

        return slice(start, stop, step)
    return slice(0, None, 1)


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
def select(query: list[str]):
    """
    Execute an SQL query on the monocorpus database.

    This command allows users to run custom SQL queries directly on the monocorpus database. 
    It provides a flexible way to retrieve, filter, or analyze data stored in the database 
    based on the specified query.
    """
    import sheets_introspect
    sheets_introspect.sheets_introspect(" ".join(query))


@extract_app.command(name="pdf")
def extract_pdf(
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
            help="Path to the document or directory in yandex disk"
        )
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force", "-f",
            help="Force the processing even if the document is already sent for annotation"
        )
    ] = False,
    pages_slice: Annotated[
        Optional[str],
        typer.Option(
            "--slice", "-s",
            parser=slice_parser,
            help="Slice of the pages to process. Format: `start:stop:step`. If not provided, all pages will be processed."
        )
    ] = "::",
    batch_size: Annotated[
        int,
        typer.Option(
            "--batch-size", "-b",
            help="Batch size for processing pages",
        )
    ] = 20,
    model: Annotated[
        str,
        typer.Option(
            "--model", "-m",
            help="Model to use for processing. See available models here: https://ai.google.dev/gemini-api/docs/models",
        )
    ] = "gemini-2.5-pro",
    limit: Annotated[
        int,
        typer.Option(
            "--limit", "-l",
            help="Limit processed documents. If not provided, than all unprocessed documents will be taken",
        )
    ] = None,
):
    from content.pdf import extract
    cli_params = ExtractPdfParams(
        md5=md5,
        path=path,
        force=force,
        page_slice=pages_slice,
        batch_size=batch_size,
        model=model,
        limit=limit,
    )
    extract(cli_params)


@extract_app.command(name="epub")
def extract_epub(
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
            help="Path to the document or directory in yandex disk"
        )
    ] = None,
    limit: Annotated[
        int,
        typer.Option(
            "--limit", "-l",
            help="Limit processed documents. If not provided, than all unprocessed documents will be taken",
        )
    ] = None,
):
    from content.epub import extract
    cli_params = ExtractEpubParams(
        md5=md5, 
        path=path,
        limit=limit
    )
    extract(cli_params)
    
@extract_app.command(name="docx")
def extract_docx():
    from content.docx import extract
    extract()
    
@app.command()
def hf():
    import hf 
    hf.assemble_dataset()
    
@app.command()
def meta():
    import metadata
    metadata.extract_metadata()
    
@app.command()
def extract2():
    import content
    content.extract_content()