import typer
from typing_extensions import Annotated
from typing import Optional
import re
from dataclasses import dataclass
from sync import sync as _sync
import metadata
import string
import prepare_shots
import extract_content
from enum import Enum

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})
slice_pattern = re.compile(r'^(?P<start>-?\d*)?:?(?P<stop>-?\d*)?:?(?P<step>-?\d*)?$')

class Tier(str, Enum):
    free = "free"
    promo = "promo"
    
@dataclass
class ExtractCliParams:
    md5: str
    path: str
    force: bool
    page_slice: str
    batch_size: int
    model: str
    parallelism: int
    limit: int 
    tier: Tier
    
@dataclass
class MetaCliParams:
    md5: str
    path: str
    model: str
    
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
    ] = 50,
    model: Annotated[
        str,
        typer.Option(
            "--model", "-m",
            help="Model to use for processing. See available models here: https://ai.google.dev/gemini-api/docs/models",
        )
    ] = "gemini-2.5-flash-preview-04-17",
    parallelism: Annotated[
        int,
        typer.Option(
            "--parallelism", "-p",
            help="Parallelism factor",
        )
    ] = 4,
    limit: Annotated[
        int,
        typer.Option(
            "--limit", "-l",
            help="Limit processed documents. If not provided, than all unprocessed documents will be taken",
        )
    ] = None,
    tier: Annotated[
        Tier,
        typer.Option(
            "--tier", "-t",
            help="Tier in Google used interact with Gemini",
            case_sensitive=False
        )
    ] = Tier.free,
    ):
    cli_params = ExtractCliParams(
        md5=md5,
        path=path,
        force=force,
        page_slice=pages_slice, 
        batch_size=batch_size,
        model=model,
        parallelism=parallelism,
        limit=limit,
        tier=tier
    )
    extract_content.extract_structured_content(cli_params)


@app.command()
def sync():
    """
    Sync the documents with the Yandex Disk and Google Sheets. It will traverse files and dirs in the yadisk and upload new entries to Google Sheets.
    """
    _sync()
    
@app.command()
def meta(
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
    model: Annotated[
        str,
        typer.Option(
            "--model", "-m",
            help="Model to use for processing. See available models here: https://ai.google.dev/gemini-api/docs/models",
        )
    ] = "gemini-2.5-flash-preview-04-17",
):
    """
    Extract metadata from the documents
    """
    cli_params = MetaCliParams(
        md5=md5,
        path=path,
        model=model
    )
    metadata.metadata(cli_params)

@app.command()
def shots():
    """
    Assemble ready-to-use prompt of structured content extraction
    """
    prepare_shots.load_inline_shots()