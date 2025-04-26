import typer
from typing_extensions import Annotated
from typing import Optional
import re
from dataclasses import dataclass
from sync import sync as _sync
import dispatch
import metadata

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})
slice_pattern = re.compile(r'^(?P<start>-?\d*)?:?(?P<stop>-?\d*)?:?(?P<step>-?\d*)?$')

@dataclass
class ExtractCliParams:
    public_url: str
    force: bool
    page_slice: str
    meta: bool
    batch_size: int
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
    

@app.command()
def extract(
    public_url: Annotated[
        str,
        typer.Argument(
            help="Public URL of the document to process. Example: https://yadi.sk/i/XXXXXX",
        )
    ],
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
    ] = 5,
    model: Annotated[
        str,
        typer.Option(
            "--model", "-m",
            help="Model to use for processing. See available models here: https://ai.google.dev/gemini-api/docs/models",
        )
    ] = "gemini-2.5-flash-preview-04-17",
    ):
    cli_params = ExtractCliParams(
        public_url=public_url,
        force=force,
        page_slice=pages_slice, 
        meta=meta,
        batch_size=batch_size,
        model=model
    )
    dispatch.extract_content(public_url, cli_params)


@app.command()
def sync():
    """
    Sync the documents with the Yandex Disk and Google Sheets. It will traverse files and dirs in the yadisk and upload new entries to Google Sheets.
    """
    _sync()
    
@app.command()
def meta(
    model: Annotated[
        str,
        typer.Option(
            "--model", "-m",
            help="Model to use for processing. See available models here: https://ai.google.dev/gemini-api/docs/models",
        )
    ] = "gemini-2.0-flash",
):
    """
    Extract metadata from the documents
    """
    metadata.extract(model)