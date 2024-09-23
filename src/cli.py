import re
import string
from typing import Optional

import typer
from typing_extensions import Annotated

from dispatch import layout_analysis_entry_point, extract_text_entry_point

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})
slice_pattern = re.compile(r'^(?P<start>-?\d*)?:?(?P<stop>-?\d*)?:?(?P<step>-?\d*)?$')

def md5_validator(value: str):
    if value:
        if len(value) != 32:
            raise typer.BadParameter("MD5 should be 32 characters long")
        value = value.lower()
        if not all(ch in string.hexdigits for ch in value):
            raise typer.BadParameter("MD5 should be a hex string")
    return value


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
def predict(
        md5: Annotated[
            Optional[str],
            typer.Option(
                "--md5", "-m",
                callback=md5_validator,
                help="MD5 hash of the document. If not provided, all local documents will be processed."
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
        ] = "::"
):
    """
    Run PDF Document Layout Analysis. This will create images of every page, run page_layout analysis prediction and
    send the tasks to the labeling service
    """
    layout_analysis_entry_point(md5, force, pages_slice)

@app.command()
def sync():
    """
    Download new and changed annotations from the object storage and update the database
    and calculate the completeness of the annotations
    """
    import annotations_indexer
    annotations_indexer.sync()


@app.command()
def extract(
        md5: Annotated[
            Optional[str],
            typer.Option(
                "--md5", "-m",
                callback=md5_validator,
                help="MD5 hash of the document. If not provided, all local documents will be processed."
            )
        ] = None,
        force: Annotated[
            bool,
            typer.Option(
                "--force", "-f",
                help="Force the processing even if the document is already sent for annotation"
            )
        ] = False,
):
    """
    Extract text from the annotated documents
    """
    extract_text_entry_point(md5, force)
