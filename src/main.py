# todo support DJVU, FB2 formats
# todo support annotations like [1], currently they are just merged with the preceding word
# todo tests for name normalization
# todo update README 
import time
import uuid
from typing import Annotated

import typer

from core import extract_text, process_files

from domain.text_source import TextSource
from file_utils import calculate_crc32

app = typer.Typer()
import json


@app.command()
def extract(count: int = 10):
    extract_text(count)


@app.command()
def process(count: int = 10):
    process_files(count)


if __name__ == "__main__":
    app()
