# todo support DJVU, FB2 formats
# todo support annotations like [1], currently they are just merged with the preceding word
# todo tests for name normalization
# todo update README 
# todo add most popular authors

import typer

from core import extract_text, process_files

app = typer.Typer()


@app.command()
def extract(count: int = 10):
    """
    Extract text from the files in the entry point folder

    :param count: number of files to process
    """
    extract_text(count)


@app.command()
def process(count: int = 10):
    """
    Post-process extracted texts

    :param count: number of files to process
    """
    process_files(count)


if __name__ == "__main__":
    app()
