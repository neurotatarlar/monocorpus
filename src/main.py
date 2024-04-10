# todo try to extract ISBN from the book
# todo support DJVU, FB2 formats
# todo check if book was recognized or not
# todo check files is not processed yet
# todo define books title and author
# todo support annotations like [1], currently they are just merged with the preceding word


import typer

from core import process

app = typer.Typer()

from bibliographic import prompt_bibliographic_info

@app.command()
def _process(count: int = 10):
    """
    Run processing of documents

    :param count: Number of documents to process
    """
    process(count)


if __name__ == "__main__":
    app()
