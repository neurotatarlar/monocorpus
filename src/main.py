import typer
from rich import print

from processor.core import extract_text, process_files
from wc import words_counter

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


@app.callback(invoke_without_command=True)
def extract_and_process(count: int = 10):
    """
    Post-process extracted texts

    :param count: number of files to process
    """
    extract_text(count)
    process_files(count)


@app.command()
def wc():
    """
    Count words in the both crawled documents and extracted from books texts
    """
    overall, words_in_book_texts, words_in_crawled_docs = words_counter()
    print(
        f"[green]{'>' * 20}[/green]\n"
        '[bold]'
        f'Overall number of words: {overall:_}, '
        f'words in the books: {words_in_book_texts:_}, '
        f'words in the crawled documents: {words_in_crawled_docs:_}'
    )


if __name__ == "__main__":
    app()
