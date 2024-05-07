import typer
from rich import print
from typing_extensions import Annotated

from consts import Dirs
from hf_connector import push_to_huggingface_repo
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


@app.command()
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
        '[bold]'
        f'Overall count of words: {overall:_}, '
        f'words in the books: {words_in_book_texts:_}, '
        f'words in the crawled documents: {words_in_crawled_docs:_}'
    )


@app.command()
def upload_artifacts(
        path: Annotated[
            str, typer.Option(help="Path to the directory with artifacts to upload")] = Dirs.ARTIFACTS.get_real_path(),
        repo_id: Annotated[
            str, typer.Option(help="HF repository to upload the artifacts to")] = 'neurotatarlar/tt-books-cyrillic',
        commit_message: Annotated[str, typer.Option(help="Commit message for the upload")] = "Updating the datasets"):
    """
    Upload the artifacts to the HF repository

    You must be logged in to Hugging Face. Use `huggingface-cli login`. See docs for additional info
    """
    push_to_huggingface_repo(path, repo_id, commit_message)
    print(f"Artifacts were successfully uploaded to the repository `{repo_id}`")


if __name__ == "__main__":
    app()
