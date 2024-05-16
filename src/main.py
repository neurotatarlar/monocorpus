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
def wc(tokens: bool = False):
    """
    Count words in the both crawled documents and extracted from books texts
    """
    words, tokens = words_counter(tokens)
    words_in_book_texts, words_in_crawled_docs = words
    tokens_in_book_texts, tokens_in_crawled_docs = tokens or (0, 0)
    overall_words = words_in_book_texts + words_in_crawled_docs
    overall_tokens = None if tokens == (None, None) else (tokens_in_book_texts + tokens_in_crawled_docs)

    print(
        '[bold]'
        f'Overall count of words: {overall_words:_}, '
        f'words in the books: {words_in_book_texts:_}, '
        f'words in the crawled documents: {words_in_crawled_docs:_}, '
        f"{f'overall count of tokens: {overall_tokens:_}, ' if overall_tokens else ''}"
        f"{f'tokens in the books: {tokens_in_book_texts:_}, ' if tokens_in_book_texts else ''}"
        f"{f'tokens in the crawled documents: {tokens_in_crawled_docs:_}' if tokens_in_crawled_docs else ''}"
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
