import typer
from rich import print
from typing_extensions import Annotated

from consts import Dirs
from extractor.core import extract_text, ExtractionCliArgs
from hf_connector import push_to_huggingface_repo
from post_processor import process_files
from type_detection import FileType
from wc import words_counter

app = typer.Typer()


@app.command()
def pdf(
        force: bool = typer.Option(False, "-f", "--force",
                                   help="Force extraction even if the text was already extracted. Default is False"),
        count: int = typer.Option(1, "-c", "--count", help="Number of files to process. Default is 1"),
        paragraph_indent: bool = typer.Option(True, "-p", "--paragraph-indent",
                                              help="Set True if new paragraph in the document is marked by a margin, set False otherwise")
):
    """
    Extract text from the PDF files
    """
    _extract(force, count, paragraph_indent, FileType.PDF)


@app.command()
def extract(
        force: bool = typer.Option(False, "-f", "--force",
                                   help="Force extraction even if the text was already extracted"),
        count: int = typer.Option(1, "-c", "--count", help="Number of files to process"),
        paragraph_indent: bool = typer.Option(True, "-p", "--paragraph-indent",
                                              help="Set True if new paragraph in the document is marked by a margin, set False otherwise")
):
    """
    Extract text from the files in the entry point folder
    """


@app.command()
def process(count: int = 1):
    """
    Post-process extracted texts

    :param count: number of files to process
    """
    process_files(count)


@app.command()
def wc():
    """
    Count words in the both crawled documents and extracted from books texts
    """
    words_in_book_texts, words_in_crawled_docs = words_counter()
    overall = words_in_book_texts + words_in_crawled_docs
    print(
        '[bold]'
        f'Overall count of words: {overall:_}, '
        f'words in the books: {words_in_book_texts:_}, '
        f'words in the crawled documents: {words_in_crawled_docs:_}'
    )


@app.command()
def upload(
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


def _extract(force: bool, count: int, has_paragraph_indentation: bool, rtype: FileType | None):
    args = ExtractionCliArgs(force, count, has_paragraph_indentation)
    extract_text(args, rtype)


# @app.command()
# def pdf(
#         keep: Annotated[Optional[List[str]], typer.Option()] = [],
#         remove: Annotated[Optional[List[str]], typer.Option()] = [],
#         force: bool = typer.Option(False, "-f", "--force"),
#         # exclude: [str] = typer.Option(None, "-e", "--exclude")
# ):
#     print(force, slice)
#     """
#     Extract text from the PDF files
#     """
#     pattern = re.compile(r'(?:(-?\d+):?|:)(?:(-?\d+):?|:)?(-?\d+)?')
#     result_items = []
#     result_items = {"1", "2", "3", "4", "5", "6", "7", "8", "9"}
#     for e in remove:
#         match = re.fullmatch(pattern, e)
#         if not match:
#             print('Invalid slice format')
#         else:
#             start, end, step = map(lambda x: None if x is None else int(x), match.groups())
#             s = slice(start, end, step)
#             result_items = result_items - set(list(result_items)[s])
#
#     print(sorted(result_items))
#


if __name__ == "__main__":
    app()
