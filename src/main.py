import json
import os

import typer
from rich import print
from typing_extensions import Annotated

from consts import Dirs
from epub import EpubExtractor
from file_utils import pick_files, precreate_folders, move_file, calculate_crc32, remove_file
from hf_connector import push_to_huggingface_repo
from pdf import PdfExtractor
from post_processor import post_process
from report import ProcessingReport
from type_detection import detect_type, FileType
from wc import words_counter

app = typer.Typer()
"""
The list of processed files is stored in the index file.
"""
INDEX_FILE_NAME = 'index.json'


@app.command()
def pdf(
        count: int = 10,
        force: bool = False
):
    """
    Extract text from pdf files in the entry point folder

    :param count: number of files to process
    """
    extract_text(count, force)


@app.command()
def post_process(count: int = 10):
    """
    Post-process extracted texts

    :param count: number of files to process
    """
    post_process_files(count)


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


def extract_text(count, force):
    """
    Extract text from the files in the entry point folder
    :param count: number of files to process
    """
    # preparation
    precreate_folders()

    # pick files to process
    if files_to_process := pick_files(Dirs.ENTRY_POINT.get_real_path(), count):
        report = _extract_text_from_files(files_to_process, force)
        typer.echo(report)
    else:
        typer.echo(
            f"No documents to extract text from, please put some documents to the folder `{Dirs.ENTRY_POINT.value}`")


def post_process_files(count):
    """
    Post-process extracted texts

    :param count: number of files to process
    """
    # preparation
    precreate_folders()

    # pick files to process
    if files_to_process := pick_files(Dirs.DIRTY.get_real_path(), count):
        _post_process_files(files_to_process)
    else:
        typer.echo(
            f"No dirty texts to process, please extract some texts first and put them to the folder `{Dirs.DIRTY.value}`")


def _extract_text_from_files(files_to_process, force):
    report = ProcessingReport()
    index = load_index()

    for file in files_to_process:
        crc32 = calculate_crc32(file)
        if force or crc32 not in index:
            detected_type = detect_type(file)
            index.add(crc32)
            dir_to_move, report_method = _extract_based_on_type(file, detected_type)
        else:
            file_name = os.path.basename(file)
            dir_to_move, report_method = Dirs.EXTRACTED_DOCS, lambda x: x.already_extracted(file_name)

        move_file(file, dir_to_move.get_real_path())
        dump_index(index)
        report_method(report)

    return report


def _extract_based_on_type(file, detected_type):
    file_name = os.path.basename(file)
    match detected_type:
        case FileType.FB2 | FileType.DJVU:
            # These types are not supported yet, so we just move it to specific folder
            return Dirs.NOT_SUPPORTED_FORMAT_YET, lambda x: x.not_supported_yet(file_name)
        case FileType.OTHER:
            # This file is not a document at all. Again, we just move it to specific folder
            return Dirs.NOT_A_DOCUMENT, lambda x: x.not_a_document(file_name)

        case FileType.PDF:
            PdfExtractor().extract(file)
        case FileType.EPUB:
            EpubExtractor().extract(file)
    return Dirs.EXTRACTED_DOCS, lambda x: x.extracted_doc(file_name)


def _post_process_files(files_to_process):
    for file in files_to_process:
        is_tatar = post_process(file)
        if not is_tatar:
            typer.echo(f"File '{file}' is not in Tatar language, moving to the folder `{Dirs.NOT_TATAR.value}`")
            move_file(file, Dirs.NOT_TATAR.get_real_path())
        else:
            remove_file(file)


def load_index() -> set[str]:
    """
    Load the index file

    :return: the index file
    """
    with open(INDEX_FILE_NAME, 'r', encoding='utf-8') as index:
        return set(json.load(index))


def dump_index(index: set[str]):
    """
    Dump the index to the file

    :param index: the index to dump
    """
    index = sorted(list(index))
    with open(INDEX_FILE_NAME, 'w', encoding='utf-8') as sink:
        json.dump(index, sink, indent=4, sort_keys=True, default=lambda o: o.__dict__, ensure_ascii=False)


if __name__ == "__main__":
    app()
