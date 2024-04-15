import json
import os
from typing import Dict

import typer

from bibliographic import prompt_bibliographic_info
from consts import Dirs
from domain.report import ProcessingReport
from domain.text_source import TextSource
from extractors.pdf import PdfExtractor
from extractors.epub import EpubExtractor
from file_utils import pick_files, precreate_folders, move_file, calculate_crc32
from post_processor import post_process
from type_detection import detect_type, FileType

"""
The list of processed files is stored in the index file.
"""
INDEX_FILE_NAME = 'index.json'


def extract_text(count):
    """
    Extract text from the files in the entry point folder
    :param count: number of files to process
    """
    # preparation
    precreate_folders()

    # pick files to process
    if files_to_process := pick_files(Dirs.ENTRY_POINT.get_real_path(), count):
        report = _extract_text_from_files(files_to_process)
        typer.echo(report)
    else:
        typer.echo(
            f"No documents to extract text from, please put some documents to the folder `{Dirs.ENTRY_POINT.value}`")


def process_files(count):
    """
    Post-process extracted texts

    :param count: number of files to process
    """
    # preparation
    precreate_folders()

    # pick files to process
    if files_to_process := pick_files(Dirs.DIRTY.get_real_path(), count):
        _process_files(files_to_process)
    else:
        typer.echo(
            f"No dirty texts to process, please extract some texts first and put them to the folder `{Dirs.DIRTY.value}`")


def _extract_text_from_files(files_to_process):
    report = ProcessingReport()
    index = load_index()

    for file in files_to_process:
        crc32 = calculate_crc32(file)
        if crc32 in index:
            file_name = os.path.basename(file)
            dir_to_move, report_method = Dirs.EXTRACTED_DOCS, lambda x: x.already_extracted(file_name)
        else:
            detected_type = detect_type(file)
            index[crc32] = {}
            dir_to_move, report_method = _extract_based_on_type(crc32, file, detected_type)

        move_file(file, dir_to_move.get_real_path())
        dump_index(index)
        report_method(report)

    return report


def _extract_based_on_type(source_id, file, detected_type):
    file_name = os.path.basename(file)
    match detected_type:
        case FileType.FB2 | FileType.DJVU:
            # These types are not supported yet, so we just move it to specific folder
            return Dirs.NOT_SUPPORTED_FORMAT_YET, lambda x: x.not_supported_yet(file_name)
        case FileType.OTHER:
            # This file is not a document at all. Again, we just move it to specific folder
            return Dirs.NOT_A_DOCUMENT, lambda x: x.not_a_document(file_name)

        case FileType.PDF:
            PdfExtractor().extract(source_id, file)
        case FileType.EPUB:
            EpubExtractor().extract(source_id, file)
    return Dirs.EXTRACTED_DOCS, lambda x: x.extracted_doc(file_name)


def _process_files(files_to_process):
    report = ProcessingReport()
    index = load_index()

    for file in files_to_process:
        is_tatar, crc32 = post_process(file)
        if is_tatar:
            report._extracted_docs.append(file)
            typer.launch(file)
            author, title, normalized_name = prompt_bibliographic_info()
            index[crc32] = TextSource(str(author), str(title), str(normalized_name))
            dump_index(index)
        else:
            move_file(file, Dirs.NOT_TATAR.get_real_path())
            report._not_tt_documents.append(file)

        report._processed_files += 1

    return report


def load_index() -> Dict[str, TextSource]:
    """
    Load the index file

    :return: the index file
    """
    with open(INDEX_FILE_NAME, 'r', encoding='utf-8') as index:
        return json.load(index)


def dump_index(index: Dict[str, TextSource]):
    """
    Dump the index to the file

    :param index: the index to dump
    """
    with open(INDEX_FILE_NAME, 'w', encoding='utf-8') as sink:
        json.dump(index, sink, indent=4, sort_keys=True, default=lambda o: o.__dict__)
