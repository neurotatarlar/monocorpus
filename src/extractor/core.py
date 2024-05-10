import json
import os

from consts import Dirs
from extractor.epub import EpubExtractor
from extractor.pdf import PdfExtractor
from file_utils import pick_files, precreate_folders, move_file, calculate_crc32
from post_processor import ProcessingReport
from rich import print
from type_detection import FileType
from extractor.parent import ExtractionCliArgs

"""
The list of processed files is stored in the index file.
"""
INDEX_FILE_NAME = 'index.json'





def extract_text(args: ExtractionCliArgs, rtype: FileType | None):
    """
    Extract text from the files in the entry point folder

    :param args: command line arguments
    :param rtype: type of the file to extract text from, None means all supported types
    """
    # preparation
    precreate_folders()

    # pick files to process
    if files_to_process := pick_files(Dirs.ENTRY_POINT.get_real_path(), args.count, rtype):
        report = _extract_text_from_files(files_to_process, args)
        print(report)
    else:
        print(f"No documents to extract text from, please put some documents to the folder `{Dirs.ENTRY_POINT.value}`")


def _extract_text_from_files(files_to_process, args: ExtractionCliArgs):
    report = ProcessingReport()
    index = _load_index()

    for dtype, file in files_to_process:
        crc32 = calculate_crc32(file)
        if crc32 in index and not args.force:
            # the file was already processed
            file_name = os.path.basename(file)
            dir_to_move, report_method = Dirs.EXTRACTED_DOCS, lambda x: x.already_extracted(file_name)
        else:
            index.append(crc32)
            dir_to_move, report_method = _extract_based_on_type(file, dtype, args)

        move_file(file, dir_to_move.get_real_path())
        _dump_index(index)
        report_method(report)

    return report


def _extract_based_on_type(file, dtype, args: ExtractionCliArgs):
    file_name = os.path.basename(file)
    match dtype:
        case FileType.FB2 | FileType.DJVU:
            # These types are not supported yet, so we just move it to specific folder
            return Dirs.NOT_SUPPORTED_FORMAT_YET, lambda x: x.not_supported_yet(file_name)
        case FileType.OTHER:
            # This file is not a document at all. Again, we just move it to specific folder
            return Dirs.NOT_A_DOCUMENT, lambda x: x.not_a_document(file_name)
        case FileType.PDF:
            PdfExtractor.extract(file, args)
        case FileType.EPUB:
            EpubExtractor.extract(file)
    return Dirs.EXTRACTED_DOCS, lambda x: x.extracted_doc(file_name)


def _load_index() -> list[str]:
    """
    Load the index file

    :return: the index file
    """
    with open(INDEX_FILE_NAME, 'r', encoding='utf-8') as index:
        return json.load(index)


def _dump_index(index: list[str]):
    """
    Dump the index to the file

    :param index: the index to dump
    """
    index.sort()
    with open(INDEX_FILE_NAME, 'w', encoding='utf-8') as sink:
        json.dump(index, sink, indent=4, sort_keys=True, default=lambda o: o.__dict__, ensure_ascii=False)
