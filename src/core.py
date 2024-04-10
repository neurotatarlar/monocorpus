from consts import Dirs
from extractors.epub import epub_to_text
from extractors.pdf import pdf_to_text
from file_utils import pick_files, precreate_folders, move_file
from post_processor import post_process
from type_detection import detect_type, FileType
from bibliographic import prompt_bibliographic_info
import subprocess


def process(count):
    """
    Processes files in the workdir

    :param count: Number of files to process
    """
    # preparation
    precreate_folders()

    # pick files to process
    files_to_process = pick_files(Dirs.ENTRY_POINT.get_real_path(), count)
    # process files
    report = _process_files(files_to_process)
    print(report)


def _process_files(files_to_process):
    report = ProcessingReport()

    for file in files_to_process:
        print(f"Processing file: {file}")

        detected_type = detect_type(file)
        print(f"Detected type: {detected_type}")

        if detected_type in [FileType.FB2, FileType.DJVU]:
            # These types are not supported yet, so we just move it to specific folder
            move_file(file, Dirs.NOT_SUPPORTED_FORMAT_YET.get_real_path())
            report.not_supported_yet.append(file)
            print(
                f"File `{file}` has unsupported format {detected_type}, moving it to the folder {Dirs.NOT_SUPPORTED_FORMAT_YET}")

        elif detected_type == FileType.OTHER:
            # This file is not a document at all. Again, we just move it to specific folder
            move_file(file, Dirs.NOT_A_DOCUMENT.get_real_path())
            report.not_a_documents.append(file)
            print(f"File `{file}` is not a document, moving it to the folder {Dirs.NOT_A_DOCUMENT}")

        if detected_type == FileType.PDF:
            path_to_tmp_txt_file = pdf_to_text(file)
        elif detected_type == FileType.EPUB:
            path_to_tmp_txt_file = epub_to_text(file)
        else:
            # we never should reach this point as long as we covered all the cases above
            continue

        if post_process(path_to_tmp_txt_file):
            # move_file(file, Dirs.COMPLETED.get_real_path())
            report.processed_docs.append(file)
            subprocess.call(('xdg-open', file))
            normalized_name = prompt_bibliographic_info()
            print("========", normalized_name)
        else:
            move_file(file, Dirs.NOT_TATAR.get_real_path())
            report.not_tt_documents.append(file)

        report.processed_counter += 1

    return report


class ProcessingReport:
    """
    Report of the processing

    processed_counter: Number of processed files
    not_a_documents: List of files that are not documents at all
    not_supported_yet: List of files with formats that are not supported yet
    processed_docs: List of files that are processed successfully
    not_tt_documents: List of files that are not in Tatar language
    """

    def __init__(self):
        self.processed_counter = 0
        self.not_a_documents = []
        self.not_supported_yet = []
        self.processed_docs = []
        self.not_tt_documents = []

    def __str__(self):
        return (
            "\nOverall report:\n"
            f"Processed {self.processed_counter} file(s): {self.processed_docs},\n"
            f"{len(self.not_a_documents)} file(s) is not a document(s): {self.not_a_documents},\n"
            f"{len(self.not_tt_documents)} file(s) is not a document(s) in Tatar language: {self.not_tt_documents},\n"
            f"{len(self.not_supported_yet)} file(s) has unsupported yet format: {self.not_supported_yet}"
        )
