import os
import shutil

import typer
from monocorpus_models import Document
from rich import print

from consts import Dirs
from file_utils import pick_files, calculate_md5, get_path_in_workdir, read_config
from integration.gsheets import find_by_md5, upsert, find_all_annotations_completed_and_not_extracted
from integration.s3 import download_annotation_summaries, create_session
from integration.yandex_disk import download_file_from_yandex_disk
from layout_analysis import layout_analysis
from text_extraction import extract_content


def _retrieve_files(md5=None, ya_public_key=None):
    """
    Retrieve files to process.

    If md5 is provided, then first check in the books local folder, if not found, then check in the entry point folder,
    if not found, then download from Yandex.Disk

    If md5 is not provided, then retrieve all files from the entry point folder

    :param md5: MD5 hash of the document
    :return: dict with key as MD5 of the document and value as the path to the document
    """
    if md5:
        # check in the books local folder
        file = os.path.join(get_path_in_workdir(Dirs.BOOKS_CACHE), f"{md5}.pdf")
        if os.path.exists(file) and os.path.isfile(file) and calculate_md5(file) == md5:
            return {md5: file}

        # check in the entry point folder
        files = _get_files_in_entry_point()
        if md5 in files:
            return {md5: files[md5]}

        if not ya_public_key:
            doc = find_by_md5(md5)
            if not doc:
                print(f"Document with the MD5 `{md5}` not found, please provide correct MD5")
                raise typer.Abort()
            ya_public_key = doc.ya_public_key

        # download from Yandex.Disk
        return {md5: download_file_from_yandex_disk(ya_public_key, file)}
    else:
        return _get_files_in_entry_point()


def _get_files_in_entry_point():
    """
    Get all files in the entry point folder
    return: dict with key as MD5 of the document and value as the path to the document
    """
    # list of all local files
    files = pick_files(get_path_in_workdir(Dirs.ENTRY_POINT))
    # key is MD5 of the document, value is the path to the source document
    return {calculate_md5(file): file for file in files}


def layout_analysis_entry_point(md5, force, pages_slice):
    """
    Analyze the page_layout of the documents in the entry point folder
    """
    if not (files := _retrieve_files(md5)):
        print(
            f"No files for page_layout analysis, please put some documents to the folder `{get_path_in_workdir(Dirs.ENTRY_POINT)}` or provide "
            f"MD5 of the document to download")
        raise typer.Abort()

    for md5, file in files.items():
        print(f"Analyzing page_layout of the document with MD5 `{md5}`...")
        # check if the document with the same md5 already exists in the remote datastore
        if not (doc := find_by_md5(md5)):
            print(f"Document with MD5 `{md5}` not found in the remote datastore, new document will be created...")
            doc = Document(md5=md5)

        if doc.sent_for_annotation and not force:
            print(f"Document with md5 `{md5}` already sent for annotation. Skipping...")
            continue

        # run page_layout analysis
        pages_count = layout_analysis(file, md5, pages_slice)

        # upsert the document
        doc.sent_for_annotation = True
        doc.pages_count = pages_count
        if not doc.names:
            doc.names = os.path.basename(file)
        if not doc.mime_type:
            doc.mime_type = "application/pdf"
        upsert(doc)

        # move file to the cache folder
        final_path = os.path.join(get_path_in_workdir(Dirs.BOOKS_CACHE), f"{md5}.pdf")
        if final_path != file:
            shutil.move(file, final_path)


def extract_text_entry_point(md5, force, pages_slice):
    if md5:
        doc = find_by_md5(md5)
        if not doc:
            print(f"Document with MD5 `{md5}` not found in the remote datastore")
            return
        if not doc.annotation_completed:
            print(f"Document with MD5 `{md5}` not annotated yet, annotate it first")
            return
        if doc.text_extracted and not force:
            print(f"Text for the document with MD5 `{md5}` already extracted. Skipping...")
            return
        docs_to_process = [doc]
    else:
        # get all documents that already annotated but the text is not extracted
        docs_to_process = find_all_annotations_completed_and_not_extracted()

        if not docs_to_process:
            print("All documents are already processed, it is time to do some annotation")
            return

    # download annotation summaries from the S3
    config = read_config()
    session = create_session(config)
    bucket = config['yc']['bucket']['annotations_summary']
    keys = [doc.md5 for doc in docs_to_process]
    downloaded_annotations = download_annotation_summaries(bucket, keys, session=session)

    for doc in docs_to_process:
        for _, path in _retrieve_files(doc.md5, doc.ya_public_key).items():
            extract_content(doc, path, downloaded_annotations[doc.md5], pages_slice)
