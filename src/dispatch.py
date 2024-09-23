import json
import os
import shutil

import typer
from monocorpus_models import Document
from rich import print
from rich.progress import track

from consts import Dirs
from file_utils import pick_files, calculate_md5, get_path_in_workdir, read_config
from layout_analysis import layout_analysis
from integration.s3 import download_annotation_summaries, create_session
from integration.yandex_disk import download_file_from_yandex_disk
from integration.gsheets import find_by_md5, upsert
from text_extraction import extract_content

def _retrieve_files(md5=None):
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

        doc = find_by_md5(md5)
        if not doc:
            print(f"Document with the MD5 `{md5}` not found, please provide correct MD5")
            raise typer.Abort()

        # download from Yandex.Disk
        return { md5: download_file_from_yandex_disk(doc.ya_public_key, file) }
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
        print(f"No files for page_layout analysis, please put some documents to the folder `{get_path_in_workdir(Dirs.ENTRY_POINT)}` or provide "
              f"MD5 of the document to download")
        raise typer.Abort()

    for md5, file in files.items():
        print(f"Analyzing page_layout of the document with MD5 `{md5}`...")
        # check if the document with the same md5 already exists in the remote datastore
        if (doc := find_by_md5(md5)) and doc.sent_for_annotation and not force:
            print(f"Document with md5 `{md5}` already sent for annotation. Skipping...")
        else:
            doc = Document(md5=md5)

            # run page_layout analysis
            pages_count = layout_analysis(file, md5, pages_slice)

            # update the document in the remote datastore
            doc.sent_for_annotation = True
            doc.pages_count = pages_count
            upsert(doc)

        # move file to the cache folder
        final_path = os.path.join(get_path_in_workdir(Dirs.BOOKS_CACHE), f"{md5}.pdf")
        if final_path != file:
            shutil.move(file, final_path)

def extract_text_entry_point(md5, force):
    pass
    # if md5:
    #     doc = find_by_md5(md5)
    #     if not doc:
    #         print(f"Document with MD5 `{md5}` not found in the Airtable")
    #         return
    #     if not doc.sent_for_annotation:
    #         print(f"Document with MD5 `{md5}` not sent for annotation yet, inference and annotate it first")
    #         return
    #     if doc.text_extracted and not force:
    #         print(f"Text for the document with MD5 `{md5}` already extracted. Skipping...")
    #         return
    #     docs_to_process = {doc.md5: doc.ya_public_key}
    # else:
    #     # get all documents that already annotated but the text is not extracted
    #     docs_to_process = [
    #         doc.to_record()['fields']
    #         for doc
    #         in Document.all(
    #             formula=match({
    #                 Document.annotation_completed.field_name: True,
    #                 Document.text_extracted.field_name: False
    #             }),
    #             fields=[Document.md5.field_name, Document.ya_public_key.field_name]
    #         )
    #     ]
    #     docs_to_process = {doc['md5']: doc['ya_public_key'] for doc in docs_to_process if doc}
    #
    #     if not docs_to_process:
    #         print("All documents are already processed, it is time to do some annotation")
    #         return
    #
    # # get all annotation summaries for the documents to get link for the results
    # anno_sums = [
    #     a.to_record()['fields']
    #     for a
    #     in AnnotationsSummary.all(
    #         fields=[AnnotationsSummary.doc_md5.field_name, AnnotationsSummary.result_link.field_name],
    #         formula=OR(
    #             *[
    #                 match({AnnotationsSummary.doc_md5.field_name: doc_md5})
    #                 for doc_md5 in docs_to_process
    #             ]
    #         )
    #     )
    # ]
    # anno_sums = {a['doc_md5']: a['result_link'] for a in anno_sums if a}
    #
    # # download annotation summaries from the S3
    # config = read_config()
    # session = create_session(config)
    # bucket = config['yc']['bucket']['annotations_summary']
    # # key is MD5 of the document, value is the path to the downloaded file with summary results
    # downloaded_annotations = download_annotation_summaries(bucket, anno_sums, session=session)
    #
    # # place to store the documents that are waiting for extraction
    # dir_with_docs = get_path_in_workdir(Dirs.WAITING_FOR_EXTRACTION)
    #
    # # Get MD5 of the local documents waiting for extraction
    # # key is MD5 of the document, value is the path to the source document
    # local_md5s = {calculate_md5(file): file for file in pick_files(dir_with_docs)}
    #
    # for md5, path_to_annot_res in track(downloaded_annotations.items(), "Extracting text from the documents..."):
    #     # if document is not in the local folder, then download it from Yandex.Disk
    #     if not (path_to_doc := local_md5s.get(md5)):
    #         print(f"Document with MD5 `{md5}` not found in the local folder, downloading it from Yandex.Disk")
    #         # download document from Yandex.Disk
    #         path_to_doc = os.path.join(dir_with_docs, f"{md5}.pdf")
    #         download_file_from_yandex_disk(docs_to_process[md5], path_to_doc)
    #
    #     extract_content(md5, path_to_doc, path_to_annot_res)
