import json
import os
import shutil

from pyairtable.formulas import match, OR
from rich import print
from rich.progress import track

from consts import Dirs
from file_utils import pick_files, create_folders, calculate_md5, get_path_in_workdir, read_config
from integration.airtable import Document, AnnotationSummary
from layout_analysis import layout_analysis
from integration.s3 import download_annotation_summaries, create_session
from integration.yandex_disk import download_file_from_yandex_disk
from text_extraction import extract_content

def layout_analysis_entry_point(force):
    """
    Analyze the layout of the documents in the entry point folder
    """
    if files := pick_files(get_path_in_workdir(Dirs.ENTRY_POINT)):
        for file in files:
            # calculate the md5 of the file to use it as a unique identifier
            md5 = calculate_md5(file)

            # check if the document with the same md5 already exists in the Airtable
            if doc := Document.first(formula=f"md5='{md5}'"):
                if doc.sent_for_annotation and not force:
                    print(f"Document with md5 `{md5}` already sent for annotation. Skipping...")
                    continue
            else:
                doc = Document(md5=md5)

            # run layout analysis
            pages_count = layout_analysis(file, md5)

            # update the document in the Airtable
            doc.sent_for_annotation = True
            doc.pages_count = pages_count
            doc.save()

            # move file
            shutil.move(file, get_path_in_workdir(Dirs.WAITING_FOR_EXTRACTION))
    else:
        print(f"No files for layout analysis, please put some documents to the folder `{Dirs.ENTRY_POINT}`")


def extract_text_entry_point():
    # get all documents that already annotated but the text is not extracted
    not_extracted_docs = [
        doc.to_record()['fields']
        for doc
        in Document.all(
            formula=match({
                Document.annotation_completed.field_name: True,
                Document.text_extracted.field_name: False
            }),
            fields=[Document.md5.field_name, Document.ya_public_key.field_name]
        )
    ]
    not_extracted_docs = {doc['md5']: doc['ya_public_key'] for doc in not_extracted_docs if doc}

    if not not_extracted_docs:
        print("All documents are already processed, it is time to do some annotation")
        return

    # get all annotation summaries for the documents to get link for the results
    anno_sums = [
        a.to_record()['fields']
        for a
        in AnnotationSummary.all(
            fields=[AnnotationSummary.doc_md5.field_name, AnnotationSummary.result_link.field_name],
            formula=OR(
                *[
                    match({AnnotationSummary.doc_md5.field_name: doc_md5})
                    for doc_md5 in not_extracted_docs
                ]
            )
        )
    ]
    anno_sums = {a['doc_md5']: a['result_link'] for a in anno_sums if a}

    # download annotation summaries from the S3
    config = read_config()
    session = create_session(config)
    bucket = config['yc']['bucket']['annotations_summary']
    # key is MD5 of the document, value is the path to the downloaded file with summary results
    downloaded_annotations = download_annotation_summaries(bucket, anno_sums, session=session)

    # place to store the documents that are waiting for extraction
    dir_with_docs = get_path_in_workdir(Dirs.WAITING_FOR_EXTRACTION)

    # Get MD5 of the local documents waiting for extraction
    # key is MD5 of the document, value is the path to the source document
    local_md5s = {calculate_md5(file): file for file in pick_files(dir_with_docs)}

    # todo remove this line
    downloaded_annotations = {k: v for k, v in downloaded_annotations.items() if k == '1a9e6d0120b09d498855adc755b780dc'}

    for md5, path_to_annot_res in track(downloaded_annotations.items(), "Extracting text from the documents..."):
        # if document is not in the local folder, then download it from Yandex.Disk
        if not (path_to_doc := local_md5s.get(md5)):
            print(f"Document with MD5 `{md5}` not found in the local folder, downloading it from Yandex.Disk")
            # download document from Yandex.Disk
            path_to_doc = os.path.join(dir_with_docs, f"{md5}.pdf")
            download_file_from_yandex_disk(not_extracted_docs[md5], path_to_doc)

        extract_content(md5, path_to_doc, path_to_annot_res)
