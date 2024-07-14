import os
import shutil

from rich import print
from rich.progress import track

from consts import Dirs
from file_utils import pick_files, create_folders, calculate_md5, get_path_in_workdir
from layout_analysis import layout_analysis
from text_extraction import extract
from integration.airtable import Document
import annotations_indexer

create_folders()


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
            shutil.move(file, get_path_in_workdir(Dirs.SENT_FOR_ANNOTATION))
    else:
        print(f"No files for layout analysis, please put some documents to the folder `{Dirs.ENTRY_POINT}`")



# def extract_text(force):
#     if files := pick_files(get_path_in_workdir(Dirs.WORK_IN_PROGRESS)):
#         for file in track(files, description="Extracting text from the documents..."):
#             md5 = file.split("/")[-1].split(".")[0]
#             path_to_la = os.path.join(get_path_in_workdir(Dirs.LABEL_STUDIO_TASKS), f"{md5}.json")
#             if os.path.exists(path_to_la):
#                 extract(file, path_to_la)
#             else:
#                 print(f"Layout analysis for the document `{file}` not found, please run layout analysis command first")
#
#     else:
#         print(
#             f"No files for text extraction, please put some documents to the folder `{Dirs.ENTRY_POINT}` and run layout analysis command first")
