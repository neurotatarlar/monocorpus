import os
import shutil

from rich import print
from rich.progress import track

from consts import Dirs
from file_utils import pick_files, create_folders, calculate_md5, get_path_in_workdir
from layout_analysis import get_layout_analysis
from text_extraction import extract

create_folders()


def layout_analysis(force):
    """
    Analyze the layout of the documents in the entry point folder
    """
    if files := pick_files(get_path_in_workdir(Dirs.ENTRY_POINT)):
        for file in files:
            md5 = calculate_md5(file)
            wip_path = os.path.join(get_path_in_workdir(Dirs.WORK_IN_PROGRESS), f"{md5}.{file.split('.')[-1]}")
            if not os.path.exists(wip_path) or force:
                shutil.copyfile(file, wip_path)

            get_layout_analysis(file, md5)
    else:
        print(f"No files for layout analysis, please put some documents to the folder `{Dirs.ENTRY_POINT}`")


def extract_text(force):
    if files := pick_files(get_path_in_workdir(Dirs.WORK_IN_PROGRESS)):
        for file in track(files, description="Extracting text from the documents..."):
            md5 = file.split("/")[-1].split(".")[0]
            path_to_la = os.path.join(get_path_in_workdir(Dirs.LABEL_STUDIO_TASKS), f"{md5}.json")
            if os.path.exists(path_to_la):
                extract(file, path_to_la)
            else:
                print(f"Layout analysis for the document `{file}` not found, please run layout analysis command first")

    else:
        print(
            f"No files for text extraction, please put some documents to the folder `{Dirs.ENTRY_POINT}` and run layout analysis command first")
