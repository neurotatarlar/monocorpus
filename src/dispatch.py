from context import Context
from rich import print
from extractor import extract
from utils import read_config, pick_files, calculate_md5
from dirs import Dirs


def extract_content():
    config = read_config()
    for path_to_doc in pick_files(Dirs.ENTRY_POINT):
        try:
            hash = calculate_md5(path_to_doc)
            with Context(path_to_doc, config, hash) as context:
                extract(context)
        except KeyboardInterrupt:
            print("\nStopping...")
            exit(0)
        except BaseException as e:
            raise e
