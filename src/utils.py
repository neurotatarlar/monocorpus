import os
from dirs import Dirs
import sys
import yaml
from typing import Union
import hashlib
from gsheets import find_by_md5, find_all_by_md5


def pick_files(dir_path: Union[str, Dirs]):
    return [
        os.path.normpath(os.path.join(dir_name, f))
        for dir_name, _, files
        in os.walk(get_in_workdir(dir_path))
        for f
        in files
    ]


def calculate_md5(file_path: str):
    """
    Calculates MD5 hash of the file

    :param file_path: path to the file
    :return: MD5 hash of the file
    """
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(2048), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def read_config(config_file: str = "config.yaml"):
    with open(get_in_workdir(file=config_file, prefix="."), 'r') as file:
        return yaml.safe_load(file)


def get_in_workdir(*dir_names: Union[str, Dirs], file: str = None, prefix: str = '~/.monocorpus'):
    dir_names = [i.value if isinstance(i, Dirs) else i for i in dir_names]
    script_parent_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
    path = [script_parent_dir, '..', os.path.expanduser(prefix), *dir_names]
    path = os.path.normpath(os.path.join(*path))
    os.makedirs(path, exist_ok=True)
    if file:
        return os.path.join(path, file)
    else:
        return path

def obtain_documents(cli_params, ya_client, fallback):
    if cli_params.md5:
        print(f"Looking for document by md5 '{cli_params.md5}'")
        return [find_by_md5(cli_params.md5)]
    
    if cli_params.path:
        _meta = ya_client.get_meta(cli_params.path, fields=['md5', 'type', 'path'])
        if _meta.type == 'file':
            print(f"Looking for document by path '{cli_params.path}'")
            return [find_by_md5(_meta.md5)]
        if _meta.type == 'dir':
            print(f"Traverse documents by path '{cli_params.path}'")
            dirs_to_visit = [_meta.path]
            md5s = set()
            while dirs_to_visit:
                dir = dirs_to_visit.pop(0)
                _listing = ya_client.listdir(dir, max_items=None, fields=['md5', 'type', 'path'])
                for _item in _listing:
                    if _item.type == 'dir':
                        dirs_to_visit.append(_item.path)
                    elif _item.type == 'file':
                        md5s.add(_item.md5)
            return find_all_by_md5(md5s)
    print("Fall back")
    return fallback()
            
def download_file_locally(ya_client, doc):
    ext = doc.mime_type.split("/")[-1]
    if ext == "pdf":
        ext = "pdf"
    elif ext == "epub":
        ext = "epub"
    else:
        raise ValueError(f"Unsupported file type: {doc.mime_type}")
    
    local_path=get_in_workdir(Dirs.ENTRY_POINT, file=f"{doc.md5}.{ext}")
    if not (os.path.exists(local_path) and calculate_md5(local_path) == doc.md5):
        with open(local_path, "wb") as f:
            ya_client.download_public(doc.ya_public_url, f)
    return local_path