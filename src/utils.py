import os
from dirs import Dirs
import sys
import yaml
from typing import Union
import hashlib


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


def get_in_workdir(*dir_names: Union[str, Dirs], file: str = None, prefix: str = 'workdir'):
    dir_names = [i.value if isinstance(i, Dirs) else i for i in dir_names]
    script_parent_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
    path = [script_parent_dir, '..', prefix, *dir_names]
    path = os.path.normpath(os.path.join(*path))
    os.makedirs(path, exist_ok=True)
    if file:
        return os.path.join(path, file)
    else:
        return path
