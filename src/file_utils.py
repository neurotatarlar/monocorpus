import hashlib
import os
import sys

import yaml

from consts import Dirs


def create_folders():
    """
    Creates all the folders that are used in the project
    """
    for d in Dirs:
        real_path = get_path_in_workdir(d)
        os.makedirs(real_path, exist_ok=True)

def get_path_in_workdir(dir_name: str | Dirs, prefix: str = 'workdir'):
    """
    Get the real path of the directory
    """
    if isinstance(dir_name, Dirs):
        dir_name = dir_name.value
    parent_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
    paths = [parent_dir, '..', prefix, dir_name]
    path = os.path.join(*paths)
    return os.path.normpath(path)


def pick_files(dir_path: str):
    files_to_process = []
    for dir_name, dirs, files in os.walk(dir_path):

        for f in files:
            path_to_file = os.path.join(dir_name, f)
            files_to_process.append(path_to_file)

    return files_to_process


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
    with open(get_path_in_workdir(config_file, prefix="."), 'r') as file:
        config = yaml.safe_load(file)
    return config


create_folders()
