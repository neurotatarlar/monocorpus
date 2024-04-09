import binascii
import os
import stat
import sys

from consts import Dirs


def move_file(path_to_file, target_dir):
    """
    Moves(rename) file to the target directory. The original name of the file is preserved.

    :param path_to_file: Path to the file to move
    :param target_dir: Directory to move file to
    """
    os.makedirs(target_dir, exist_ok=True)
    new_path = os.path.join(target_dir, os.path.basename(path_to_file))
    os.rename(path_to_file, new_path)


def calculate_crc(path_to_file):
    """
    Calculates CRC32 checksum as proof of integrity of the file

    :param path_to_file: Path to the file to calculate CRC32 checksum
    :return: hex representation of the CRC32 checksum
    """
    with open(path_to_file, "rb", encoding='utf-8') as f:
        return hex(binascii.crc32(f.read()))


# sys.argv[0]
def precreate_folders():
    """
    Creates all the folders that are used in the project
    """
    for d in Dirs:
        real_path = d.get_real_path()
        os.makedirs(real_path, exist_ok=True)
        git_keep_file = os.path.join(real_path, ".gitkeep")
        with open(git_keep_file, 'w', encoding='utf-8'):
            os.utime(git_keep_file)


def is_hidden(path_to_file):
    """
    Checks if the file is hidden

    :param path_to_file: Path to the file to check
    :return: True if the file is hidden, False otherwise
    """
    name = os.path.basename(path_to_file)
    # linux case
    if name.startswith('.'):
        return True

    # warning: no idea if it actually works on windows
    s = os.stat(path_to_file)
    if hasattr(s, 'st_file_attributes'):
        return s.st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN

    return False


def pick_files(dir_path: str, target_to_process: int):
    """
    Picks target number of files from the directory and its subdirectories

    :param dir_path: Path to the directory to pick files from
    :param target_to_process:  Number of files to pick
    :return: List of paths to the picked files
    """
    return _traverse_recursively(dir_path, target_to_process, 0, [])


def _traverse_recursively(dir_path: str, target_to_process: int, found_files_counter: int, files_to_process):
    for dir_name, dirs, files in os.walk(dir_path):

        for f in files:
            path_to_file = os.path.join(dir_name, f)
            if is_hidden(path_to_file):
                continue
            files_to_process.append(path_to_file)
            found_files_counter += 1
            if found_files_counter == target_to_process:
                return files_to_process

        for d in dirs:
            _traverse_recursively(d, found_files_counter, target_to_process, files_to_process)

    return files_to_process


def get_real_path_to_dir_with_executed_script():
    """
    Returns the path to the directory where the script was executed
    """
    return os.path.dirname(os.path.realpath(sys.argv[0]))
