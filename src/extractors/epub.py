import os.path

import ebooklib
from bs4 import BeautifulSoup
from consts import Dirs
from ebooklib import epub


def epub_to_text(path_to_epub_file):
    """
    Extracts text from the epub file and saves it to the txt file

    :param path_to_epub_file: Path to the epub file
    :return: path to the txt file with extracted text
    """
    print(f"Extracting text from {path_to_epub_file}")

    file_name, _ = os.path.splitext(os.path.basename(path_to_epub_file))
    path_to_txt_path = os.path.join(Dirs.DIRTY.get_real_path(), file_name + ".txt")
    book = epub.read_epub(path_to_epub_file, {'ignore_ncx': True})

    with open(path_to_txt_path, 'w', encoding='utf-8') as output:
        for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
            soup = BeautifulSoup(item.get_body_content(), "html.parser")
            output.write(soup.get_text().strip() + "\n")  # we strip it to remove linebreaks spam

    return path_to_txt_path
