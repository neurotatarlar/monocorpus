import os.path
import os.path

import ebooklib
from bs4 import BeautifulSoup
from consts import Dirs
from ebooklib import epub
from processor.extractor.base import Extractor
from rich.progress import track


class EpubExtractor(Extractor):
    def extract(self, path_to_src_file):
        """
        Extracts text from the epub file and saves it to the txt file

        :param path_to_epub_file: Path to the epub file
        :return: path to the txt file with extracted text
        """
        file_name, _ = os.path.splitext(os.path.basename(path_to_src_file))
        path_to_txt_file = os.path.join(Dirs.DIRTY.get_real_path(), file_name + ".txt")
        book = epub.read_epub(path_to_src_file, {'ignore_ncx': True})

        with open(path_to_txt_file, 'w', encoding='utf-8') as output:
            pages_iter = list(book.get_items_of_type(ebooklib.ITEM_DOCUMENT))
            for item in track(pages_iter, description=f"Processing document `{file_name}`"):
                soup = BeautifulSoup(item.get_body_content(), "html.parser")
                output.write(soup.get_text().strip() + "\n")  # we strip it to remove linebreaks spam

        return path_to_txt_file
