import os.path
import os.path
from abc import ABC, abstractmethod

import ebooklib
from bs4 import BeautifulSoup
from ebooklib import epub
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfinterp import resolve1
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from rich.progress import track

from consts import Dirs
from extractors.base import Extractor



class EpubExtractor(Extractor):
    def extract(self, source_id, path_to_src_file):
        """
        Extracts text from the epub file and saves it to the txt file

        :param path_to_epub_file: Path to the epub file
        :return: path to the txt file with extracted text
        """
        file_name, _ = os.path.splitext(os.path.basename(path_to_src_file))
        path_to_txt_file = os.path.join(Dirs.DIRTY.get_real_path(), file_name + ".txt")
        book = epub.read_epub(path_to_src_file, {'ignore_ncx': True})

        with open(path_to_txt_file, 'w', encoding='utf-8') as output:
            output.writelines(f"=====#{source_id}#=====Please do not delete this identification\n")

            pages_iter = list(book.get_items_of_type(ebooklib.ITEM_DOCUMENT))
            for item in track(pages_iter, description=f"Processing document `{file_name}`"):
                soup = BeautifulSoup(item.get_body_content(), "html.parser")
                output.write(soup.get_text().strip() + "\n")  # we strip it to remove linebreaks spam

        return path_to_txt_file