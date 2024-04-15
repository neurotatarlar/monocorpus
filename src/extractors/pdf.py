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

class PdfExtractor(Extractor):
    def extract(self, source_id, path_to_src_file):
        """
        Extracts text from the pdf file and saves it to the txt file

        :param path_to_pdf_file: Path to the pdf file
        :return: Path to the txt file with extracted text
        """
        full_file_name = os.path.basename(path_to_src_file)
        file_name, _ = os.path.splitext(full_file_name)
        path_to_txt_file = os.path.join(Dirs.DIRTY.get_real_path(), file_name + ".txt")
        with open(path_to_src_file, "rb") as input, open(path_to_txt_file, 'w', encoding='utf-8') as output:
            output.writelines(f"=====#{source_id}#=====Please do not delete this identification\n")
            parser = PDFParser(input)
            doc = PDFDocument(parser)
            rsrcmgr = PDFResourceManager()
            layout_params = LAParams(
                line_overlap=0.5,  # how much 2 chars overlap to be considered as a single word
                char_margin=2.0,  # how close 2 chars must be to each other to be considered as a single word
                line_margin=2,  # how close 2 lines must be to each other to be considered as a single paragraph
                word_margin=0.1,  # how close 2 words must be to each other to be considered as a single line
                boxes_flow=0.0,  # how much a horizontal(-1.0) and vertical(1.0) position of a text matters
                detect_vertical=False,  # ignore vertical text
                all_texts=False  # ignore text in figures
            )
            device = TextConverter(rsrcmgr, output, laparams=layout_params)
            interpreter = PDFPageInterpreter(rsrcmgr, device)
            pages_iter = PDFPage.create_pages(doc)
            total_pages = resolve1(doc.catalog['Pages'])['Count']
            for value in track(pages_iter, description=f"Extracting text from document `{full_file_name}`",
                               total=total_pages):
                interpreter.process_page(value)

        return path_to_txt_file
